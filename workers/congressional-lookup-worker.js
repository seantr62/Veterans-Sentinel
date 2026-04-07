// congressional-lookup-worker.js
// Serves ZIP-to-representative lookups from the verified D1 database
// All 51 jurisdictions — Truth-By-Consensus verified data

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS_HEADERS });
    }

    // GET /reps?zip=XXXXX
    if (url.pathname === "/reps") {
      const zip = url.searchParams.get("zip");
      if (!zip || !/^\d{5}$/.test(zip)) {
        return jsonResponse({ error: "Invalid ZIP code" }, 400);
      }

      try {
        // Step 1: Look up districts for this ZIP
        const districtRows = await env.DB.prepare(
          "SELECT state, district FROM zip_districts WHERE zip = ?"
        ).bind(zip).all();

        if (!districtRows.results || districtRows.results.length === 0) {
          // ZIP not in database yet — fall back to Claude for district ID
          return await fallbackClaudeLookup(zip, env);
        }

        const state = districtRows.results[0].state;
        const districts = districtRows.results.map(r => r.district);

        // Step 2: Get House rep(s) for those districts from verified database
        const results = [];

        for (const district of districts) {
          const rep = await env.DB.prepare(
            `SELECT * FROM members
             WHERE state = ? AND district = ? AND role_type = 'representative'
             AND status_consensus = 'Verified'
             ORDER BY last_verified DESC LIMIT 1`
          ).bind(state, district).first();

          if (rep) {
            results.push(formatMember(rep, "House"));
          }
        }

        // Step 3: Get both senators for the state
        const senators = await env.DB.prepare(
          `SELECT * FROM members
           WHERE state = ? AND role_type = 'senator'
           AND status_consensus = 'Verified'
           ORDER BY last_verified DESC LIMIT 2`
        ).bind(state).all();

        for (const sen of senators.results || []) {
          results.push(formatMember(sen, "Senate"));
        }

        if (results.length === 0) {
          return await fallbackClaudeLookup(zip, env);
        }

        return jsonResponse({
          results,
          zip,
          state,
          multi_district: districts.length > 1,
          verified: true,
          source: "Congressional Verified Database — Truth-By-Consensus"
        });

      } catch (err) {
        console.error("Lookup error:", err.message);
        return jsonResponse({ error: "Lookup failed. Please try again." }, 500);
      }
    }

    // GET /status — database health check
    if (url.pathname === "/status") {
      try {
        const count = await env.DB.prepare(
          "SELECT COUNT(*) as total, SUM(CASE WHEN status_consensus='Verified' THEN 1 ELSE 0 END) as verified FROM members"
        ).first();
        const lastRun = await env.DB.prepare(
          "SELECT MAX(checked_at) as last_run FROM verification_log"
        ).first();
        return jsonResponse({ ...count, last_verified: lastRun?.last_run, database: "congressional-verified" });
      } catch (err) {
        return jsonResponse({ error: "Status check failed" }, 500);
      }
    }

    // GET / — serve Contact Your Rep HTML
    if (url.pathname === "/" || url.pathname === "") {
      try {
        const res = await fetch("https://seantr62.github.io/Veterans-Sentinel/contact-your-rep/index.html");
        if (!res.ok) throw new Error("GitHub fetch failed");
        const html = await res.text();
        return new Response(html, {
          headers: { "Content-Type": "text/html;charset=UTF-8", "Cache-Control": "public, max-age=300" }
        });
      } catch (err) {
        return new Response("<h1>Page temporarily unavailable. Please visit valiantheroes.org</h1>", {
          status: 503, headers: { "Content-Type": "text/html" }
        });
      }
    }

    return new Response("Not found", { status: 404 });
  }
};

function formatMember(m, type) {
  return {
    name: m.first_name + " " + m.last_name,
    title: type === "House" ? "Representative" : "Senator",
    party: m.party,
    district: m.district || "",
    role: type === "House" ? `U.S. House — District ${m.district}` : `U.S. Senator — ${m.state}`,
    state: m.state,
    phone: m.phone || "",
    office: m.office || "",
    verified: m.status_consensus === "Verified",
    sources_agree: m.sources_agree,
    type
  };
}

// Fallback: use Claude + GovTrack when ZIP not yet in database
async function fallbackClaudeLookup(zip, env) {
  const CLAUDE_SYSTEM = `You are a congressional district lookup database for the 119th United States Congress (2025-2027).
When given a ZIP code, return ONLY a raw JSON object — no markdown, no backticks.
Return: { "state": "AZ", "zip": "85001", "house_districts": [3], "multi_district": false, "note": "" }
Rules:
- List ALL possible house districts for this ZIP — never guess just one if uncertain
- Return ONLY the JSON object`;

  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      max_tokens: 256,
      system: CLAUDE_SYSTEM,
      messages: [{ role: "user", content: `ZIP code: ${zip}` }]
    })
  });

  if (!claudeRes.ok) throw new Error("Claude API error");

  const claudeData = await claudeRes.json();
  let rawText = claudeData.content[0].text.trim().replace(/```json|```/g, "").trim();
  let districtInfo;
  try { districtInfo = JSON.parse(rawText); }
  catch (e) { const m = rawText.match(/\{[\s\S]*\}/); districtInfo = m ? JSON.parse(m[0]) : null; }

  if (!districtInfo?.house_districts) throw new Error("Could not parse district info");

  const state = districtInfo.state;
  const districts = districtInfo.house_districts;

  // Store in zip_districts for future lookups
  for (const d of districts) {
    await env.DB.prepare(
      "INSERT OR IGNORE INTO zip_districts (zip, state, district) VALUES (?, ?, ?)"
    ).bind(zip, state, String(d)).run();
  }

  // Verify via GovTrack
  const govtrackCalls = [
    fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&role_type=senator`),
    ...districts.map(d => fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&district=${d}&role_type=representative`))
  ];
  const responses = await Promise.all(govtrackCalls);
  const govtrackData = await Promise.all(responses.map(r => r.json()));

  const senators = govtrackData[0].objects || [];
  const houseReps = govtrackData.slice(1).flatMap(d => d.objects || []);
  const results = [];

  for (const rep of houseReps) {
    results.push({
      name: rep.person.firstname + " " + rep.person.lastname,
      title: "Representative",
      party: rep.party,
      district: String(rep.district),
      role: `U.S. House — District ${rep.district}`,
      state: rep.state,
      phone: rep.phone || "",
      office: rep.extra?.address || "",
      verified: false,
      type: "House",
      note: "Live lookup — not yet in verified database"
    });
  }

  for (const sen of senators) {
    results.push({
      name: sen.person.firstname + " " + sen.person.lastname,
      title: "Senator",
      party: sen.party,
      district: "",
      role: `U.S. Senator — ${state}`,
      state: sen.state,
      phone: sen.phone || "",
      office: sen.extra?.address || "",
      verified: false,
      type: "Senate",
      note: "Live lookup — not yet in verified database"
    });
  }

  return jsonResponse({
    results,
    zip,
    state,
    multi_district: districts.length > 1,
    note: districtInfo.note || "",
    verified: false,
    source: "Live fallback — Claude + GovTrack"
  });
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS }
  });
}
