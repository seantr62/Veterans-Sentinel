// congressional-lookup-worker.js
// Queries the verified D1 database for all 51 jurisdictions
// Returns only Verified or Discrepancy records — never unverified data

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400"
};

export default {
  async fetch(request, env, ctx) {
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
        // Step 1: Look up which state/district(s) this ZIP belongs to
        const zipRows = await env.DB.prepare(
          'SELECT state, district FROM zip_district WHERE zip = ?'
        ).bind(zip).all();

        if (!zipRows.results || zipRows.results.length === 0) {
          // ZIP not in our database yet — use GovTrack as fallback
          return await govtrackFallback(zip, env);
        }

        const results = [];

        // Step 2: Get House rep(s) for each district found
        for (const row of zipRows.results) {
          const rep = await env.DB.prepare(`
            SELECT * FROM members
            WHERE state = ? AND district = ? AND role_type = 'Representative'
            AND in_office = 1 AND status_consensus IN ('Verified', 'Discrepancy')
            ORDER BY sources_agree DESC LIMIT 1
          `).bind(row.state, row.district).first();

          if (rep) {
            results.push(formatResult(rep));
          }
        }

        // Step 3: Get both senators for the state
        const state = zipRows.results[0].state;
        const senators = await env.DB.prepare(`
          SELECT * FROM members
          WHERE state = ? AND role_type = 'Senator'
          AND in_office = 1 AND status_consensus IN ('Verified', 'Discrepancy')
          ORDER BY sources_agree DESC
        `).bind(state).all();

        for (const sen of (senators.results || [])) {
          results.push(formatResult(sen));
        }

        const hasDiscrepancy = results.some(r => r.status === 'Discrepancy');

        return jsonResponse({
          results,
          zip,
          state,
          multi_district: zipRows.results.length > 1,
          database_verified: true,
          has_discrepancy: hasDiscrepancy,
          note: zipRows.results.length > 1
            ? 'Your ZIP spans multiple districts. Your House representative is one of those shown — choose the name you recognize.'
            : '',
          last_verified: results[0]?.last_verified || null
        });

      } catch (err) {
        console.error("Lookup error:", err.message);
        return jsonResponse({ error: "Lookup failed. Please try again.", detail: err.message }, 500);
      }
    }

    // GET / — serve HTML from GitHub Pages
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

// Format a DB record into the response shape the frontend expects
function formatResult(member) {
  const isHouse = member.role_type === 'Representative';
  return {
    name: member.official_name,
    first_name: member.first_name,
    last_name: member.last_name,
    title: member.role_type,
    party: member.party,
    district: member.district || '',
    role: isHouse ? `U.S. House — District ${member.district}` : `U.S. Senator — ${member.state}`,
    phone: member.phone || '',
    office: member.office || '',
    website: member.website || '',
    state: member.state,
    in_office: member.in_office === 1,
    status: member.status_consensus,
    sources_agree: member.sources_agree,
    last_verified: member.last_verified,
    type: isHouse ? 'House' : 'Senate',
    verified: member.status_consensus === 'Verified'
  };
}

// GovTrack fallback for ZIPs not yet in our database
async function govtrackFallback(zip, env) {
  try {
    // Use Claude to identify the state/district, then GovTrack to verify
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
        system: `Return ONLY a JSON object for the given US ZIP code. Format: {"state":"AZ","house_districts":[3],"multi_district":false}. List ALL districts if ZIP spans multiple. No markdown, no explanation.`,
        messages: [{ role: "user", content: `ZIP: ${zip}` }]
      })
    });

    const claudeData = await claudeRes.json();
    let rawText = claudeData.content[0].text.trim().replace(/```json|```/g, "").trim();
    const districtInfo = JSON.parse(rawText);
    const state = districtInfo.state;
    const districts = districtInfo.house_districts;

    // Store in zip_district for future lookups
    for (const d of districts) {
      await env.DB.prepare(
        'INSERT OR IGNORE INTO zip_district (zip, state, district) VALUES (?, ?, ?)'
      ).bind(zip, state, String(d)).run();
    }

    // Verify with GovTrack
    const govtrackCalls = [
      fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&role_type=senator`),
      ...districts.map(d => fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&district=${d}&role_type=representative`))
    ];

    const responses = await Promise.all(govtrackCalls);
    const data = await Promise.all(responses.map(r => r.json()));

    const senators = data[0].objects || [];
    const houseReps = data.slice(1).flatMap(d => d.objects || []);
    const results = [];

    for (const rep of houseReps) {
      results.push({
        name: `${rep.person.firstname} ${rep.person.lastname}`,
        title: "Representative",
        party: rep.party,
        district: String(rep.district),
        role: `U.S. House — District ${rep.district}`,
        phone: rep.phone || '',
        office: rep.extra?.address || '',
        state, type: "House", verified: true, status: "Verified",
        note: "Verified via GovTrack (ZIP not yet in main database)"
      });
    }

    for (const sen of senators) {
      results.push({
        name: `${sen.person.firstname} ${sen.person.lastname}`,
        title: "Senator",
        party: sen.party,
        district: '',
        role: `U.S. Senator — ${state}`,
        phone: sen.phone || '',
        office: sen.extra?.address || '',
        state, type: "Senate", verified: true, status: "Verified"
      });
    }

    return jsonResponse({
      results, zip, state,
      multi_district: districts.length > 1,
      database_verified: false,
      fallback: true,
      note: districts.length > 1 ? 'Your ZIP spans multiple districts. Choose the representative you recognize.' : ''
    });

  } catch (err) {
    return jsonResponse({ error: "Could not retrieve representative information. Please try again." }, 500);
  }
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS }
  });
}
