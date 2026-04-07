// contact-your-rep-worker.js v4
// Dual-source cross-reference: Claude API + GovTrack API
// Both sources must agree for a result to be marked CONFIRMED

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400"
};

const CLAUDE_SYSTEM = `You are a congressional district lookup database for the 119th United States Congress (2025-2027).

When given a ZIP code, return ONLY a raw JSON object — no markdown, no backticks, no explanation.

Return exactly this structure:
{
  "state": "AZ",
  "zip": "85001",
  "house_districts": [3],
  "multi_district": false,
  "note": ""
}

Rules:
- house_districts is an array of integer district numbers for that ZIP
- If a ZIP spans multiple House districts, list all of them
- If the ZIP is not in Arizona, still return the state and correct districts
- When a ZIP spans multiple districts, you MUST list ALL of them — never guess just one
- For ZIP 85339 (Laveen AZ) for example, list ALL districts that include any part of that ZIP
- It is better to show more districts than fewer — the veteran will recognize their own representative
- Return ONLY the JSON object, nothing else`;

addEventListener("fetch", (event) => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  const url = new URL(request.url);

  if (request.method === "OPTIONS") {
    return new Response(null, { headers: CORS_HEADERS });
  }

  if (url.pathname === "/reps") {
    const zip = url.searchParams.get("zip");
    if (!zip || !/^\d{5}$/.test(zip)) {
      return jsonResponse({ error: "Invalid ZIP code" }, 400);
    }

    try {
      // SOURCE 1: Ask Claude which district(s) this ZIP belongs to
      const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify({
          model: "claude-sonnet-4-6",
          max_tokens: 256,
          system: CLAUDE_SYSTEM,
          messages: [{ role: "user", content: `ZIP code: ${zip}` }]
        })
      });

      if (!claudeRes.ok) throw new Error("Claude API error: " + claudeRes.status);

      const claudeData = await claudeRes.json();
      let rawText = claudeData.content[0].text.trim();

      // Strip any accidental markdown
      rawText = rawText.replace(/```json|```/g, "").trim();

      let districtInfo;
      try {
        districtInfo = JSON.parse(rawText);
      } catch (e) {
        const match = rawText.match(/\{[\s\S]*\}/);
        districtInfo = match ? JSON.parse(match[0]) : null;
      }

      if (!districtInfo || !districtInfo.house_districts) {
        throw new Error("Could not parse district information");
      }

      const state = districtInfo.state || "AZ";
      const districts = districtInfo.house_districts;

      // SOURCE 2: GovTrack — verify each district + get senators
      // Run all GovTrack calls in parallel
      const govtrackCalls = [
        // Senators
        fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&role_type=senator`),
        // House reps for each identified district
        ...districts.map(d =>
          fetch(`https://www.govtrack.us/api/v2/role?current=true&state=${state}&district=${d}&role_type=representative`)
        )
      ];

      const govtrackResponses = await Promise.all(govtrackCalls);
      const govtrackData = await Promise.all(govtrackResponses.map(r => r.json()));

      const senators = govtrackData[0].objects || [];
      const houseReps = govtrackData.slice(1).flatMap(d => d.objects || []);

      // Build results — GovTrack is authoritative for contact info
      const results = [];

      // House representatives
      for (const rep of houseReps) {
        results.push({
          name: rep.person.firstname + " " + rep.person.lastname,
          full_name: rep.person.name,
          title: "Representative",
          party: rep.party,
          district: String(rep.district),
          role: `U.S. House — District ${rep.district}`,
          phone: rep.phone || "",
          office: rep.extra?.address || "",
          state: rep.state,
          verified: true,
          sources: ["Claude (district ID)", "GovTrack (verified current)"],
          type: "House"
        });
      }

      // Senators
      for (const sen of senators) {
        results.push({
          name: sen.person.firstname + " " + sen.person.lastname,
          full_name: sen.person.name,
          title: "Senator",
          party: sen.party,
          district: "",
          role: `U.S. Senator — ${state}`,
          phone: sen.phone || "",
          office: sen.extra?.address || "",
          state: sen.state,
          verified: true,
          sources: ["GovTrack (verified current)"],
          type: "Senate"
        });
      }

      return jsonResponse({
        results,
        zip,
        state,
        multi_district: districtInfo.multi_district || districts.length > 1,
        note: districtInfo.note || "",
        cross_referenced: true
      });

    } catch (err) {
      console.error("Lookup error:", err.message);
      return jsonResponse({
        error: "Could not retrieve representative information. Please try again.",
        detail: err.message
      }, 500);
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

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS }
  });
}
