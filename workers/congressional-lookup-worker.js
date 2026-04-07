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
          return jsonResponse({
            error: "ZIP code not yet in verified database. Please try again after the next scheduled update.",
            zip,
            database_verified: false
          }, 404);
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


function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS }
  });
}
