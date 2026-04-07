// congressional-ingest-worker.js
// Scheduled Worker — runs every 24 hours
// Truth-By-Consensus: queries 4 APIs, requires 3-of-4 agreement to mark Verified
// Covers all 51 jurisdictions (50 states + DC)

const DB_ID = "9d51b85f-5e67-4119-8bf0-595fa5477f1f";

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runIngestion(env));
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/run" && request.method === "POST") {
      await runIngestion(env);
      return new Response(JSON.stringify({ status: "Ingestion complete" }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    return new Response("Congressional Ingest Worker — OK", { status: 200 });
  }
};

async function runIngestion(env) {
  console.log("Starting congressional ingestion:", new Date().toISOString());

  try {
    // Pull all four sources in parallel
    const [congressData, propublicaData, legiscanData, pluralData] = await Promise.allSettled([
      fetchCongress(env.CONGRESS_API_KEY),
      fetchPropublica(env.PROPUBLICA_API_KEY),
      fetchLegiscan(env.LEGISCAN_API_KEY),
      fetchPlural(env.PLURAL_API_KEY)
    ]);

    // Build lookup maps keyed by bioguide_id
    const congressMap = congressData.status === "fulfilled" ? congressData.value : {};
    const propublicaMap = propublicaData.status === "fulfilled" ? propublicaData.value : {};
    const legiscanMap = legiscanData.status === "fulfilled" ? legiscanData.value : {};
    const pluralMap = pluralData.status === "fulfilled" ? pluralData.value : {};

    // Congress.gov is the anchor — iterate over its members as the base set
    const allBioguides = Object.keys(congressMap);
    console.log(`Congress.gov returned ${allBioguides.length} members`);

    let verified = 0, discrepancy = 0, vacant = 0;

    for (const bioguide_id of allBioguides) {
      const cm = congressMap[bioguide_id];
      const pm = propublicaMap[bioguide_id];
      const lm = legiscanMap[bioguide_id];
      const plm = pluralMap[bioguide_id];

      // Truth-By-Consensus: count how many sources say in_office = true
      const votes = [
        cm?.in_office ? 1 : 0,
        pm?.in_office ? 1 : 0,
        lm?.in_office ? 1 : 0,
        plm?.in_office ? 1 : 0
      ];
      const agreeing = votes.reduce((a, b) => a + b, 0);

      let status;
      if (agreeing >= 3) status = "Verified";
      else if (agreeing === 0) status = "Vacant";
      else status = "Discrepancy";

      if (status === "Verified") verified++;
      else if (status === "Vacant") vacant++;
      else discrepancy++;

      const apiFlags = JSON.stringify({
        congress: cm?.in_office ? "Active" : "Inactive",
        propublica: pm?.in_office ? "Active" : "Inactive",
        legiscan: lm?.in_office ? "Active" : "Inactive",
        plural: plm?.in_office ? "Active" : "Inactive"
      });

      // Upsert into D1 — Congress.gov is authoritative for names/contact
      await env.DB.prepare(`
        INSERT INTO members (
          bioguide_id, official_name, first_name, last_name, party, state, district,
          role_type, phone, office, status_consensus,
          in_office_congress, in_office_propublica, in_office_legiscan, in_office_plural,
          sources_agree, api_flags, last_verified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(bioguide_id) DO UPDATE SET
          official_name = excluded.official_name,
          first_name = excluded.first_name,
          last_name = excluded.last_name,
          party = excluded.party,
          state = excluded.state,
          district = excluded.district,
          role_type = excluded.role_type,
          phone = excluded.phone,
          office = excluded.office,
          status_consensus = excluded.status_consensus,
          in_office_congress = excluded.in_office_congress,
          in_office_propublica = excluded.in_office_propublica,
          in_office_legiscan = excluded.in_office_legiscan,
          in_office_plural = excluded.in_office_plural,
          sources_agree = excluded.sources_agree,
          api_flags = excluded.api_flags,
          last_verified = datetime('now')
      `).bind(
        bioguide_id,
        cm.official_name || "",
        cm.first_name || "",
        cm.last_name || "",
        cm.party || pm?.party || "",
        cm.state || "",
        cm.district || "",
        cm.role_type || "",
        cm.phone || pm?.phone || "",
        cm.office || "",
        status,
        cm?.in_office ? 1 : 0,
        pm?.in_office ? 1 : 0,
        lm?.in_office ? 1 : 0,
        plm?.in_office ? 1 : 0,
        agreeing,
        apiFlags
      ).run();

      // Log verification result
      await env.DB.prepare(`
        INSERT INTO verification_log (bioguide_id, congress_status, propublica_status, legiscan_status, plural_status, result)
        VALUES (?, ?, ?, ?, ?, ?)
      `).bind(
        bioguide_id,
        cm?.in_office ? "Active" : "Inactive",
        pm?.in_office ? "Active" : "Inactive",
        lm?.in_office ? "Active" : "Inactive",
        plm?.in_office ? "Active" : "Inactive",
        status
      ).run();
    }

    console.log(`Ingestion complete — Verified: ${verified}, Discrepancy: ${discrepancy}, Vacant: ${vacant}`);

  } catch (err) {
    console.error("Ingestion error:", err.message);
  }
}

// SOURCE 1: Congress.gov API
async function fetchCongress(apiKey) {
  const map = {};
  let offset = 0;
  const limit = 250;

  while (true) {
    const url = `https://api.congress.gov/v3/member?limit=${limit}&offset=${offset}&currentMember=true&api_key=${apiKey}`;
    const res = await fetch(url);
    if (!res.ok) break;
    const data = await res.json();
    const members = data.members || [];
    if (members.length === 0) break;

    for (const m of members) {
      const bioguide = m.bioguideId;
      if (!bioguide) continue;

      // Get terms to find current role
      const terms = m.terms?.item || [];
      const currentTerm = terms[terms.length - 1] || {};
      const isSenator = currentTerm.chamber === "Senate";
      const district = isSenator ? "" : String(currentTerm.district || "");

      map[bioguide] = {
        in_office: true,
        official_name: m.name || `${m.firstName} ${m.lastName}`,
        first_name: m.firstName || "",
        last_name: m.lastName || "",
        party: m.partyName || "",
        state: m.state || "",
        district,
        role_type: isSenator ? "senator" : "representative",
        phone: "",
        office: ""
      };
    }

    offset += limit;
    if (members.length < limit) break;
  }

  return map;
}

// SOURCE 2: ProPublica Congress API
async function fetchPropublica(apiKey) {
  const map = {};
  const chambers = ["senate", "house"];

  for (const chamber of chambers) {
    const url = `https://api.propublica.org/congress/v1/119/${chamber}/members.json`;
    const res = await fetch(url, { headers: { "X-API-Key": apiKey } });
    if (!res.ok) continue;
    const data = await res.json();
    const members = data.results?.[0]?.members || [];

    for (const m of members) {
      if (!m.id) continue;
      map[m.id] = {
        in_office: m.in_office === true,
        party: m.party === "R" ? "Republican" : m.party === "D" ? "Democrat" : m.party,
        phone: m.phone || "",
        district: m.district || ""
      };
    }
  }

  return map;
}

// SOURCE 3: LegiScan API
async function fetchLegiscan(apiKey) {
  const map = {};

  const url = `https://api.legiscan.com/?key=${apiKey}&op=getSessionList&state=US`;
  const res = await fetch(url);
  if (!res.ok) return map;
  const data = await res.json();

  const sessions = data.sessions || [];
  const session119 = sessions.find(s => s.session_name && s.session_name.includes("119"));
  if (!session119) return map;

  const peopleUrl = `https://api.legiscan.com/?key=${apiKey}&op=getSessionPeople&id=${session119.session_id}`;
  const peopleRes = await fetch(peopleUrl);
  if (!peopleRes.ok) return map;
  const peopleData = await peopleRes.json();

  const people = peopleData.sessionpeople?.people || [];
  for (const p of people) {
    if (!p.bioguide_id) continue;
    map[p.bioguide_id] = {
      in_office: p.active === 1,
      party: p.party_id === 1 ? "Democrat" : p.party_id === 2 ? "Republican" : "Other"
    };
  }

  return map;
}

// SOURCE 4: Plural (Open States) API
async function fetchPlural(apiKey) {
  const map = {};

  const query = `{
    people(memberOf: "ocd-organization/united-states-of-america", first: 600) {
      edges {
        node {
          id
          name
          party: primaryParty
          currentRole { active district title }
          identifiers { identifier scheme }
          contactDetails { type value }
        }
      }
    }
  }`;

  const res = await fetch("https://v3.openstates.org/graphql", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-KEY": apiKey
    },
    body: JSON.stringify({ query })
  });

  if (!res.ok) return map;
  const data = await res.json();
  const edges = data.data?.people?.edges || [];

  for (const edge of edges) {
    const node = edge.node;
    const bioguide = node.identifiers?.find(i => i.scheme === "bioguide")?.identifier;
    if (!bioguide) continue;

    const phone = node.contactDetails?.find(c => c.type === "voice")?.value || "";

    map[bioguide] = {
      in_office: node.currentRole?.active === true,
      party: node.party || "",
      phone
    };
  }

  return map;
}
