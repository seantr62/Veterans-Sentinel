// congressional-ingest-worker.js
// Scheduled Worker — runs every 24 hours
// Truth-By-Consensus: queries 4 APIs, verifies across 3 of 4
// Covers all 51 jurisdictions (50 states + DC)

const DB_NAME = 'congressional-verified';

// All 51 state/territory codes
const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
  'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
];

export default {
  // Scheduled trigger — runs every 24 hours
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runIngestion(env));
  },

  // Manual trigger via GET /ingest for testing
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/ingest') {
      ctx.waitUntil(runIngestion(env));
      return new Response(JSON.stringify({ status: 'Ingestion started', time: new Date().toISOString() }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }
    if (url.pathname === '/status') {
      const result = await env.DB.prepare(
        'SELECT status_consensus, COUNT(*) as count FROM members GROUP BY status_consensus'
      ).all();
      const lastRun = await env.DB.prepare(
        'SELECT MAX(check_time) as last_run FROM verification_log'
      ).first();
      return new Response(JSON.stringify({ counts: result.results, last_run: lastRun }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }
    return new Response('Congressional Verification Worker — use /ingest or /status', { status: 200 });
  }
};

async function runIngestion(env) {
  console.log('Starting congressional ingestion:', new Date().toISOString());

  try {
    // SOURCE 1: Congress.gov — The Anchor
    const congressMembers = await fetchCongressGov(env.CONGRESS_API_KEY);
    console.log(`Congress.gov: ${congressMembers.length} members`);

    // SOURCE 2: ProPublica — The Pulse
    const propublicaMembers = await fetchProPublica(env.PROPUBLICA_API_KEY);
    console.log(`ProPublica: ${propublicaMembers.length} members`);

    // SOURCE 3: LegiScan — The Cross-Check
    const legiscanMembers = await fetchLegiScan(env.LEGISCAN_API_KEY);
    console.log(`LegiScan: ${legiscanMembers.length} members`);

    // SOURCE 4: Plural — The Public Advocate
    const pluralMembers = await fetchPlural(env.PLURAL_API_KEY);
    console.log(`Plural: ${pluralMembers.length} members`);

    // Build consensus map keyed by bioguide_id
    const consensusMap = buildConsensus(
      congressMembers,
      propublicaMembers,
      legiscanMembers,
      pluralMembers
    );

    // Write verified records to D1
    let verified = 0, discrepancy = 0;
    for (const [bioguideId, record] of Object.entries(consensusMap)) {
      await env.DB.prepare(`
        INSERT INTO members (
          bioguide_id, official_name, first_name, last_name, party,
          state, district, role_type, phone, office, website, in_office,
          status_consensus, api_congress, api_propublica, api_legiscan, api_plural,
          sources_agree, last_verified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(bioguide_id) DO UPDATE SET
          official_name = excluded.official_name,
          party = excluded.party,
          state = excluded.state,
          district = excluded.district,
          phone = excluded.phone,
          office = excluded.office,
          website = excluded.website,
          in_office = excluded.in_office,
          status_consensus = excluded.status_consensus,
          api_congress = excluded.api_congress,
          api_propublica = excluded.api_propublica,
          api_legiscan = excluded.api_legiscan,
          api_plural = excluded.api_plural,
          sources_agree = excluded.sources_agree,
          last_verified = datetime('now')
      `).bind(
        bioguideId,
        record.official_name,
        record.first_name,
        record.last_name,
        record.party,
        record.state,
        record.district,
        record.role_type,
        record.phone || '',
        record.office || '',
        record.website || '',
        record.in_office ? 1 : 0,
        record.status_consensus,
        record.api_congress ? 1 : 0,
        record.api_propublica ? 1 : 0,
        record.api_legiscan ? 1 : 0,
        record.api_plural ? 1 : 0,
        record.sources_agree,
        datetime('now')
      ).run();

      // Log verification
      await env.DB.prepare(`
        INSERT INTO verification_log (bioguide_id, api_congress, api_propublica, api_legiscan, api_plural, consensus, flag)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).bind(
        bioguideId,
        record.api_congress ? 'Active' : 'Not Found',
        record.api_propublica ? 'Active' : 'Not Found',
        record.api_legiscan ? 'Active' : 'Not Found',
        record.api_plural ? 'Active' : 'Not Found',
        record.status_consensus,
        record.status_consensus === 'Discrepancy' ? 'Manual Review Required' : null
      ).run();

      if (record.status_consensus === 'Verified') verified++;
      else discrepancy++;
    }

    console.log(`Ingestion complete — Verified: ${verified}, Discrepancy: ${discrepancy}`);

  } catch (err) {
    console.error('Ingestion error:', err.message);
  }
}

// SOURCE 1: Congress.gov API
async function fetchCongressGov(apiKey) {
  const members = [];
  try {
    // Current 119th Congress members
    const res = await fetch(
      `https://api.congress.gov/v3/member?congress=119&limit=250&format=json`,
      { headers: { 'X-API-Key': apiKey } }
    );
    if (!res.ok) throw new Error('Congress.gov API error: ' + res.status);
    const data = await res.json();
    for (const m of (data.members || [])) {
      members.push({
        bioguide_id: m.bioguideId,
        official_name: m.name,
        first_name: m.firstName || '',
        last_name: m.lastName || '',
        party: m.partyName || '',
        state: m.state || '',
        district: m.district ? String(m.district) : '',
        role_type: m.terms?.item?.[0]?.memberType || '',
        in_office: m.terms?.item?.[0]?.endYear >= 2027,
        source: 'congress'
      });
    }
  } catch (err) {
    console.error('Congress.gov fetch error:', err.message);
  }
  return members;
}

// SOURCE 2: ProPublica Congress API
async function fetchProPublica(apiKey) {
  const members = [];
  try {
    for (const chamber of ['senate', 'house']) {
      const res = await fetch(
        `https://api.propublica.org/congress/v1/119/${chamber}/members.json`,
        { headers: { 'X-API-Key': apiKey } }
      );
      if (!res.ok) continue;
      const data = await res.json();
      for (const m of (data.results?.[0]?.members || [])) {
        if (!m.in_office) continue;
        members.push({
          bioguide_id: m.id,
          official_name: `${m.first_name} ${m.last_name}`,
          first_name: m.first_name || '',
          last_name: m.last_name || '',
          party: m.party === 'R' ? 'Republican' : m.party === 'D' ? 'Democrat' : m.party,
          state: m.state || '',
          district: m.district || '',
          role_type: chamber === 'senate' ? 'Senator' : 'Representative',
          phone: m.phone || '',
          office: m.office || '',
          website: m.url || '',
          in_office: true,
          source: 'propublica'
        });
      }
    }
  } catch (err) {
    console.error('ProPublica fetch error:', err.message);
  }
  return members;
}

// SOURCE 3: LegiScan API
async function fetchLegiScan(apiKey) {
  const members = [];
  try {
    const res = await fetch(
      `https://api.legiscan.com/?key=${apiKey}&op=getSessionPeople&session_id=2096`
    );
    if (!res.ok) throw new Error('LegiScan API error: ' + res.status);
    const data = await res.json();
    for (const m of (data.sessionpeople?.people || [])) {
      members.push({
        bioguide_id: m.people_id ? `LS_${m.people_id}` : null,
        official_name: m.name || '',
        first_name: m.first_name || '',
        last_name: m.last_name || '',
        party: m.party_id === 1 ? 'Democrat' : m.party_id === 2 ? 'Republican' : 'Other',
        state: m.state_id || '',
        district: m.district || '',
        role_type: m.role || '',
        in_office: true,
        source: 'legiscan'
      });
    }
  } catch (err) {
    console.error('LegiScan fetch error:', err.message);
  }
  return members;
}

// SOURCE 4: Plural (Open States) API
async function fetchPlural(apiKey) {
  const members = [];
  try {
    const res = await fetch(
      'https://v3.openstates.org/people?jurisdiction=us&per_page=100&apikey=' + apiKey
    );
    if (!res.ok) throw new Error('Plural API error: ' + res.status);
    const data = await res.json();
    for (const m of (data.results || [])) {
      members.push({
        bioguide_id: m.openstates_url?.split('/').pop() || null,
        official_name: m.name || '',
        first_name: m.given_name || '',
        last_name: m.family_name || '',
        party: m.party?.[0]?.name || '',
        state: m.jurisdiction?.name || '',
        district: m.current_role?.district || '',
        role_type: m.current_role?.title || '',
        phone: m.offices?.[0]?.voice || '',
        website: m.links?.[0]?.url || '',
        in_office: true,
        source: 'plural'
      });
    }
  } catch (err) {
    console.error('Plural fetch error:', err.message);
  }
  return members;
}

// Truth-By-Consensus — 3 of 4 sources must agree
function buildConsensus(congressList, propublicaList, legiscanList, pluralList) {
  const map = {};

  // Congress.gov is the anchor — start with its data
  for (const m of congressList) {
    if (!m.bioguide_id) continue;
    map[m.bioguide_id] = {
      ...m,
      api_congress: true,
      api_propublica: false,
      api_legiscan: false,
      api_plural: false,
      sources_agree: 1,
      status_consensus: 'Pending'
    };
  }

  // Cross-reference ProPublica by bioguide_id
  for (const m of propublicaList) {
    if (!m.bioguide_id) continue;
    if (map[m.bioguide_id]) {
      map[m.bioguide_id].api_propublica = true;
      map[m.bioguide_id].sources_agree++;
      // ProPublica has better phone/office data — merge it
      if (m.phone) map[m.bioguide_id].phone = m.phone;
      if (m.office) map[m.bioguide_id].office = m.office;
      if (m.website) map[m.bioguide_id].website = m.website;
    } else {
      map[m.bioguide_id] = { ...m, api_congress: false, api_propublica: true, api_legiscan: false, api_plural: false, sources_agree: 1, status_consensus: 'Pending' };
    }
  }

  // Cross-reference LegiScan by name matching
  for (const m of legiscanList) {
    const match = Object.values(map).find(r =>
      r.last_name && m.last_name &&
      r.last_name.toLowerCase() === m.last_name.toLowerCase() &&
      r.state === m.state
    );
    if (match) {
      match.api_legiscan = true;
      match.sources_agree++;
    }
  }

  // Cross-reference Plural by name matching
  for (const m of pluralList) {
    const match = Object.values(map).find(r =>
      r.last_name && m.last_name &&
      r.last_name.toLowerCase() === m.last_name.toLowerCase() &&
      r.state === m.state
    );
    if (match) {
      match.api_plural = true;
      match.sources_agree++;
      if (m.phone && !match.phone) match.phone = m.phone;
      if (m.website && !match.website) match.website = m.website;
    }
  }

  // Apply consensus logic — 3 of 4 = Verified
  for (const id of Object.keys(map)) {
    const r = map[id];
    if (r.sources_agree >= 3) {
      r.status_consensus = 'Verified';
    } else if (r.sources_agree === 2) {
      r.status_consensus = 'Discrepancy';
    } else {
      r.status_consensus = 'Manual Review';
    }
  }

  return map;
}
