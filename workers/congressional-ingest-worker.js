// congressional-ingest-worker.js v2
// Truth-By-Consensus: 4 APIs, 3 of 4 must agree — all 51 jurisdictions

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runIngestion(env));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/ingest') {
      ctx.waitUntil(runIngestion(env));
      return new Response(JSON.stringify({ status: 'Ingestion started', time: new Date().toISOString() }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (url.pathname === '/test') {
      try {
        const res = await fetch(
          'https://api.congress.gov/v3/member?congress=119&limit=5&format=json',
          { headers: { 'X-API-Key': env.CONGRESS_API_KEY } }
        );
        const text = await res.text();
        return new Response(JSON.stringify({
          status: res.status,
          ok: res.ok,
          body_preview: text.substring(0, 500)
        }), { headers: { 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { headers: { 'Content-Type': 'application/json' } });
      }
    }
    if (url.pathname === '/status') {
      const counts = await env.DB.prepare(
        'SELECT status_consensus, COUNT(*) as count FROM members GROUP BY status_consensus'
      ).all();
      const last = await env.DB.prepare(
        'SELECT MAX(last_verified) as last_run FROM members'
      ).first();
      return new Response(JSON.stringify({ counts: counts.results, last_run: last?.last_run || null, total: counts.results.reduce((a,r) => a + r.count, 0) }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response('Congressional Verification Worker — use /ingest or /status');
  }
};

async function runIngestion(env) {
  console.log('Ingestion started:', new Date().toISOString());
  const now = new Date().toISOString();

  try {
    // Pull all four sources in parallel
    const [congressMembers, propublicaSenate, propublicaHouse, legiscanMembers, pluralMembers] = await Promise.all([
      fetchCongressGov(env.CONGRESS_API_KEY),
      fetchProPublica(env.PROPUBLICA_API_KEY, 'senate'),
      fetchProPublica(env.PROPUBLICA_API_KEY, 'house'),
      fetchLegiScan(env.LEGISCAN_API_KEY),
      fetchPlural(env.PLURAL_API_KEY)
    ]);

    const propublicaMembers = [...propublicaSenate, ...propublicaHouse];
    console.log(`Sources — Congress: ${congressMembers.length}, ProPublica: ${propublicaMembers.length}, LegiScan: ${legiscanMembers.length}, Plural: ${pluralMembers.length}`);

    // Build consensus
    const consensusMap = buildConsensus(congressMembers, propublicaMembers, legiscanMembers, pluralMembers);
    console.log(`Consensus map built: ${Object.keys(consensusMap).length} members`);

    let verified = 0, discrepancy = 0, manual = 0;

    for (const [bioguideId, r] of Object.entries(consensusMap)) {
      try {
        await env.DB.prepare(`
          INSERT INTO members (
            bioguide_id, official_name, first_name, last_name, party,
            state, district, role_type, phone, office, website, in_office,
            status_consensus, api_congress, api_propublica, api_legiscan, api_plural,
            sources_agree, last_verified
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_verified = excluded.last_verified
        `).bind(
          bioguideId,
          r.official_name || '',
          r.first_name || '',
          r.last_name || '',
          r.party || '',
          r.state || '',
          r.district || '',
          r.role_type || '',
          r.phone || '',
          r.office || '',
          r.website || '',
          r.in_office ? 1 : 0,
          r.status_consensus,
          r.api_congress ? 1 : 0,
          r.api_propublica ? 1 : 0,
          r.api_legiscan ? 1 : 0,
          r.api_plural ? 1 : 0,
          r.sources_agree,
          now
        ).run();

        if (r.status_consensus === 'Verified') verified++;
        else if (r.status_consensus === 'Discrepancy') discrepancy++;
        else manual++;
      } catch (dbErr) {
        console.error(`DB error for ${bioguideId}:`, dbErr.message);
      }
    }

    console.log(`Ingestion complete — Verified: ${verified}, Discrepancy: ${discrepancy}, Manual: ${manual}`);

  } catch (err) {
    console.error('Ingestion failed:', err.message, err.stack);
  }
}

async function fetchCongressGov(apiKey) {
  const members = [];
  try {
    let offset = 0;
    while (true) {
      const res = await fetch(
        `https://api.congress.gov/v3/member?congress=119&limit=250&offset=${offset}&format=json`,
        { headers: { 'X-API-Key': apiKey } }
      );
      if (!res.ok) { console.error('Congress.gov error:', res.status); break; }
      const data = await res.json();
      const batch = data.members || [];
      if (batch.length === 0) break;
      for (const m of batch) {
        members.push({
          bioguide_id: m.bioguideId,
          official_name: m.name || '',
          first_name: (m.name || '').split(', ')[1] || '',
          last_name: (m.name || '').split(', ')[0] || '',
          party: m.partyName || '',
          state: m.state || '',
          district: m.district !== undefined ? String(m.district) : '',
          role_type: m.terms?.item?.[0]?.memberType === 'Senator' ? 'Senator' : 'Representative',
          in_office: true
        });
      }
      if (batch.length < 250) break;
      offset += 250;
    }
  } catch (err) { console.error('Congress.gov fetch error:', err.message); }
  return members;
}

async function fetchProPublica(apiKey, chamber) {
  const members = [];
  try {
    const res = await fetch(
      `https://api.propublica.org/congress/v1/119/${chamber}/members.json`,
      { headers: { 'X-API-Key': apiKey } }
    );
    if (!res.ok) { console.error(`ProPublica ${chamber} error:`, res.status); return members; }
    const data = await res.json();
    for (const m of (data.results?.[0]?.members || [])) {
      if (!m.in_office) continue;
      members.push({
        bioguide_id: m.id,
        official_name: `${m.last_name}, ${m.first_name}`,
        first_name: m.first_name || '',
        last_name: m.last_name || '',
        party: m.party === 'R' ? 'Republican' : m.party === 'D' ? 'Democrat' : m.party || '',
        state: m.state || '',
        district: m.district || '',
        role_type: chamber === 'senate' ? 'Senator' : 'Representative',
        phone: m.phone || '',
        office: m.office || '',
        website: m.url || '',
        in_office: true
      });
    }
  } catch (err) { console.error(`ProPublica ${chamber} error:`, err.message); }
  return members;
}

async function fetchLegiScan(apiKey) {
  const members = [];
  try {
    // LegiScan session 2096 = 119th US Congress
    const res = await fetch(`https://api.legiscan.com/?key=${apiKey}&op=getSessionPeople&session_id=2096`);
    if (!res.ok) { console.error('LegiScan error:', res.status); return members; }
    const data = await res.json();
    for (const m of (data.sessionpeople?.people || [])) {
      members.push({
        bioguide_id: null, // LegiScan uses its own IDs
        legiscan_id: m.people_id,
        official_name: m.name || '',
        first_name: m.first_name || '',
        last_name: m.last_name || '',
        party: m.party_id === 1 ? 'Democrat' : m.party_id === 2 ? 'Republican' : 'Other',
        state: m.state || '',
        district: m.district || '',
        role_type: (m.role || '').includes('Sen') ? 'Senator' : 'Representative',
        in_office: true
      });
    }
  } catch (err) { console.error('LegiScan error:', err.message); }
  return members;
}

async function fetchPlural(apiKey) {
  const members = [];
  try {
    const res = await fetch(
      `https://v3.openstates.org/people?jurisdiction=us&per_page=100&apikey=${apiKey}`
    );
    if (!res.ok) { console.error('Plural error:', res.status); return members; }
    const data = await res.json();
    for (const m of (data.results || [])) {
      members.push({
        bioguide_id: null,
        official_name: m.name || '',
        first_name: m.given_name || '',
        last_name: m.family_name || '',
        party: m.party?.[0]?.name || '',
        state: m.current_role?.division_id?.split('/')?.pop()?.toUpperCase() || '',
        district: m.current_role?.district || '',
        role_type: (m.current_role?.title || '').includes('Senator') ? 'Senator' : 'Representative',
        phone: m.offices?.[0]?.voice || '',
        website: m.links?.[0]?.url || '',
        in_office: true
      });
    }
  } catch (err) { console.error('Plural error:', err.message); }
  return members;
}

function buildConsensus(congressList, propublicaList, legiscanList, pluralList) {
  const map = {};

  // Congress.gov is the anchor
  for (const m of congressList) {
    if (!m.bioguide_id) continue;
    map[m.bioguide_id] = { ...m, api_congress: true, api_propublica: false, api_legiscan: false, api_plural: false, sources_agree: 1 };
  }

  // ProPublica — matches by bioguide_id directly
  for (const m of propublicaList) {
    if (!m.bioguide_id) continue;
    if (map[m.bioguide_id]) {
      map[m.bioguide_id].api_propublica = true;
      map[m.bioguide_id].sources_agree++;
      if (m.phone) map[m.bioguide_id].phone = m.phone;
      if (m.office) map[m.bioguide_id].office = m.office;
      if (m.website) map[m.bioguide_id].website = m.website;
    } else {
      map[m.bioguide_id] = { ...m, api_congress: false, api_propublica: true, api_legiscan: false, api_plural: false, sources_agree: 1 };
    }
  }

  // LegiScan — match by last name + state
  for (const m of legiscanList) {
    if (!m.last_name || !m.state) continue;
    const match = Object.values(map).find(r =>
      r.last_name?.toLowerCase() === m.last_name?.toLowerCase() &&
      r.state === m.state &&
      r.role_type === m.role_type
    );
    if (match) { match.api_legiscan = true; match.sources_agree++; }
  }

  // Plural — match by last name + state
  for (const m of pluralList) {
    if (!m.last_name || !m.state) continue;
    const match = Object.values(map).find(r =>
      r.last_name?.toLowerCase() === m.last_name?.toLowerCase() &&
      r.state === m.state &&
      r.role_type === m.role_type
    );
    if (match) {
      match.api_plural = true;
      match.sources_agree++;
      if (m.phone && !match.phone) match.phone = m.phone;
      if (m.website && !match.website) match.website = m.website;
    }
  }

  // Apply consensus — 3 of 4 = Verified
  for (const id of Object.keys(map)) {
    const r = map[id];
    if (r.sources_agree >= 3) r.status_consensus = 'Verified';
    else if (r.sources_agree === 2) r.status_consensus = 'Discrepancy';
    else r.status_consensus = 'Manual Review';
  }

  return map;
}
