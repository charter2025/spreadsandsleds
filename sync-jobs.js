// sync-jobs.js — Front Office Jobs Board
// Sources: Greenhouse ATS + Lever ATS + eFinancialCareers RSS
// Batch classification via Claude Haiku (20 roles per API call)
// Runs daily via GitHub Actions cron

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

async function fetchWithTimeout(url, options = {}, ms = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    clearTimeout(t);
    return res;
  } catch (e) { clearTimeout(t); throw e; }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE FIRMS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  { slug: 'point72',              name: 'Point72' },
  { slug: 'janestreet',           name: 'Jane Street' },
  { slug: 'mangroup',             name: 'Man Group' },
  { slug: 'lazard',               name: 'Lazard' },
  { slug: 'evercore',             name: 'Evercore' },
  { slug: 'evercoregroup',        name: 'Evercore' },
  { slug: 'moelis',               name: 'Moelis & Company' },
  { slug: 'pwp',                  name: 'Perella Weinberg Partners' },
  { slug: 'pwpartners',           name: 'Perella Weinberg Partners' },
  { slug: 'houlihanlokeyinc',     name: 'Houlihan Lokey' },
  { slug: 'houlihanlokey',        name: 'Houlihan Lokey' },
  { slug: 'jefferies',            name: 'Jefferies' },
  { slug: 'jefferiesllc',         name: 'Jefferies' },
  { slug: 'guggenheimpartners',   name: 'Guggenheim Partners' },
  { slug: 'guggenheim',           name: 'Guggenheim Partners' },
  { slug: 'baird',                name: 'Baird' },
  { slug: 'rwbaird',              name: 'Baird' },
  { slug: 'pipersandler',         name: 'Piper Sandler' },
  { slug: 'stifel',               name: 'Stifel' },
  { slug: 'cowen',                name: 'Cowen' },
  { slug: 'tdcowen',              name: 'TD Cowen' },
  { slug: 'nuveen',               name: 'Nuveen' },
  { slug: 'williamblair',         name: 'William Blair' },
  { slug: 'oppenheimer',          name: 'Oppenheimer' },
  { slug: 'rbc',                  name: 'RBC Capital Markets' },
  { slug: 'rbccm',                name: 'RBC Capital Markets' },
  { slug: 'bmocm',                name: 'BMO Capital Markets' },
  { slug: 'bmo',                  name: 'BMO Capital Markets' },
  { slug: 'tdsecurities',         name: 'TD Securities' },
  { slug: 'scotiabank',           name: 'Scotiabank' },
  { slug: 'natixis',              name: 'Natixis' },
  { slug: 'mizuho',               name: 'Mizuho' },
  { slug: 'mizuhofs',             name: 'Mizuho' },
  { slug: 'smbc',                 name: 'SMBC' },
  { slug: 'nomura',               name: 'Nomura' },
  { slug: 'macquarie',            name: 'Macquarie' },
  { slug: 'macquariegroup',       name: 'Macquarie' },
  { slug: 'cantorfitzgerald',     name: 'Cantor Fitzgerald' },
  { slug: 'drw',                  name: 'DRW' },
  { slug: 'drwtrading',           name: 'DRW' },
  { slug: 'optiver',              name: 'Optiver' },
  { slug: 'susquehanna',          name: 'Susquehanna (SIG)' },
  { slug: 'sig',                  name: 'Susquehanna (SIG)' },
  { slug: 'twosigma',             name: 'Two Sigma' },
  { slug: 'twosigmainvestments',  name: 'Two Sigma' },
  { slug: 'deshawgroup',          name: 'D.E. Shaw' },
  { slug: 'deshaw',               name: 'D.E. Shaw' },
  { slug: 'hudsonrivertrading',   name: 'Hudson River Trading' },
  { slug: 'hrt',                  name: 'Hudson River Trading' },
  { slug: 'pimco',                name: 'PIMCO' },
  { slug: 'virtu',                name: 'Virtu Financial' },
  { slug: 'virtufinancial',       name: 'Virtu Financial' },
  { slug: 'akunacapital',         name: 'Akuna Capital' },
  { slug: 'imc',                  name: 'IMC Trading' },
  { slug: 'squarepointcapital',   name: 'Squarepoint Capital' },
  { slug: 'millennium',           name: 'Millennium Management' },
  { slug: 'millenniummanagement', name: 'Millennium Management' },
  { slug: 'aqr',                  name: 'AQR Capital Management' },
  { slug: 'aqrcapital',           name: 'AQR Capital Management' },
  { slug: 'bridgewater',          name: 'Bridgewater Associates' },
  { slug: 'wintongroup',          name: 'Winton Group' },
  { slug: 'balyasny',             name: 'Balyasny Asset Management' },
  { slug: 'balyasnyam',           name: 'Balyasny Asset Management' },
  { slug: 'marshallwace',         name: 'Marshall Wace' },
  { slug: 'citadelam',            name: 'Citadel' },
  { slug: 'citadelllc',           name: 'Citadel' },
  { slug: 'fidelity',             name: 'Fidelity Investments' },
  { slug: 'fidelityinvestments',  name: 'Fidelity Investments' },
  { slug: 'troweprice',           name: 'T. Rowe Price' },
  { slug: 'invesco',              name: 'Invesco' },
  { slug: 'franklintempleton',    name: 'Franklin Templeton' },
  { slug: 'pgim',                 name: 'PGIM' },
  { slug: 'westernasset',         name: 'Western Asset Management' },
  { slug: 'alliancebernstein',    name: 'AllianceBernstein' },
  { slug: 'ab',                   name: 'AllianceBernstein' },
  { slug: 'neubergerberman',      name: 'Neuberger Berman' },
  { slug: 'loomissayles',         name: 'Loomis Sayles' },
  { slug: 'dimensional',          name: 'Dimensional Fund Advisors' },
  { slug: 'dfa',                  name: 'Dimensional Fund Advisors' },
  { slug: 'wellington',           name: 'Wellington Management' },
  { slug: 'wellingtonmanagement', name: 'Wellington Management' },
  { slug: 'mfs',                  name: 'MFS Investment Management' },
  { slug: 'gmo',                  name: 'GMO' },
  { slug: 'kkr',                  name: 'KKR' },
  { slug: 'kkrecruitment',        name: 'KKR' },
  { slug: 'apolloglobal',         name: 'Apollo Global Management' },
  { slug: 'apollo',               name: 'Apollo Global Management' },
  { slug: 'carlyle',              name: 'The Carlyle Group' },
  { slug: 'thecarlylegroup',      name: 'The Carlyle Group' },
  { slug: 'tpg',                  name: 'TPG Capital' },
  { slug: 'warburgpincus',        name: 'Warburg Pincus' },
  { slug: 'silverlake',           name: 'Silver Lake' },
  { slug: 'silverlakepartners',   name: 'Silver Lake' },
  { slug: 'generalatlantic',      name: 'General Atlantic' },
  { slug: 'golubcapital',         name: 'Golub Capital' },
  { slug: 'aresmanagement',       name: 'Ares Management' },
  { slug: 'ares',                 name: 'Ares Management' },
  { slug: 'blueowl',              name: 'Blue Owl Capital' },
  { slug: 'brookfield',           name: 'Brookfield Asset Management' },
  { slug: 'oaktreecapital',       name: 'Oaktree Capital' },
  { slug: 'oaktree',              name: 'Oaktree Capital' },
  { slug: 'hps',                  name: 'HPS Investment Partners' },
  { slug: 'cerberuscapital',      name: 'Cerberus Capital' },
  { slug: 'blackrock',            name: 'BlackRock' },
  { slug: 'blackrockjobs',        name: 'BlackRock' },
  { slug: 'grahamcapital',        name: 'Graham Capital Management' },
  { slug: 'tudor',                name: 'Tudor Investment Corp' },
  { slug: 'renaissance',          name: 'Renaissance Technologies' },
  { slug: 'elliotmgmt',           name: 'Elliott Management' },
  { slug: 'pershing',             name: 'Pershing Square' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// LEVER FIRMS
// API: https://api.lever.co/v0/postings/{slug}?mode=json
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const LEVER_FIRMS = [
  { slug: 'citadel',             name: 'Citadel' },
  { slug: 'citadelsecurities',   name: 'Citadel Securities' },
  { slug: 'blackstone',          name: 'Blackstone' },
  { slug: 'goldmansachs',        name: 'Goldman Sachs' },
  { slug: 'morganstanley',       name: 'Morgan Stanley' },
  { slug: 'jpmorgan',            name: 'JPMorgan' },
  { slug: 'coatue',              name: 'Coatue Management' },
  { slug: 'iconiqcapital',       name: 'ICONIQ Capital' },
  { slug: 'andreessen',          name: 'Andreessen Horowitz' },
  { slug: 'sequoia',             name: 'Sequoia Capital' },
  { slug: 'lightspeed',          name: 'Lightspeed' },
  { slug: 'accel',               name: 'Accel' },
  { slug: 'insight',             name: 'Insight Partners' },
  { slug: 'vistaequitypartners', name: 'Vista Equity Partners' },
  { slug: 'thoma',               name: 'Thoma Bravo' },
  { slug: 'thomabravo',          name: 'Thoma Bravo' },
  { slug: 'warburg',             name: 'Warburg Pincus' },
  { slug: 'advent',              name: 'Advent International' },
  { slug: 'bain',                name: 'Bain Capital' },
  { slug: 'baincapital',         name: 'Bain Capital' },
  { slug: 'charlesbank',         name: 'Charlesbank Capital' },
  { slug: 'francisco',           name: 'Francisco Partners' },
  { slug: 'ga',                  name: 'General Atlantic' },
  { slug: 'hggc',                name: 'HGGC' },
  { slug: 'nea',                 name: 'NEA' },
  { slug: 'norwestventure',      name: 'Norwest Venture Partners' },
  { slug: 'summit',              name: 'Summit Partners' },
  { slug: 'ta',                  name: 'TA Associates' },
  { slug: 'taassociates',        name: 'TA Associates' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// eFINANCIALCAREERS RSS — finance-specific job board
// No API key needed, public RSS feed
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const EFC_QUERIES = [
  'sales+trading',
  'investment+banking+analyst',
  'investment+banking+associate',
  'portfolio+manager',
  'equity+research',
  'credit+research',
  'quantitative+researcher',
  'quantitative+trader',
  'private+equity+associate',
  'fixed+income+trader',
  'FX+trader',
  'credit+trader',
  'macro+strategist',
  'hedge+fund+analyst',
  'prime+brokerage',
  'structured+finance',
  'leveraged+finance',
  'DCM+analyst',
  'ECM+analyst',
  'wealth+management+advisor',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BATCH CLAUDE CLASSIFICATION — 20 roles per call
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyBatch(roles) {
  const list = roles.map((r, i) => `${i}: ${r.title} @ ${r.firm}`).join('\n');

  const prompt = `Classify these finance job titles for a front office jobs board.

INCLUDE (fo: true): trading, sales & trading, investment banking (M&A/ECM/DCM/LevFin), 
asset/fund management, private equity, equity/credit research, quant research (alpha/investment focused),
wealth/private banking, capital markets origination, macro strategy, structured finance, securitization.

EXCLUDE (fo: false): software/tech engineers (Java, Python, C++, DevOps, SRE, infrastructure),
operations, settlements, compliance, legal, HR, accounting, back office.

RULE: "Engineer" or "Developer" in title = EXCLUDE, even at a hedge fund.
"Researcher", "Analyst", "Trader", "PM", "Banker", "Sales" = INCLUDE.

Respond with exactly one JSON per line, no other text:
{"i":0,"fo":true,"fn":"S&T","lv":"Analyst"}

fn options: S&T, IBD, AM, PE, RM, PB (null if unclear)
lv options: Analyst, Associate, VP, Director, MD, Partner (null if unclear)

Jobs:
${list}`;

  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 800,
      messages: [{ role: 'user', content: prompt }]
    });

    const lines = res.content[0].text.trim().split('\n');
    const results = {};
    for (const line of lines) {
      try {
        const clean = line.trim().replace(/```json|```/g, '').trim();
        if (!clean.startsWith('{')) continue;
        const obj = JSON.parse(clean);
        if (typeof obj.i === 'number') {
          results[obj.i] = { is_front_office: !!obj.fo, function: obj.fn || null, level: obj.lv || null };
        }
      } catch (e) {}
    }
    roles.forEach((_, i) => {
      if (!results[i]) results[i] = { is_front_office: false, function: null, level: null };
    });
    return results;
  } catch (e) {
    console.warn('  Batch classification failed:', e.message);
    const fallback = {};
    roles.forEach((_, i) => { fallback[i] = { is_front_office: false, function: null, level: null }; });
    return fallback;
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SHARED: batch insert helper
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function insertBatch(roles, source, firmName) {
  const BATCH = 20;
  let added = 0;

  // Get existing source_ids in one query
  const sourceIds = roles.map(r => r.source_id);
  const { data: existing } = await supabase
    .from('jobs').select('source_id').in('source_id', sourceIds);
  const existingSet = new Set((existing || []).map(e => e.source_id));
  const newRoles = roles.filter(r => !existingSet.has(r.source_id));
  if (!newRoles.length) return 0;

  for (let i = 0; i < newRoles.length; i += BATCH) {
    const batch = newRoles.slice(i, i + BATCH);
    const batchInput = batch.map(r => ({ id: r.source_id, title: r.title, firm: r.firm || firmName }));
    const classifications = await classifyBatch(batchInput);

    const toInsert = [];
    batch.forEach((role, idx) => {
      const cl = classifications[idx];
      if (!cl?.is_front_office) return;
      toInsert.push({ ...role, function: cl.function, level: cl.level });
    });

    if (toInsert.length) {
      const { error } = await supabase.from('jobs').insert(toInsert);
      if (error) {
        console.error(`  ✗ Insert error: ${error.message}`);
      } else {
        toInsert.forEach(j => console.log(`    + ${j.title}`));
        added += toInsert.length;
      }
    }
    await sleep(300);
  }
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncGreenhouse() {
  console.log('\n── Syncing Greenhouse ATS ──');
  let totalFetched = 0, totalAdded = 0;
  const seenNames = new Set();

  for (const firm of GREENHOUSE_FIRMS) {
    if (seenNames.has(firm.name)) continue;
    try {
      const res = await fetchWithTimeout(
        `https://boards-api.greenhouse.io/v1/boards/${firm.slug}/jobs`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } }, 6000
      );
      if (!res.ok) continue;
      const data  = await res.json();
      const roles = data.jobs || [];
      if (!roles.length) continue;

      seenNames.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${roles.length} postings`);
      totalFetched += roles.length;

      const mapped = roles.map(r => ({
        source_id:       `greenhouse-${r.id}`,
        title:           r.title,
        firm:            firm.name,
        location:        r.location?.name || null,
        description:     (r.content || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 1500),
        apply_url:       r.absolute_url,
        source:          'Greenhouse',
        is_front_office: true,
        is_approved:     true,
        posted_at:       r.updated_at || new Date().toISOString(),
      }));

      const added = await insertBatch(mapped, 'Greenhouse', firm.name);
      totalAdded += added;
      await sleep(200);
    } catch (e) {}
  }

  console.log(`  Greenhouse: ${totalFetched} fetched, ${totalAdded} added\n`);
  return totalAdded;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// LEVER SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncLever() {
  console.log('── Syncing Lever ATS ──');
  let totalFetched = 0, totalAdded = 0;
  const seenNames = new Set();

  for (const firm of LEVER_FIRMS) {
    if (seenNames.has(firm.name)) continue;
    try {
      const res = await fetchWithTimeout(
        `https://api.lever.co/v0/postings/${firm.slug}?mode=json&limit=100`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } }, 6000
      );
      if (!res.ok) continue;
      const roles = await res.json();
      if (!Array.isArray(roles) || !roles.length) continue;

      seenNames.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${roles.length} postings`);
      totalFetched += roles.length;

      const mapped = roles.map(r => ({
        source_id:       `lever-${r.id}`,
        title:           r.text,
        firm:            firm.name,
        location:        r.categories?.location || r.workplaceType || null,
        description:     (r.descriptionPlain || r.description || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 1500),
        apply_url:       r.hostedUrl || r.applyUrl,
        source:          'Lever',
        is_front_office: true,
        is_approved:     true,
        posted_at:       r.createdAt ? new Date(r.createdAt).toISOString() : new Date().toISOString(),
      }));

      const added = await insertBatch(mapped, 'Lever', firm.name);
      totalAdded += added;
      await sleep(200);
    } catch (e) {}
  }

  console.log(`  Lever: ${totalFetched} fetched, ${totalAdded} added\n`);
  return totalAdded;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// eFINANCIALCAREERS RSS SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncEFC() {
  console.log('── Syncing eFinancialCareers RSS ──');
  let totalAdded = 0;

  for (const query of EFC_QUERIES) {
    try {
      const url = `https://www.efinancialcareers.com/search?q=${query}&employment_type=permanent&format=rss`;
      const res = await fetchWithTimeout(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
      }, 8000);
      if (!res.ok) continue;

      const xml   = await res.text();
      const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
      if (!items.length) continue;

      console.log(`  Query "${query}": ${items.length} results`);

      const mapped = [];
      for (const item of items) {
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/)      || [])[1]?.trim() || '';
        const link    = (item.match(/<link>(.*?)<\/link>/)                         || [])[1]?.trim() || '';
        const desc    = (item.match(/<description><!\[CDATA\[(.*?)\]\]>/)          || [])[1] || '';
        const firm    = (item.match(/<[^>]*:company[^>]*><!\[CDATA\[(.*?)\]\]>/)   || 
                         item.match(/<source[^>]*>(.*?)<\/source>/)                || [])[1]?.trim() || 'Unknown';
        const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/)                   || [])[1] || '';
        const locRaw  = (item.match(/<[^>]*:city[^>]*>(.*?)<\/[^>]*:city>/)        || [])[1] || '';

        if (!title || !link) continue;
        const sourceId = `efc-${Buffer.from(link).toString('base64').slice(0, 40)}`;

        mapped.push({
          source_id:       sourceId,
          title:           title,
          firm:            firm,
          location:        locRaw || null,
          description:     desc.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 1500),
          apply_url:       link,
          source:          'eFinancialCareers',
          is_front_office: true,
          is_approved:     true,
          posted_at:       pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
        });
      }

      if (mapped.length) {
        const added = await insertBatch(mapped, 'eFinancialCareers', '');
        totalAdded += added;
      }
      await sleep(600);
    } catch (e) {}
  }

  console.log(`  eFinancialCareers: ${totalAdded} new roles added\n`);
  return totalAdded;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function cleanupExpired() {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const { count } = await supabase
    .from('jobs').delete({ count: 'exact' })
    .lt('posted_at', cutoff)
    .eq('is_featured', false);
  console.log(`── Cleanup: removed ${count || 0} expired listings\n`);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function main() {
  console.log(`\n[${new Date().toISOString()}] Starting Front Office Jobs sync...\n`);
  const ghAdded  = await syncGreenhouse();
  const lvAdded  = await syncLever();
  const efcAdded = await syncEFC();
  await cleanupExpired();
  const total = ghAdded + lvAdded + efcAdded;
  console.log(`\n✓ Sync complete. Added ${total} new front office roles.`);
  console.log(`  Greenhouse: ${ghAdded} | Lever: ${lvAdded} | eFinancialCareers: ${efcAdded}`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
