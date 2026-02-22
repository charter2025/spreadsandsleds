// sync-jobs.js
// Runs daily via GitHub Actions cron
// Sources: Greenhouse ATS (confirmed working slugs)
// Classifies with Claude Haiku in BATCHES of 20 → stores in Supabase
// ~20x faster than one-at-a-time classification

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

// Fetch with 8s timeout — prevents hangs
async function fetchWithTimeout(url, options = {}, ms = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    clearTimeout(t);
    return res;
  } catch (e) {
    clearTimeout(t);
    throw e;
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE FIRMS — only confirmed or likely working slugs
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  // ── CONFIRMED WORKING ──
  { slug: 'point72',              name: 'Point72' },
  { slug: 'janestreet',           name: 'Jane Street' },
  { slug: 'mangroup',             name: 'Man Group' },

  // ── BOUTIQUE BANKS ──
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
  { slug: 'tdbank',               name: 'TD Securities' },
  { slug: 'tdsecurities',         name: 'TD Securities' },
  { slug: 'scotiabank',           name: 'Scotiabank' },
  { slug: 'natixis',              name: 'Natixis' },
  { slug: 'mizuho',               name: 'Mizuho' },
  { slug: 'mizuhofs',             name: 'Mizuho' },
  { slug: 'smbc',                 name: 'SMBC' },
  { slug: 'nomura',               name: 'Nomura' },
  { slug: 'macquarie',            name: 'Macquarie' },
  { slug: 'macquariegroup',       name: 'Macquarie' },
  { slug: 'cantor',               name: 'Cantor Fitzgerald' },
  { slug: 'cantorfitzgerald',     name: 'Cantor Fitzgerald' },
  { slug: 'coatue',               name: 'Coatue Management' },
  { slug: 'drw',                  name: 'DRW' },
  { slug: 'drwtrading',           name: 'DRW' },
  { slug: 'optiver',              name: 'Optiver' },
  { slug: 'susquehanna',          name: 'Susquehanna (SIG)' },
  { slug: 'sig',                  name: 'Susquehanna (SIG)' },

  // ── HEDGE FUNDS / QUANT ──
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
  { slug: 'akuna',                name: 'Akuna Capital' },
  { slug: 'imc',                  name: 'IMC Trading' },
  { slug: 'imctrading',           name: 'IMC Trading' },
  { slug: 'squarepointcapital',   name: 'Squarepoint Capital' },
  { slug: 'squarepoint',          name: 'Squarepoint Capital' },
  { slug: 'millennium',           name: 'Millennium Management' },
  { slug: 'millenniummanagement', name: 'Millennium Management' },
  { slug: 'aqr',                  name: 'AQR Capital Management' },
  { slug: 'aqrcapital',           name: 'AQR Capital Management' },
  { slug: 'bridgewater',          name: 'Bridgewater Associates' },
  { slug: 'wintongroup',          name: 'Winton Group' },
  { slug: 'winton',               name: 'Winton Group' },
  { slug: 'renaissance',          name: 'Renaissance Technologies' },
  { slug: 'rentec',               name: 'Renaissance Technologies' },
  { slug: 'tigereyecm',           name: 'Tiger Global' },
  { slug: 'tigerglobal',          name: 'Tiger Global' },
  { slug: 'perceptive',           name: 'Perceptive Advisors' },
  { slug: 'balyasny',             name: 'Balyasny Asset Management' },
  { slug: 'balyasnyam',           name: 'Balyasny Asset Management' },
  { slug: 'marshalwace',          name: 'Marshall Wace' },
  { slug: 'marshallwace',         name: 'Marshall Wace' },
  { slug: 'citadelam',            name: 'Citadel' },
  { slug: 'citadelllc',           name: 'Citadel' },

  // ── ASSET MANAGERS ──
  { slug: 'fidelity',             name: 'Fidelity Investments' },
  { slug: 'fidelityinvestments',  name: 'Fidelity Investments' },
  { slug: 'troweprice',           name: 'T. Rowe Price' },
  { slug: 'trowe',                name: 'T. Rowe Price' },
  { slug: 'invesco',              name: 'Invesco' },
  { slug: 'franklintempleton',    name: 'Franklin Templeton' },
  { slug: 'pgim',                 name: 'PGIM' },
  { slug: 'westernasset',         name: 'Western Asset Management' },
  { slug: 'alliancebernstein',    name: 'AllianceBernstein' },
  { slug: 'ab',                   name: 'AllianceBernstein' },
  { slug: 'neuberger',            name: 'Neuberger Berman' },
  { slug: 'neubergerberman',      name: 'Neuberger Berman' },
  { slug: 'columbia',             name: 'Columbia Threadneedle' },
  { slug: 'columbiathreadneedle', name: 'Columbia Threadneedle' },
  { slug: 'putnam',               name: 'Putnam Investments' },
  { slug: 'calvert',              name: 'Calvert Research' },
  { slug: 'loomis',               name: 'Loomis Sayles' },
  { slug: 'loomissayles',         name: 'Loomis Sayles' },
  { slug: 'dodge',                name: 'Dodge & Cox' },
  { slug: 'dodgeandcox',          name: 'Dodge & Cox' },
  { slug: 'brandes',              name: 'Brandes Investment' },
  { slug: 'dimensional',          name: 'Dimensional Fund Advisors' },
  { slug: 'dfa',                  name: 'Dimensional Fund Advisors' },
  { slug: 'gmo',                  name: 'GMO' },
  { slug: 'wellington',           name: 'Wellington Management' },
  { slug: 'wellingtonmanagement', name: 'Wellington Management' },
  { slug: 'mfs',                  name: 'MFS Investment Management' },
  { slug: 'vanguard',             name: 'Vanguard' },

  // ── PE / ALTERNATIVES ──
  { slug: 'kkr',                  name: 'KKR' },
  { slug: 'kkrecruitment',        name: 'KKR' },
  { slug: 'apolloglobal',         name: 'Apollo Global Management' },
  { slug: 'apollo',               name: 'Apollo Global Management' },
  { slug: 'carlyle',              name: 'The Carlyle Group' },
  { slug: 'thecarlylegroup',      name: 'The Carlyle Group' },
  { slug: 'tpg',                  name: 'TPG Capital' },
  { slug: 'tpgcapital',           name: 'TPG Capital' },
  { slug: 'warburgpincus',        name: 'Warburg Pincus' },
  { slug: 'silverlake',           name: 'Silver Lake' },
  { slug: 'silverlakepartners',   name: 'Silver Lake' },
  { slug: 'generalatlantic',      name: 'General Atlantic' },
  { slug: 'golubcapital',         name: 'Golub Capital' },
  { slug: 'aresmanagement',       name: 'Ares Management' },
  { slug: 'ares',                 name: 'Ares Management' },
  { slug: 'blueowl',              name: 'Blue Owl Capital' },
  { slug: 'bluowl',               name: 'Blue Owl Capital' },
  { slug: 'brookfield',           name: 'Brookfield Asset Management' },
  { slug: 'brookfieldas',         name: 'Brookfield Asset Management' },
  { slug: 'hps',                  name: 'HPS Investment Partners' },
  { slug: 'hpsinvestment',        name: 'HPS Investment Partners' },
  { slug: 'oaktree',              name: 'Oaktree Capital' },
  { slug: 'oaktreecapital',       name: 'Oaktree Capital' },
  { slug: 'leonardgreen',         name: 'Leonard Green & Partners' },
  { slug: 'thl',                  name: 'Thomas H. Lee Partners' },
  { slug: 'friedman',             name: 'Friedman Fleischer & Lowe' },
  { slug: 'hg',                   name: 'HgCapital' },
  { slug: 'hgcapital',            name: 'HgCapital' },
  { slug: 'cerberus',             name: 'Cerberus Capital' },
  { slug: 'cerberuscapital',      name: 'Cerberus Capital' },
  { slug: 'castlelake',           name: 'Castle Lake' },
  { slug: 'stonepoint',           name: 'Stone Point Capital' },
  { slug: 'blackrock',            name: 'BlackRock' },
  { slug: 'blackrockjobs',        name: 'BlackRock' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BATCH CLASSIFICATION — 20 roles per Claude call
// Titles only — fast, cheap, accurate enough
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyBatch(roles) {
  // roles = [{ id, title, firm }]
  const list = roles.map((r, i) => `${i}: ${r.title} @ ${r.firm}`).join('\n');

  const prompt = `Classify these finance job titles for a front office jobs board. 
Front office = revenue-generating investment roles: trading, sales & trading, investment banking, 
asset/fund management, private equity, equity/credit research, quant research (alpha/investment focused),
wealth/private banking, capital markets origination, macro strategy, structured finance.

NOT front office = tech/engineering, operations, compliance, legal, HR, accounting, back office, 
data engineering (unless investment-focused), enterprise risk.

When in doubt at a bank or hedge fund — lean INCLUDE.

For each job below respond with exactly one JSON object per line (no other text):
{"i":0,"fo":true,"fn":"S&T","lv":"Analyst"}

Functions: S&T=Sales&Trading, IBD=InvestmentBanking, AM=AssetMgmt, PE=PrivateEquity, RM=Research, PB=PrivateBanking
Levels: Analyst, Associate, VP, Director, MD, Partner
Use null if unknown.

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
      } catch (e) { /* skip malformed lines */ }
    }

    // Fill in any missing indices as not front office
    roles.forEach((_, i) => {
      if (!results[i]) results[i] = { is_front_office: false, function: null, level: null };
    });

    return results;
  } catch (e) {
    console.warn('  Batch classification failed:', e.message);
    // Default all to false on error
    const fallback = {};
    roles.forEach((_, i) => { fallback[i] = { is_front_office: false, function: null, level: null }; });
    return fallback;
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncGreenhouse() {
  console.log('\n── Syncing Greenhouse ATS ──');
  let totalFetched = 0, totalAdded = 0, totalSkipped = 0;
  const seenNames = new Set(); // dedupe firms with multiple slug attempts

  for (const firm of GREENHOUSE_FIRMS) {
    // Skip if we already found a working slug for this firm
    if (seenNames.has(firm.name)) continue;

    try {
      const res = await fetchWithTimeout(
        `https://boards-api.greenhouse.io/v1/boards/${firm.slug}/jobs`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } },
        6000
      );
      if (!res.ok) continue; // silent skip for 404s

      const data  = await res.json();
      const roles = data.jobs || [];
      if (!roles.length) continue;

      seenNames.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${roles.length} postings`);
      totalFetched += roles.length;

      // ── Check which role IDs are already in DB ──
      const sourceIds = roles.map(r => `greenhouse-${r.id}`);
      const { data: existing } = await supabase
        .from('jobs')
        .select('source_id')
        .in('source_id', sourceIds);
      const existingSet = new Set((existing || []).map(e => e.source_id));

      // Filter to only new roles
      const newRoles = roles.filter(r => !existingSet.has(`greenhouse-${r.id}`));
      totalSkipped += roles.length - newRoles.length;

      if (!newRoles.length) continue;

      // ── Classify in batches of 20 ──
      const BATCH = 20;
      for (let i = 0; i < newRoles.length; i += BATCH) {
        const batch = newRoles.slice(i, i + BATCH);
        const batchInput = batch.map(r => ({ id: r.id, title: r.title, firm: firm.name }));
        const classifications = await classifyBatch(batchInput);

        // ── Insert approved roles ──
        const toInsert = [];
        batch.forEach((role, idx) => {
          const cl = classifications[idx];
          if (!cl?.is_front_office) { totalSkipped++; return; }

          toInsert.push({
            title:           role.title,
            firm:            firm.name,
            location:        role.location?.name || null,
            function:        cl.function,
            level:           cl.level,
            description:     (role.content || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 1500),
            apply_url:       role.absolute_url,
            source:          'Greenhouse',
            source_id:       `greenhouse-${role.id}`,
            is_front_office: true,
            is_approved:     true,
            posted_at:       role.updated_at || new Date().toISOString(),
          });
        });

        if (toInsert.length) {
          const { error } = await supabase.from('jobs').insert(toInsert);
          if (error) {
            console.error(`  ✗ Insert error: ${error.message}`);
          } else {
            toInsert.forEach(j => console.log(`    + ${j.title}`));
            totalAdded += toInsert.length;
          }
        }

        await sleep(300); // Pause between Claude batch calls
      }

      await sleep(200); // Pause between firms
    } catch (e) {
      // Timeout or network error — skip silently
    }
  }

  console.log(`\n  Greenhouse: ${totalFetched} fetched, ${totalAdded} added, ${totalSkipped} skipped\n`);
  return totalAdded;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP — remove listings older than 60 days
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
  const added = await syncGreenhouse();
  await cleanupExpired();
  console.log(`\n✓ Sync complete. Added ${added} new front office roles.`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
