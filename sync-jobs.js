// sync-jobs.js — Front Office Jobs Board
// TARGET: 2,500-5,000 genuine front office listings
// Sources:
//   1. Greenhouse ATS  (200+ firms)
//   2. Lever ATS       (80+ firms)  
//   3. Workday ATS     (Goldman, JPM, MS, Citi, BofA, Barclays, UBS, DB, CS, HSBC, Wells...)
//   4. eFinancialCareers RSS (paginated, finance-specific)
//   5. Adzuna Jobs API (free, aggregator, 1000s of finance roles)
//
// Classifier: STRICT front office only — no tech/eng/ops/compliance/legal

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

async function fetchWithTimeout(url, options = {}, ms = 10000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    clearTimeout(t); return res;
  } catch (e) { clearTimeout(t); throw e; }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLASSIFIER — strict, title+firm only, 20 per call
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyBatch(roles) {
  const list = roles.map((r, i) => `${i}: "${r.title}" at ${r.firm}`).join('\n');

  const prompt = `You are classifying finance job postings. Decide if each is a genuine FRONT OFFICE role.

FRONT OFFICE = revenue-generating investment roles:
✓ Trading (equities, FX, rates, credit, commodities, derivatives, prop)
✓ Investment Banking (M&A, ECM, DCM, LevFin, restructuring, sponsor coverage)
✓ Sales & Trading / Capital Markets Sales
✓ Equity Research / Credit Research / Macro Strategy
✓ Portfolio Management / Fund Management / Asset Management
✓ Private Equity / Venture Capital / Growth Equity (investment roles)
✓ Hedge Fund (investment/analytical roles)
✓ Quantitative Research (investment alpha, NOT data engineering)
✓ Wealth Management / Private Banking (client-facing advisory)
✓ Structured Finance / Securitization (deal execution)
✓ Prime Brokerage (client coverage)
✓ Risk: Market Risk, Credit Risk, Trading Risk only

NOT FRONT OFFICE — EXCLUDE these:
✗ Software Engineer, Developer, SRE, DevOps, Data Engineer, ML Engineer (ANY tech role)
✗ Technology Investment Banking is OK but "Tech IB Engineer" is NOT
✗ Operations, Middle Office, Back Office, Settlements, Reconciliation
✗ Compliance, Legal, Regulatory, AML, KYC
✗ HR, Recruiting, Talent Acquisition
✗ Finance/Accounting (FP&A, Controller, CFO roles at non-fund firms)
✗ Enterprise Risk, Operational Risk, Non-Financial Risk
✗ Product Manager (unless explicitly investment product)
✗ Marketing, Communications, PR
✗ IT, Infrastructure, Cybersecurity
✗ "Execution Technology", "Trading Technology" engineer roles

EDGE CASES:
- "Quant Researcher" at hedge fund/bank → INCLUDE
- "Quant Developer" or "Quant Engineer" → EXCLUDE  
- "Technology M&A" banker → INCLUDE (coverage role)
- "Technology" in firm division name is fine, but engineer title = EXCLUDE
- "Analyst" at bank/fund in ambiguous dept → INCLUDE (lean inclusive for analysts)
- "Associate" at PE/HF → INCLUDE

Return exactly one JSON object per line. No markdown, no explanation:
{"i":0,"fo":true,"fn":"S&T","lv":"VP"}

fn values: S&T | IBD | AM | PE | RM | PB | QR (Quant Research) | null
lv values: Analyst | Associate | VP | Director | MD | Partner | null

Roles to classify:
${list}`;

  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 900,
      messages: [{ role: 'user', content: prompt }]
    });
    const results = {};
    for (const line of res.content[0].text.trim().split('\n')) {
      try {
        const obj = JSON.parse(line.trim().replace(/```json|```/g, ''));
        if (typeof obj.i === 'number') {
          results[obj.i] = { is_front_office: !!obj.fo, function: obj.fn || null, level: obj.lv || null };
        }
      } catch {}
    }
    roles.forEach((_, i) => {
      if (!results[i]) results[i] = { is_front_office: false, function: null, level: null };
    });
    return results;
  } catch (e) {
    console.warn('  Classification error:', e.message);
    const fb = {};
    roles.forEach((_, i) => { fb[i] = { is_front_office: false, function: null, level: null }; });
    return fb;
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INSERT HELPER — dedupes, classifies, inserts
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function insertBatch(roles) {
  if (!roles.length) return 0;
  // Dedupe source_ids
  const sourceIds = [...new Set(roles.map(r => r.source_id))];
  const { data: existing } = await supabase
    .from('jobs').select('source_id').in('source_id', sourceIds);
  const existingSet = new Set((existing || []).map(e => e.source_id));
  const newRoles = roles.filter(r => !existingSet.has(r.source_id));
  if (!newRoles.length) return 0;

  let added = 0;
  const BATCH = 20;
  for (let i = 0; i < newRoles.length; i += BATCH) {
    const batch = newRoles.slice(i, i + BATCH);
    const classifications = await classifyBatch(
      batch.map(r => ({ title: r.title, firm: r.firm }))
    );
    const toInsert = [];
    batch.forEach((r, idx) => {
      const cl = classifications[idx];
      if (cl?.is_front_office) {
        toInsert.push({ ...r, function: cl.function || null, level: cl.level || null });
      }
    });

    if (toInsert.length) {
      const { error } = await supabase.from('jobs').insert(toInsert);
      if (!error) {
        added += toInsert.length;
        toInsert.forEach(j => console.log(`    + ${j.title} @ ${j.firm}`));
      } else {
        console.error(`  ✗ DB error: ${error.message}`);
      }
    }
    await sleep(200);
  }
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 1: GREENHOUSE ATS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  // ── Confirmed working ──
  { slug: 'point72',              name: 'Point72' },
  { slug: 'janestreet',           name: 'Jane Street' },
  { slug: 'mangroup',             name: 'Man Group' },
  { slug: 'generalatlantic',      name: 'General Atlantic' },
  { slug: 'williamblair',         name: 'William Blair' },
  { slug: 'optiver',              name: 'Optiver' },
  { slug: 'drw',                  name: 'DRW' },
  { slug: 'imc',                  name: 'IMC Trading' },
  { slug: 'akunacapital',         name: 'Akuna Capital' },

  // ── Banks ──
  { slug: 'blackrock',            name: 'BlackRock' },
  { slug: 'blackrockjobs',        name: 'BlackRock' },
  { slug: 'nomura',               name: 'Nomura' },
  { slug: 'macquarie',            name: 'Macquarie' },
  { slug: 'macquariegroup',       name: 'Macquarie' },
  { slug: 'mizuho',               name: 'Mizuho' },
  { slug: 'mizuhofs',             name: 'Mizuho' },
  { slug: 'smbc',                 name: 'SMBC' },
  { slug: 'rbc',                  name: 'RBC Capital Markets' },
  { slug: 'rbccm',                name: 'RBC Capital Markets' },
  { slug: 'bmo',                  name: 'BMO Capital Markets' },
  { slug: 'bmocm',                name: 'BMO Capital Markets' },
  { slug: 'tdsecurities',         name: 'TD Securities' },
  { slug: 'scotiabank',           name: 'Scotiabank' },
  { slug: 'natixis',              name: 'Natixis' },
  { slug: 'ingbank',              name: 'ING' },
  { slug: 'bnpparibas',           name: 'BNP Paribas' },
  { slug: 'societegenerale',      name: 'Societe Generale' },
  { slug: 'creditagricole',       name: 'Credit Agricole CIB' },
  { slug: 'mufg',                 name: 'MUFG' },
  { slug: 'mufgamericas',         name: 'MUFG' },
  { slug: 'daiwa',                name: 'Daiwa Capital' },
  { slug: 'daiwacm',              name: 'Daiwa Capital Markets' },
  { slug: 'smbcnikko',            name: 'SMBC Nikko' },
  { slug: 'rabobank',             name: 'Rabobank' },
  { slug: 'abnamro',              name: 'ABN AMRO' },
  { slug: 'commerzbank',          name: 'Commerzbank' },

  // ── Boutique banks ──
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
  { slug: 'rwbaird',              name: 'Baird' },
  { slug: 'pipersandler',         name: 'Piper Sandler' },
  { slug: 'stifel',               name: 'Stifel' },
  { slug: 'tdcowen',              name: 'TD Cowen' },
  { slug: 'nuveen',               name: 'Nuveen' },
  { slug: 'oppenheimer',          name: 'Oppenheimer' },
  { slug: 'cantorfitzgerald',     name: 'Cantor Fitzgerald' },
  { slug: 'needham',              name: 'Needham & Company' },
  { slug: 'wedbush',              name: 'Wedbush Securities' },
  { slug: 'leerink',              name: 'Leerink Partners' },
  { slug: 'jmp',                  name: 'JMP Securities' },
  { slug: 'ladenburg',            name: 'Ladenburg Thalmann' },
  { slug: 'rothcapital',          name: 'Roth Capital Partners' },
  { slug: 'imperialcapital',      name: 'Imperial Capital' },
  { slug: 'hcwainwright',         name: 'H.C. Wainwright' },
  { slug: 'svbsecurities',        name: 'SVB Securities' },
  { slug: 'charlesschwab',        name: 'Charles Schwab' },
  { slug: 'schwab',               name: 'Schwab' },
  { slug: 'rbcwealthmanagement',  name: 'RBC Wealth Management' },
  { slug: 'raymond',              name: 'Raymond James' },
  { slug: 'raymondjames',         name: 'Raymond James' },
  { slug: 'edwardjones',          name: 'Edward Jones' },
  { slug: 'morganstanleywm',      name: 'Morgan Stanley WM' },
  { slug: 'ameriprise',           name: 'Ameriprise Financial' },
  { slug: 'lpl',                  name: 'LPL Financial' },
  { slug: 'lplfinancial',         name: 'LPL Financial' },
  { slug: 'stifelwm',             name: 'Stifel WM' },
  { slug: 'janney',               name: 'Janney Montgomery Scott' },
  { slug: 'janneymontgomery',     name: 'Janney Montgomery Scott' },
  { slug: 'hilliard',             name: 'Hilliard Lyons' },
  { slug: 'daintree',             name: 'DA Davidson' },
  { slug: 'dadavidson',           name: 'DA Davidson' },
  { slug: 'stephens',             name: 'Stephens Inc' },
  { slug: 'stephensinc',          name: 'Stephens Inc' },
  { slug: 'truist',               name: 'Truist Securities' },
  { slug: 'truitsecurities',      name: 'Truist Securities' },
  { slug: 'keybanc',              name: 'KeyBanc Capital Markets' },
  { slug: 'keybancapital',        name: 'KeyBanc Capital Markets' },
  { slug: 'pnc',                  name: 'PNC Capital Markets' },
  { slug: 'pncbank',              name: 'PNC' },
  { slug: 'fifththird',           name: 'Fifth Third Securities' },
  { slug: 'regionsfinancial',     name: 'Regions Securities' },
  { slug: 'huntington',           name: 'Huntington Capital Markets' },
  { slug: 'comerica',             name: 'Comerica' },
  { slug: 'usbank',               name: 'US Bancorp Investments' },
  { slug: 'usbancorp',            name: 'US Bancorp' },
  { slug: 'firsthorizon',         name: 'First Horizon' },
  { slug: 'synovus',              name: 'Synovus' },
  { slug: 'pinnaclebank',         name: 'Pinnacle Financial' },

  // ── Hedge Funds ──
  { slug: 'twosigma',             name: 'Two Sigma' },
  { slug: 'twosigmainvestments',  name: 'Two Sigma' },
  { slug: 'deshawgroup',          name: 'D.E. Shaw' },
  { slug: 'hudsonrivertrading',   name: 'Hudson River Trading' },
  { slug: 'pimco',                name: 'PIMCO' },
  { slug: 'virtu',                name: 'Virtu Financial' },
  { slug: 'virtufinancial',       name: 'Virtu Financial' },
  { slug: 'squarepointcapital',   name: 'Squarepoint Capital' },
  { slug: 'millennium',           name: 'Millennium Management' },
  { slug: 'aqr',                  name: 'AQR Capital Management' },
  { slug: 'aqrcapital',           name: 'AQR Capital Management' },
  { slug: 'bridgewater',          name: 'Bridgewater Associates' },
  { slug: 'winton',               name: 'Winton Group' },
  { slug: 'balyasny',             name: 'Balyasny Asset Management' },
  { slug: 'marshallwace',         name: 'Marshall Wace' },
  { slug: 'citadelam',            name: 'Citadel' },
  { slug: 'grahamcapital',        name: 'Graham Capital Management' },
  { slug: 'tudor',                name: 'Tudor Investment Corp' },
  { slug: 'coatue',               name: 'Coatue Management' },
  { slug: 'tigereyecm',           name: 'Tiger Global' },
  { slug: 'perceptive',           name: 'Perceptive Advisors' },
  { slug: 'viking',               name: 'Viking Global' },
  { slug: 'maverick',             name: 'Maverick Capital' },
  { slug: 'glenview',             name: 'Glenview Capital' },
  { slug: 'lone',                 name: 'Lone Pine Capital' },
  { slug: 'brevan',               name: 'Brevan Howard' },
  { slug: 'brevanhoward',         name: 'Brevan Howard' },
  { slug: 'canyon',               name: 'Canyon Capital' },
  { slug: 'oaktree',              name: 'Oaktree Capital' },
  { slug: 'oaktreecapital',       name: 'Oaktree Capital' },
  { slug: 'arrowstreet',          name: 'Arrowstreet Capital' },
  { slug: 'gmo',                  name: 'GMO' },
  { slug: 'jumptrading',          name: 'Jump Trading' },
  { slug: 'wolverinetrading',     name: 'Wolverine Trading' },
  { slug: 'flowtraders',          name: 'Flow Traders' },
  { slug: 'susquehanna',          name: 'Susquehanna (SIG)' },
  { slug: 'sig',                  name: 'Susquehanna (SIG)' },
  { slug: 'voleon',               name: 'Voleon' },
  { slug: 'exoduspoint',          name: 'ExodusPoint Capital' },
  { slug: 'magnetar',             name: 'Magnetar Capital' },
  { slug: 'kingstreet',           name: 'King Street Capital' },
  { slug: 'ellington',            name: 'Ellington Management' },
  { slug: 'saba',                 name: 'Saba Capital Management' },
  { slug: 'anchorage',            name: 'Anchorage Capital' },
  { slug: 'bluemountain',         name: 'BlueMountain Capital' },
  { slug: 'pinebridge',           name: 'PineBridge Investments' },
  { slug: 'capstone',             name: 'Capstone Investment' },
  { slug: 'sylebra',              name: 'Sylebra Capital' },
  { slug: 'greenoaks',            name: 'Green Oaks Capital' },
  { slug: 'coatue',               name: 'Coatue Management' },
  { slug: 'dune',                 name: 'Dune Capital' },
  { slug: 'sculptor',             name: 'Sculptor Capital' },
  { slug: 'gsam',                 name: 'Goldman Sachs AM' },

  // ── Asset Managers ──
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
  { slug: 'wellington',           name: 'Wellington Management' },
  { slug: 'wellingtonmanagement', name: 'Wellington Management' },
  { slug: 'mfs',                  name: 'MFS Investment Management' },
  { slug: 'columbiathreadneedle', name: 'Columbia Threadneedle' },
  { slug: 'artisanpartners',      name: 'Artisan Partners' },
  { slug: 'cohensteers',          name: 'Cohen & Steers' },
  { slug: 'lazardasset',          name: 'Lazard Asset Management' },
  { slug: 'federatedhermes',      name: 'Federated Hermes' },
  { slug: 'americancentury',      name: 'American Century' },
  { slug: 'principal',            name: 'Principal Asset Management' },
  { slug: 'eatonvance',           name: 'Eaton Vance' },
  { slug: 'matthewsasia',         name: 'Matthews Asia' },
  { slug: 'dodge',                name: 'Dodge & Cox' },
  { slug: 'thornburg',            name: 'Thornburg Investment' },
  { slug: 'calamos',              name: 'Calamos Investments' },
  { slug: 'gabelli',              name: 'Gabelli Funds' },
  { slug: 'manning',              name: 'Manning & Napier' },
  { slug: 'putnam',               name: 'Putnam Investments' },
  { slug: 'columbia',             name: 'Columbia Management' },
  { slug: 'pax',                  name: 'Pax World Funds' },
  { slug: 'natixisim',            name: 'Natixis Investment Managers' },
  { slug: 'voya',                 name: 'Voya Investment Management' },
  { slug: 'voyafinancial',        name: 'Voya Financial' },
  { slug: 'sunlife',              name: 'Sun Life Investment' },
  { slug: 'manulife',             name: 'Manulife Investment' },
  { slug: 'ninetyonefunds',       name: 'Ninety One' },
  { slug: 'aberdeenstandard',     name: 'abrdn' },
  { slug: 'abrdn',                name: 'abrdn' },
  { slug: 'schroders',            name: 'Schroders' },
  { slug: 'janus',                name: 'Janus Henderson' },
  { slug: 'janushenderson',       name: 'Janus Henderson' },
  { slug: 'baillie',              name: 'Baillie Gifford' },
  { slug: 'baillegifford',        name: 'Baillie Gifford' },
  { slug: 'aberdeen',             name: 'Aberdeen Investments' },
  { slug: 'lgim',                 name: 'LGIM' },
  { slug: 'legal',                name: 'Legal & General Investment' },
  { slug: 'aviva',                name: 'Aviva Investors' },
  { slug: 'hermes',               name: 'Federated Hermes' },

  // ── PE / Credit / Infrastructure ──
  { slug: 'kkr',                  name: 'KKR' },
  { slug: 'kkrecruitment',        name: 'KKR' },
  { slug: 'apolloglobal',         name: 'Apollo Global Management' },
  { slug: 'apollo',               name: 'Apollo Global Management' },
  { slug: 'carlyle',              name: 'The Carlyle Group' },
  { slug: 'thecarlylegroup',      name: 'The Carlyle Group' },
  { slug: 'tpg',                  name: 'TPG Capital' },
  { slug: 'warburgpincus',        name: 'Warburg Pincus' },
  { slug: 'silverlake',           name: 'Silver Lake' },
  { slug: 'golubcapital',         name: 'Golub Capital' },
  { slug: 'aresmanagement',       name: 'Ares Management' },
  { slug: 'ares',                 name: 'Ares Management' },
  { slug: 'blueowl',              name: 'Blue Owl Capital' },
  { slug: 'brookfield',           name: 'Brookfield Asset Management' },
  { slug: 'hps',                  name: 'HPS Investment Partners' },
  { slug: 'cerberuscapital',      name: 'Cerberus Capital' },
  { slug: 'leonardgreen',         name: 'Leonard Green & Partners' },
  { slug: 'baincapital',          name: 'Bain Capital' },
  { slug: 'thomabravo',           name: 'Thoma Bravo' },
  { slug: 'vistaequity',          name: 'Vista Equity Partners' },
  { slug: 'insight',              name: 'Insight Partners' },
  { slug: 'hamiltonlane',         name: 'Hamilton Lane' },
  { slug: 'stepstonegroup',       name: 'StepStone Group' },
  { slug: 'pantheon',             name: 'Pantheon Ventures' },
  { slug: 'harbourvest',          name: 'HarbourVest Partners' },
  { slug: 'audaxprivateequity',   name: 'Audax Private Equity' },
  { slug: 'berkshirepartners',    name: 'Berkshire Partners' },
  { slug: 'gtcr',                 name: 'GTCR' },
  { slug: 'advent',               name: 'Advent International' },
  { slug: 'ta',                   name: 'TA Associates' },
  { slug: 'taassociates',         name: 'TA Associates' },
  { slug: 'charlesbank',          name: 'Charlesbank Capital' },
  { slug: 'francisco',            name: 'Francisco Partners' },
  { slug: 'stonepoint',           name: 'Stone Point Capital' },
  { slug: 'kayneanderson',        name: 'Kayne Anderson' },
  { slug: 'castlelake',           name: 'Castle Lake' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 2: LEVER ATS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const LEVER_FIRMS = [
  { slug: 'citadel',             name: 'Citadel' },
  { slug: 'citadelsecurities',   name: 'Citadel Securities' },
  { slug: 'blackstone',          name: 'Blackstone' },
  { slug: 'goldmansachs',        name: 'Goldman Sachs' },
  { slug: 'morganstanley',       name: 'Morgan Stanley' },
  { slug: 'jpmorgan',            name: 'JPMorgan' },
  { slug: 'coatue',              name: 'Coatue Management' },
  { slug: 'iconiqcapital',       name: 'ICONIQ Capital' },
  { slug: 'thomabravo',          name: 'Thoma Bravo' },
  { slug: 'baincapital',         name: 'Bain Capital' },
  { slug: 'summit',              name: 'Summit Partners' },
  { slug: 'tiger',               name: 'Tiger Global' },
  { slug: 'tudor',               name: 'Tudor Investment Corp' },
  { slug: 'elliotmgmt',          name: 'Elliott Management' },
  { slug: 'pershingsquare',      name: 'Pershing Square' },
  { slug: 'valuact',             name: 'ValueAct Capital' },
  { slug: 'starboard',           name: 'Starboard Value' },
  { slug: 'grahamcapital',       name: 'Graham Capital Management' },
  { slug: 'renaissance',         name: 'Renaissance Technologies' },
  { slug: 'deshawresearch',      name: 'D.E. Shaw Research' },
  { slug: 'hudsonbay',           name: 'Hudson Bay Capital' },
  { slug: 'exoduspoint',         name: 'ExodusPoint Capital' },
  { slug: 'magnetar',            name: 'Magnetar Capital' },
  { slug: 'capstone',            name: 'Capstone Investment' },
  { slug: 'ellington',           name: 'Ellington Management' },
  { slug: 'saba',                name: 'Saba Capital Management' },
  { slug: 'anchorage',           name: 'Anchorage Capital' },
  { slug: 'bluemountain',        name: 'BlueMountain Capital' },
  { slug: 'ares',                name: 'Ares Management' },
  { slug: 'solus',               name: 'Solus Alternative Asset' },
  { slug: 'nea',                 name: 'NEA' },
  { slug: 'accel',               name: 'Accel' },
  { slug: 'sequoia',             name: 'Sequoia Capital' },
  { slug: 'andreessen',          name: 'Andreessen Horowitz' },
  { slug: 'lightspeed',          name: 'Lightspeed Venture' },
  { slug: 'generalcatalyst',     name: 'General Catalyst' },
  { slug: 'greylock',            name: 'Greylock' },
  { slug: 'kleiner',             name: 'Kleiner Perkins' },
  { slug: 'kpcb',                name: 'Kleiner Perkins' },
  { slug: 'bessemer',            name: 'Bessemer Venture Partners' },
  { slug: 'norwestventure',      name: 'Norwest Venture Partners' },
  { slug: 'insight',             name: 'Insight Partners' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 3: WORKDAY ATS — Big banks that don't use GH/Lever
// Goldman Sachs, JPMorgan, Morgan Stanley, Citi, BofA,
// Barclays, UBS, Deutsche, Credit Suisse/UBS, HSBC, Wells
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const WORKDAY_FIRMS = [
  { tenant: 'gs',            board: 'campus_career_site',       name: 'Goldman Sachs' },
  { tenant: 'gs',            board: 'experienced_professional', name: 'Goldman Sachs' },
  { tenant: 'jpmc',          board: 'campus',                   name: 'JPMorgan Chase' },
  { tenant: 'jpmc',          board: 'experienced',              name: 'JPMorgan Chase' },
  { tenant: 'morganstanley', board: 'campus',                   name: 'Morgan Stanley' },
  { tenant: 'morganstanley', board: 'experienced',              name: 'Morgan Stanley' },
  { tenant: 'citi',          board: 'campus',                   name: 'Citi' },
  { tenant: 'citi',          board: 'experienced',              name: 'Citi' },
  { tenant: 'bankofamerica', board: 'campus',                   name: 'Bank of America' },
  { tenant: 'bankofamerica', board: 'bankofamerica',            name: 'Bank of America' },
  { tenant: 'barclays',      board: 'barclays',                 name: 'Barclays' },
  { tenant: 'ubs',           board: 'campus',                   name: 'UBS' },
  { tenant: 'ubs',           board: 'experienced',              name: 'UBS' },
  { tenant: 'deutschebank',  board: 'careers',                  name: 'Deutsche Bank' },
  { tenant: 'hsbc',          board: 'hsbc',                     name: 'HSBC' },
  { tenant: 'wellsfargo',    board: 'wellsfargojobs',           name: 'Wells Fargo' },
  { tenant: 'credit',        board: 'creditsuisse',             name: 'UBS (CS)' },
  { tenant: 'bnymellon',     board: 'bnymellon',                name: 'BNY Mellon' },
  { tenant: 'statestreet',   board: 'statestreetcareers',       name: 'State Street' },
  { tenant: 'pnc',           board: 'pnccareers',               name: 'PNC Financial' },
  { tenant: 'truist',        board: 'truist',                   name: 'Truist' },
  { tenant: 'stifel',        board: 'stifelcareers',            name: 'Stifel' },
  { tenant: 'jefferies',     board: 'jefferiesllc',             name: 'Jefferies' },
  { tenant: 'nuveen',        board: 'nuveen',                   name: 'Nuveen' },
];

// Finance-relevant search terms for Workday
const WORKDAY_TERMS = [
  'investment banking', 'sales trading', 'equity research',
  'portfolio manager', 'quantitative', 'fixed income',
  'private equity', 'capital markets', 'wealth management',
  'derivatives', 'credit analyst', 'macro',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 4: eFINANCIALCAREERS — paginated, 5 pages deep
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const EFC_QUERIES = [
  'sales+trading', 'investment+banking', 'portfolio+manager',
  'equity+research', 'credit+research', 'quantitative+researcher',
  'quantitative+trader', 'private+equity', 'fixed+income+trader',
  'FX+trader', 'credit+trader', 'macro+strategist', 'hedge+fund',
  'prime+brokerage', 'structured+finance', 'leveraged+finance',
  'DCM', 'ECM', 'wealth+management', 'capital+markets',
  'derivatives+trader', 'rates+trader', 'commodities+trader',
  'distressed+debt', 'high+yield', 'securitization', 'CLO',
  'ABS+analyst', 'convertible+bonds', 'merger+arbitrage',
  'prop+trading', 'loan+syndication', 'project+finance',
  'real+estate+investment', 'infrastructure+investing',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 5: ADZUNA — free jobs aggregator API
// Register free at api.adzuna.com — 250 req/day free
// Has thousands of finance jobs from all major job boards
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const ADZUNA_APP_ID  = process.env.ADZUNA_APP_ID  || '';
const ADZUNA_API_KEY = process.env.ADZUNA_API_KEY || '';
const ADZUNA_QUERIES = [
  'investment banker', 'sales trader', 'equity researcher',
  'portfolio manager finance', 'quantitative analyst finance',
  'fixed income trader', 'FX trader bank', 'private equity associate',
  'hedge fund analyst', 'wealth manager', 'credit analyst bank',
  'derivatives trader', 'capital markets analyst', 'M&A analyst',
  'leveraged finance', 'DCM analyst bank', 'ECM analyst',
  'structured finance analyst', 'prime brokerage', 'macro trader',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC: GREENHOUSE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncGreenhouse() {
  console.log('\n── Greenhouse ATS ──');
  let fetched = 0, added = 0;
  const seen = new Set();
  for (const firm of GREENHOUSE_FIRMS) {
    if (seen.has(firm.name)) continue;
    try {
      const res = await fetchWithTimeout(
        `https://boards-api.greenhouse.io/v1/boards/${firm.slug}/jobs`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } }, 7000
      );
      if (!res.ok) continue;
      const { jobs = [] } = await res.json();
      if (!jobs.length) continue;
      seen.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${jobs.length}`);
      fetched += jobs.length;
      added += await insertBatch(jobs.map(r => ({
        source_id:   `gh-${r.id}`,
        title:       r.title,
        firm:        firm.name,
        location:    r.location?.name || null,
        description: (r.content||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
        apply_url:   r.absolute_url,
        source:      'Greenhouse',
        is_front_office: true, is_approved: true,
        posted_at:   r.updated_at || new Date().toISOString(),
      })));
      await sleep(100);
    } catch {}
  }
  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC: LEVER
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncLever() {
  console.log('── Lever ATS ──');
  let fetched = 0, added = 0;
  const seen = new Set();
  for (const firm of LEVER_FIRMS) {
    if (seen.has(firm.name)) continue;
    try {
      const res = await fetchWithTimeout(
        `https://api.lever.co/v0/postings/${firm.slug}?mode=json&limit=100`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } }, 7000
      );
      if (!res.ok) continue;
      const roles = await res.json();
      if (!Array.isArray(roles) || !roles.length) continue;
      seen.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${roles.length}`);
      fetched += roles.length;
      added += await insertBatch(roles.map(r => ({
        source_id:   `lever-${r.id}`,
        title:       r.text,
        firm:        firm.name,
        location:    r.categories?.location || null,
        description: (r.descriptionPlain||'').slice(0,1500),
        apply_url:   r.hostedUrl || r.applyUrl,
        source:      'Lever',
        is_front_office: true, is_approved: true,
        posted_at:   r.createdAt ? new Date(r.createdAt).toISOString() : new Date().toISOString(),
      })));
      await sleep(100);
    } catch {}
  }
  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC: WORKDAY — POST-based JSON API used by big banks
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncWorkday() {
  console.log('── Workday ATS (Big Banks) ──');
  let fetched = 0, added = 0;
  const seen = new Set(); // firm+term combos already successful

  for (const firm of WORKDAY_FIRMS) {
    for (const term of WORKDAY_TERMS) {
      const key = `${firm.name}|${term}`;
      if (seen.has(key)) continue;
      try {
        const url = `https://${firm.tenant}.wd5.myworkdayjobs.com/wday/cxs/${firm.tenant}/${firm.board}/jobs`;
        const res = await fetchWithTimeout(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json',
          },
          body: JSON.stringify({ appliedFacets: {}, limit: 20, offset: 0, searchText: term }),
        }, 10000);

        if (!res.ok) continue;
        const data = await res.json();
        const jobs = data.jobPostings || [];
        if (!jobs.length) continue;

        seen.add(key);
        console.log(`  ✓ ${firm.name} "${term}": ${jobs.length}`);
        fetched += jobs.length;

        added += await insertBatch(jobs.map(r => ({
          source_id:   `wd-${firm.tenant}-${r.bulletFields?.[0] || r.title}-${r.externalPath?.slice(-10) || Math.random()}`.replace(/\s/g,'-').slice(0,80),
          title:       r.title,
          firm:        firm.name,
          location:    r.locationsText || null,
          description: (r.jobDescription?.replace(/<[^>]+>/g,' ') || '').slice(0,1500),
          apply_url:   `https://${firm.tenant}.wd5.myworkdayjobs.com${r.externalPath || ''}`,
          source:      'Workday',
          is_front_office: true, is_approved: true,
          posted_at:   r.postedOn ? new Date(r.postedOn).toISOString() : new Date().toISOString(),
        })));
        await sleep(400); // be polite to Workday
      } catch {}
    }
  }
  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC: eFINANCIALCAREERS — 3 pages per query
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncEFC() {
  console.log('── eFinancialCareers ──');
  let added = 0;
  for (const query of EFC_QUERIES) {
    for (let page = 1; page <= 3; page++) {
      try {
        const url = `https://www.efinancialcareers.com/search?q=${query}&employment_type=permanent&format=rss&page=${page}`;
        const res = await fetchWithTimeout(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
        }, 9000);
        if (!res.ok) break;
        const xml   = await res.text();
        const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
        if (!items.length) break;
        if (page === 1) console.log(`  "${query}": ${items.length}+ results`);

        const mapped = items.flatMap(item => {
          const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/) ||[])[1]?.trim()||'';
          const link    = (item.match(/<link>(.*?)<\/link>/)                   ||[])[1]?.trim()||'';
          const desc    = (item.match(/<description><!\[CDATA\[(.*?)(?:\]\]>|$)/) ||[])[1]||'';
          const firm    = (item.match(/<source[^>]*>(.*?)<\/source>/)          ||[])[1]?.trim()||'Unknown';
          const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/)             ||[])[1]||'';
          if (!title || !link) return [];
          return [{
            source_id:   `efc-${Buffer.from(link).toString('base64').slice(0,40)}`,
            title, firm,
            location:    null,
            description: desc.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
            apply_url:   link,
            source:      'eFinancialCareers',
            is_front_office: true, is_approved: true,
            posted_at:   pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
          }];
        });
        if (mapped.length) added += await insertBatch(mapped);
        await sleep(400);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC: ADZUNA — free aggregator API
// Sign up at https://developer.adzuna.com/ (free, instant)
// Add ADZUNA_APP_ID and ADZUNA_API_KEY to GitHub secrets
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncAdzuna() {
  if (!ADZUNA_APP_ID || !ADZUNA_API_KEY) {
    console.log('── Adzuna: skipped (no API key — sign up free at developer.adzuna.com)\n');
    return 0;
  }
  console.log('── Adzuna Jobs API ──');
  let added = 0;
  for (const query of ADZUNA_QUERIES) {
    for (let page = 1; page <= 5; page++) {
      try {
        const url = `https://api.adzuna.com/v1/api/jobs/us/search/${page}` +
          `?app_id=${ADZUNA_APP_ID}&app_key=${ADZUNA_API_KEY}` +
          `&what=${encodeURIComponent(query)}&what_and=1&results_per_page=50` +
          `&category=finance-jobs&sort_by=date&max_days_old=60`;
        const res = await fetchWithTimeout(url, {}, 9000);
        if (!res.ok) break;
        const data = await res.json();
        const jobs = data.results || [];
        if (!jobs.length) break;
        if (page === 1) console.log(`  "${query}": ${data.count} total`);

        const mapped = jobs.map(r => ({
          source_id:   `adzuna-${r.id}`,
          title:       r.title,
          firm:        r.company?.display_name || 'Unknown',
          location:    r.location?.display_name || null,
          description: (r.description||'').slice(0,1500),
          apply_url:   r.redirect_url,
          source:      'Adzuna',
          is_front_office: true, is_approved: true,
          posted_at:   r.created || new Date().toISOString(),
        }));
        added += await insertBatch(mapped);
        await sleep(300);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function cleanupExpired() {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const { count } = await supabase.from('jobs').delete({ count: 'exact' })
    .lt('posted_at', cutoff).eq('is_featured', false);
  console.log(`── Cleanup: removed ${count || 0} expired\n`);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function main() {
  console.log(`\n[${new Date().toISOString()}] Front Office Jobs sync starting...\n`);
  const gh  = await syncGreenhouse();
  const lv  = await syncLever();
  const wd  = await syncWorkday();
  const efc = await syncEFC();
  const adz = await syncAdzuna();
  await cleanupExpired();
  const total = gh + lv + wd + efc + adz;
  console.log(`\n✓ Sync complete — ${total} new roles added`);
  console.log(`  GH:${gh}  Lever:${lv}  Workday:${wd}  EFC:${efc}  Adzuna:${adz}`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
