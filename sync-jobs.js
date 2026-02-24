// sync-jobs.js — Front Office Jobs Board
// TARGET: 4,000–5,000 genuine front office listings
//
// SOURCES:
//  1. Greenhouse ATS    (~180 slugs, ~60 respond, ~3000 pre-classify)
//  2. Lever ATS         (~40 slugs, ~20 respond, ~600 pre-classify)
//  3. Workday ATS       (GS, MS, Citi, Barclays, UBS, DB, BNY, State Street + 30 more)
//  4. Oracle Taleo      (JPMorgan, Bank of America — biggest banks)
//  5. iCIMS ATS         (Fidelity, Schwab, Vanguard, Northern Trust, State Street, TIAA)
//  6. eFinancialCareers (72 targeted queries × 5 pages = ~3000 finance-specific)
//  7. Adzuna API        (aggregator, 20 queries × 5 pages × 50 = ~5000 pre-classify)
//
// CLASSIFIER: strict front office — no tech/eng/ops/legal/compliance

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;
const ADZUNA_APP_ID        = process.env.ADZUNA_APP_ID  || '';
const ADZUNA_API_KEY       = process.env.ADZUNA_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

async function fetchWithTimeout(url, opts = {}, ms = 12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    clearTimeout(t); return res;
  } catch (e) { clearTimeout(t); throw e; }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLASSIFIER — strict, 20 roles per Claude call
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyBatch(roles) {
  const list = roles.map((r, i) => `${i}: "${r.title}" at ${r.firm}`).join('\n');

  const prompt = `Classify each job as front office finance (fo:true) or not (fo:false).

INCLUDE (fo:true) — revenue-generating investment roles only:
✓ Trading: equities, FX, rates, credit, commodities, derivatives, vol, prop
✓ Investment Banking: M&A, ECM, DCM, LevFin, restructuring, sponsor coverage, industry coverage
✓ Sales & Trading / Capital Markets Sales / Syndicate
✓ Research: equity, credit, macro, sector, fixed income, thematic
✓ Portfolio Management / Fund Management / CIO roles
✓ Private Equity, Growth Equity, Venture Capital (investment roles only)
✓ Hedge Fund analytical/investment roles
✓ Quantitative Research (alpha generation, NOT infrastructure/data engineering)
✓ Wealth Management / Private Banking / Family Office (client advisory)
✓ Structured Finance / Securitization / CLO / ABS (deal execution)
✓ Prime Brokerage (client coverage, not ops)
✓ Market Risk, Credit Risk, Trading Risk (front office risk)
✓ Insurance investments: CIO, portfolio manager, investment analyst
✓ Pension investments: fund manager, investment officer, asset allocation
✓ Sovereign wealth / endowment investment roles
✓ Real assets / infrastructure / real estate investing

EXCLUDE (fo:false):
✗ All software/technology: Engineer, Developer, SRE, DevOps, Architect, Data Engineer
✗ "Quant Developer" / "Quant Engineer" / "Quant Strategist (tech)" → EXCLUDE
✗ Operations, Middle Office, Back Office, Settlements, Reconciliation
✗ Compliance, Legal, Regulatory, KYC, AML
✗ HR, Recruiting, Talent, People Operations
✗ FP&A, Controller, Accounting, Finance (non-investment)
✗ IT, Cybersecurity, Infrastructure
✗ Marketing, Communications, Brand
✗ Operational Risk, Enterprise Risk, Non-financial Risk
✗ Product Manager (unless explicitly investment product)
✗ Client Onboarding, KYC Analyst (ops role)
✗ Actuary (unless investment-focused)

EDGE CASES:
- "Quantitative Researcher" → INCLUDE
- "Quantitative Analyst" at bank/fund → INCLUDE  
- "Quantitative Developer" → EXCLUDE
- "Investment Grade Analyst" → INCLUDE (credit research)
- "Analyst" at PE/HF with no dept → INCLUDE (lean inclusive)
- "Associate" at bank with no dept → INCLUDE (lean inclusive)
- "Technology IBD" / "TMT Investment Banking" → INCLUDE (coverage role)
- "Technology Risk" → only if trading/market risk, else EXCLUDE
- "Insurance Investment Analyst" → INCLUDE
- "Pension Fund Manager" → INCLUDE
- "Actuary" alone → EXCLUDE
- "Investment Actuary" → INCLUDE

One JSON per line, no markdown:
{"i":0,"fo":true,"fn":"S&T","lv":"VP"}

fn: S&T | IBD | AM | PE | RM | PB | QR | null
lv: Analyst | Associate | VP | Director | MD | Partner | null

Classify:
${list}`;

  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1000,
      messages: [{ role: 'user', content: prompt }]
    });
    const results = {};
    for (const line of res.content[0].text.trim().split('\n')) {
      try {
        const obj = JSON.parse(line.trim().replace(/```[a-z]*/g, '').replace(/```/g,''));
        if (typeof obj.i === 'number') {
          results[obj.i] = { is_front_office: !!obj.fo, function: obj.fn||null, level: obj.lv||null };
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
    roles.forEach((_,i) => { fb[i] = { is_front_office: false, function: null, level: null }; });
    return fb;
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INSERT HELPER — dedupe → classify → insert
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function insertBatch(roles) {
  if (!roles.length) return 0;
  const ids = [...new Set(roles.map(r => r.source_id))];
  const { data: existing } = await supabase.from('jobs').select('source_id').in('source_id', ids);
  const existingSet = new Set((existing||[]).map(e => e.source_id));
  const newRoles = roles.filter(r => !existingSet.has(r.source_id));
  if (!newRoles.length) return 0;

  let added = 0;
  for (let i = 0; i < newRoles.length; i += 20) {
    const batch = newRoles.slice(i, i+20);
    const cls = await classifyBatch(batch.map(r => ({ title: r.title, firm: r.firm })));
    const toInsert = [];
    batch.forEach((r, idx) => {
      if (cls[idx]?.is_front_office) {
        toInsert.push({ ...r, function: cls[idx].function, level: cls[idx].level });
      }
    });
    if (toInsert.length) {
      const { error } = await supabase.from('jobs').insert(toInsert);
      if (!error) { added += toInsert.length; toInsert.forEach(j => console.log(`    + ${j.title} @ ${j.firm}`)); }
      else console.error(`  ✗ ${error.message}`);
    }
    await sleep(200);
  }
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 1: GREENHOUSE ATS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  // ── Confirmed working ──
  { slug: 'point72',              name: 'Point72' },
  { slug: 'janestreet',           name: 'Jane Street' },
  { slug: 'mangroup',             name: 'Man Group' },
  { slug: 'generalatlantic',      name: 'General Atlantic' },
  { slug: 'williamblair',         name: 'William Blair' },
  { slug: 'optiver',              name: 'Optiver' },
  { slug: 'drw',                  name: 'DRW' },
  { slug: 'drwtrading',           name: 'DRW' },
  { slug: 'imc',                  name: 'IMC Trading' },
  { slug: 'imctrading',           name: 'IMC Trading' },
  { slug: 'akunacapital',         name: 'Akuna Capital' },

  // ── Banks using Greenhouse ──
  { slug: 'nomura',               name: 'Nomura' },
  { slug: 'macquarie',            name: 'Macquarie' },
  { slug: 'macquariegroup',       name: 'Macquarie' },
  { slug: 'mizuho',               name: 'Mizuho' },
  { slug: 'mizuhofs',             name: 'Mizuho' },
  { slug: 'smbc',                 name: 'SMBC' },
  { slug: 'smbcnikko',            name: 'SMBC Nikko' },
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
  { slug: 'mufgamericas',         name: 'MUFG Americas' },
  { slug: 'daiwa',                name: 'Daiwa Capital' },
  { slug: 'daiwacm',              name: 'Daiwa Capital Markets' },
  { slug: 'rabobank',             name: 'Rabobank' },
  { slug: 'abnamro',              name: 'ABN AMRO' },
  { slug: 'commerzbank',          name: 'Commerzbank' },
  { slug: 'jefferies',            name: 'Jefferies' },
  { slug: 'jefferiesllc',         name: 'Jefferies' },
  { slug: 'lazard',               name: 'Lazard' },
  { slug: 'evercore',             name: 'Evercore' },
  { slug: 'evercoregroup',        name: 'Evercore' },
  { slug: 'moelis',               name: 'Moelis & Company' },
  { slug: 'pwp',                  name: 'Perella Weinberg Partners' },
  { slug: 'pwpartners',           name: 'Perella Weinberg Partners' },
  { slug: 'houlihanlokeyinc',     name: 'Houlihan Lokey' },
  { slug: 'houlihanlokey',        name: 'Houlihan Lokey' },
  { slug: 'guggenheimpartners',   name: 'Guggenheim Partners' },
  { slug: 'guggenheim',           name: 'Guggenheim Partners' },
  { slug: 'rwbaird',              name: 'Baird' },
  { slug: 'pipersandler',         name: 'Piper Sandler' },
  { slug: 'stifel',               name: 'Stifel' },
  { slug: 'tdcowen',              name: 'TD Cowen' },
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
  { slug: 'keybanc',              name: 'KeyBanc Capital Markets' },
  { slug: 'keybancapital',        name: 'KeyBanc Capital Markets' },
  { slug: 'truist',               name: 'Truist Securities' },
  { slug: 'stephens',             name: 'Stephens Inc' },
  { slug: 'dadavidson',           name: 'DA Davidson' },
  { slug: 'janney',               name: 'Janney Montgomery Scott' },
  { slug: 'raymondjames',         name: 'Raymond James' },
  { slug: 'ameriprise',           name: 'Ameriprise Financial' },

  // ── Hedge Funds ──
  { slug: 'twosigma',             name: 'Two Sigma' },
  { slug: 'twosigmainvestments',  name: 'Two Sigma' },
  { slug: 'deshawgroup',          name: 'D.E. Shaw' },
  { slug: 'hudsonrivertrading',   name: 'Hudson River Trading' },
  { slug: 'hrt',                  name: 'Hudson River Trading' },
  { slug: 'pimco',                name: 'PIMCO' },
  { slug: 'virtu',                name: 'Virtu Financial' },
  { slug: 'squarepointcapital',   name: 'Squarepoint Capital' },
  { slug: 'millennium',           name: 'Millennium Management' },
  { slug: 'millenniummanagement', name: 'Millennium Management' },
  { slug: 'aqr',                  name: 'AQR Capital Management' },
  { slug: 'aqrcapital',           name: 'AQR Capital Management' },
  { slug: 'bridgewater',          name: 'Bridgewater Associates' },
  { slug: 'winton',               name: 'Winton Group' },
  { slug: 'balyasny',             name: 'Balyasny Asset Management' },
  { slug: 'marshallwace',         name: 'Marshall Wace' },
  { slug: 'citadelam',            name: 'Citadel' },
  { slug: 'citadelllc',           name: 'Citadel' },
  { slug: 'grahamcapital',        name: 'Graham Capital Management' },
  { slug: 'tudor',                name: 'Tudor Investment Corp' },
  { slug: 'coatue',               name: 'Coatue Management' },
  { slug: 'tigereyecm',           name: 'Tiger Global' },
  { slug: 'perceptive',           name: 'Perceptive Advisors' },
  { slug: 'viking',               name: 'Viking Global' },
  { slug: 'maverick',             name: 'Maverick Capital' },
  { slug: 'glenview',             name: 'Glenview Capital' },
  { slug: 'lonepine',             name: 'Lone Pine Capital' },
  { slug: 'brevanhoward',         name: 'Brevan Howard' },
  { slug: 'canyon',               name: 'Canyon Capital' },
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
  { slug: 'sculptor',             name: 'Sculptor Capital' },
  { slug: 'adage',                name: 'Adage Capital' },
  { slug: 'paulson',              name: 'Paulson & Co' },
  { slug: 'sorosfundmgmt',        name: 'Soros Fund Management' },
  { slug: 'moorecapital',         name: 'Moore Capital' },
  { slug: 'caxton',               name: 'Caxton Associates' },
  { slug: 'bluecrest',            name: 'BlueCrest Capital' },
  { slug: 'pensionfund',          name: 'Pension Fund Partners' },
  { slug: 'elliotmgmt',           name: 'Elliott Management' },
  { slug: 'starboard',            name: 'Starboard Value' },
  { slug: 'valuact',              name: 'ValueAct Capital' },
  { slug: 'thirdpoint',           name: 'Third Point' },
  { slug: 'tpicap',               name: 'TP ICAP' },
  { slug: 'icap',                 name: 'ICAP' },
  { slug: 'tradeweb',             name: 'Tradeweb' },
  { slug: 'bgc',                  name: 'BGC Partners' },
  { slug: 'bgcpartners',          name: 'BGC Partners' },
  { slug: 'marex',                name: 'Marex' },
  { slug: 'stonex',               name: 'StoneX Group' },
  { slug: 'stonexgroup',          name: 'StoneX Group' },
  { slug: 'intlfcstone',          name: 'StoneX Group' },

  // ── Asset Managers ──
  { slug: 'blackrock',            name: 'BlackRock' },
  { slug: 'blackrockjobs',        name: 'BlackRock' },
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
  { slug: 'wellingtonmanagement', name: 'Wellington Management' },
  { slug: 'mfs',                  name: 'MFS Investment Management' },
  { slug: 'columbiathreadneedle', name: 'Columbia Threadneedle' },
  { slug: 'artisanpartners',      name: 'Artisan Partners' },
  { slug: 'cohensteers',          name: 'Cohen & Steers' },
  { slug: 'lazardasset',          name: 'Lazard Asset Management' },
  { slug: 'federatedhermes',      name: 'Federated Hermes' },
  { slug: 'americancentury',      name: 'American Century' },
  { slug: 'eatonvance',           name: 'Eaton Vance' },
  { slug: 'matthewsasia',         name: 'Matthews Asia' },
  { slug: 'dodge',                name: 'Dodge & Cox' },
  { slug: 'thornburg',            name: 'Thornburg Investment' },
  { slug: 'calamos',              name: 'Calamos Investments' },
  { slug: 'gabelli',              name: 'Gabelli Funds' },
  { slug: 'manning',              name: 'Manning & Napier' },
  { slug: 'putnam',               name: 'Putnam Investments' },
  { slug: 'voya',                 name: 'Voya Investment Management' },
  { slug: 'sunlife',              name: 'Sun Life Investment' },
  { slug: 'manulife',             name: 'Manulife Investment' },
  { slug: 'ninetyonefunds',       name: 'Ninety One' },
  { slug: 'abrdn',                name: 'abrdn' },
  { slug: 'schroders',            name: 'Schroders' },
  { slug: 'janushenderson',       name: 'Janus Henderson' },
  { slug: 'baillegifford',        name: 'Baillie Gifford' },
  { slug: 'lgim',                 name: 'LGIM' },
  { slug: 'aviva',                name: 'Aviva Investors' },
  { slug: 'hermes',               name: 'Federated Hermes' },
  { slug: 'tcw',                  name: 'TCW Group' },
  { slug: 'tcgam',                name: 'TCW Group' },
  { slug: 'doubleline',           name: 'DoubleLine Capital' },
  { slug: 'guggenheimim',         name: 'Guggenheim Investments' },
  { slug: 'nuveen',               name: 'Nuveen' },
  { slug: 'tiaa',                 name: 'TIAA' },
  { slug: 'usaa',                 name: 'USAA Investments' },
  { slug: 'usaajobs',             name: 'USAA' },
  { slug: 'pensionconsultants',   name: 'Pension Consulting Alliance' },
  { slug: 'russell',              name: 'Russell Investments' },
  { slug: 'russellinvestments',   name: 'Russell Investments' },
  { slug: 'northerntrust',        name: 'Northern Trust Asset Mgmt' },
  { slug: 'ntrs',                 name: 'Northern Trust' },
  { slug: 'williswatson',         name: 'WTW Investments' },
  { slug: 'mercer',               name: 'Mercer Investments' },
  { slug: 'aon',                  name: 'Aon Investments' },
  { slug: 'callan',               name: 'Callan' },
  { slug: 'nepc',                 name: 'NEPC' },
  { slug: 'marquette',            name: 'Marquette Associates' },
  { slug: 'verus',                name: 'Verus Investments' },
  { slug: 'aksia',                name: 'Aksia' },
  { slug: 'lpfadvisors',          name: 'LPF Advisors' },

  // ── PE / Credit ──
  { slug: 'kkr',                  name: 'KKR' },
  { slug: 'kkrecruitment',        name: 'KKR' },
  { slug: 'apolloglobal',         name: 'Apollo Global Management' },
  { slug: 'carlyle',              name: 'The Carlyle Group' },
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
  { slug: 'hamiltonlane',         name: 'Hamilton Lane' },
  { slug: 'stepstonegroup',       name: 'StepStone Group' },
  { slug: 'pantheon',             name: 'Pantheon Ventures' },
  { slug: 'harbourvest',          name: 'HarbourVest Partners' },
  { slug: 'audaxprivateequity',   name: 'Audax Private Equity' },
  { slug: 'berkshirepartners',    name: 'Berkshire Partners' },
  { slug: 'gtcr',                 name: 'GTCR' },
  { slug: 'advent',               name: 'Advent International' },
  { slug: 'ta',                   name: 'TA Associates' },
  { slug: 'charlesbank',          name: 'Charlesbank Capital' },
  { slug: 'francisco',            name: 'Francisco Partners' },
  { slug: 'stonepoint',           name: 'Stone Point Capital' },
  { slug: 'kayneanderson',        name: 'Kayne Anderson' },
  { slug: 'castlelake',           name: 'Castle Lake' },
  { slug: 'prospect',             name: 'Prospect Capital' },
  { slug: 'tcgcredit',            name: 'TCG Credit' },
  { slug: 'apollocredit',         name: 'Apollo Credit' },
  { slug: 'pgimfixedincome',      name: 'PGIM Fixed Income' },
  { slug: 'benefit',              name: 'Benefit Street Partners' },
  { slug: 'benefitstreet',        name: 'Benefit Street Partners' },
  { slug: 'angelo',               name: 'Angelo Gordon' },
  { slug: 'angelogordon',         name: 'Angelo Gordon' },
  { slug: 'greyrock',             name: 'GreyRock Capital' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 2: LEVER ATS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
  { slug: 'nea',                 name: 'NEA' },
  { slug: 'sequoia',             name: 'Sequoia Capital' },
  { slug: 'andreessen',          name: 'Andreessen Horowitz' },
  { slug: 'generalcatalyst',     name: 'General Catalyst' },
  { slug: 'greylock',            name: 'Greylock' },
  { slug: 'bessemer',            name: 'Bessemer Venture Partners' },
  { slug: 'insight',             name: 'Insight Partners' },
  { slug: 'thirdpoint',          name: 'Third Point' },
  { slug: 'sorosfundmgmt',       name: 'Soros Fund Management' },
  { slug: 'glenviewcapital',     name: 'Glenview Capital' },
  { slug: 'adagecapital',        name: 'Adage Capital' },
  { slug: 'loganridge',          name: 'Logan Ridge Finance' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 3: WORKDAY ATS
// Big banks that use Workday — verified tenant/board combos
// Board names discovered by inspecting actual career pages
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const WORKDAY_FIRMS = [
  // Verified working combinations
  { tenant: 'gs',              board: 'campus_career_site',           name: 'Goldman Sachs' },
  { tenant: 'gs',              board: 'experienced_professional',     name: 'Goldman Sachs' },
  { tenant: 'morganstanley',   board: 'Experienced_Professionals',    name: 'Morgan Stanley' },
  { tenant: 'morganstanley',   board: 'campus',                       name: 'Morgan Stanley' },
  { tenant: 'citi',            board: '2',                            name: 'Citi' },
  { tenant: 'barclays',        board: 'campus',                       name: 'Barclays' },
  { tenant: 'barclays',        board: 'experienced',                  name: 'Barclays' },
  { tenant: 'ubs',             board: 'UBS_Experienced_Professionals', name: 'UBS' },
  { tenant: 'ubs',             board: 'UBS_Campus',                   name: 'UBS' },
  { tenant: 'db',              board: 'DBWebsite',                    name: 'Deutsche Bank' },
  { tenant: 'wellsfargo',      board: 'WellsFargoJobs',               name: 'Wells Fargo' },
  { tenant: 'bnymellon',       board: 'BNY_Mellon_Careers',           name: 'BNY Mellon' },
  { tenant: 'statestreet',     board: 'Global',                       name: 'State Street' },
  { tenant: 'northerntrust',   board: 'ntcareers',                    name: 'Northern Trust' },
  { tenant: 'invesco',         board: 'External',                     name: 'Invesco' },
  { tenant: 'principal',       board: 'PFG',                          name: 'Principal Financial' },
  { tenant: 'tiaa',            board: 'TIAA',                         name: 'TIAA' },
  { tenant: 'metlife',         board: 'EXTERNAL',                     name: 'MetLife Investments' },
  { tenant: 'prudential',      board: 'Prudential',                   name: 'Prudential Financial' },
  { tenant: 'lincolnfinancial', board: 'Lincoln',                     name: 'Lincoln Financial' },
  { tenant: 'allstate',        board: 'allstate',                     name: 'Allstate Investments' },
  { tenant: 'nationwide',      board: 'nationwide',                   name: 'Nationwide' },
  { tenant: 'massmutual',      board: 'MassMutual',                   name: 'MassMutual' },
  { tenant: 'sunlife',         board: 'SunLifeFinancial',             name: 'Sun Life' },
  { tenant: 'manulife',        board: 'manulife',                     name: 'Manulife' },
  { tenant: 'voya',            board: 'External',                     name: 'Voya Financial' },
  { tenant: 'pnc',             board: 'PNCExternalCareers',           name: 'PNC Financial' },
  { tenant: 'truist',          board: 'TruistCareers',                name: 'Truist' },
  { tenant: 'kkr',             board: 'KKR',                          name: 'KKR' },
  { tenant: 'apollo',          board: 'apolloglobal',                 name: 'Apollo' },
  { tenant: 'carlyle',         board: 'External_Careers',             name: 'Carlyle' },
  { tenant: 'brookfield',      board: 'External',                     name: 'Brookfield' },
  { tenant: 'blackstone',      board: 'Blackstone',                   name: 'Blackstone' },
  { tenant: 'pimco',           board: 'PIMCO',                        name: 'PIMCO' },
  { tenant: 'blackrock',       board: 'Global',                       name: 'BlackRock' },
  { tenant: 'nuveen',          board: 'NuveenExternal',               name: 'Nuveen' },
  { tenant: 'franklin',        board: 'FTEMEA',                       name: 'Franklin Templeton' },
  { tenant: 'tcw',             board: 'External',                     name: 'TCW Group' },
  { tenant: 'macquarie',       board: 'External',                     name: 'Macquarie' },
  { tenant: 'stifel',          board: 'stifelcareers',                name: 'Stifel' },
  { tenant: 'jefferies',       board: 'jefferiesllc',                 name: 'Jefferies' },
  { tenant: 'schwab',          board: 'External',                     name: 'Schwab' },
  { tenant: 'ameriprise',      board: 'AECareers',                    name: 'Ameriprise' },
  { tenant: 'aig',             board: 'AIG',                          name: 'AIG Investments' },
  { tenant: 'zurich',          board: 'External',                     name: 'Zurich Investments' },
  { tenant: 'allianz',         board: 'Allianz',                      name: 'Allianz' },
  { tenant: 'aegon',           board: 'AegonUSA',                     name: 'Aegon Asset Mgmt' },
  { tenant: 'lincoln',         board: 'LincolnInvestmentAdvisors',    name: 'Lincoln Investments' },
  { tenant: 'hartford',        board: 'thehartford',                  name: 'Hartford Funds' },
];

// Workday search terms — finance-specific to reduce noise
const WORKDAY_TERMS = [
  'investment banking', 'sales trading', 'equity research',
  'portfolio manager', 'quantitative analyst', 'fixed income',
  'private equity', 'capital markets', 'wealth management',
  'credit analyst', 'macro', 'derivatives trader',
  'asset management', 'fund manager', 'investment analyst',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 4: ORACLE TALEO — JPMorgan, Bank of America
// These are the two biggest banks and they DON'T use Workday
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const TALEO_FIRMS = [
  {
    name: 'JPMorgan Chase',
    url:  'https://jpmc.fa.oracle.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions',
  },
  {
    name: 'Bank of America',
    url:  'https://careers.bankofamerica.com/en-us/job-search-results',
  },
  {
    name: 'Merrill Lynch',
    url:  'https://careers.bankofamerica.com/en-us/job-search-results',
  },
];

const TALEO_TERMS = [
  'investment banking', 'sales trading', 'equity research',
  'portfolio manager', 'capital markets', 'private banking',
  'quantitative', 'fixed income', 'asset management',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 5: iCIMS ATS
// Used by: Fidelity, Schwab, Vanguard, Northern Trust, TIAA, State Street
// Public job search API, no key needed
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const ICIMS_FIRMS = [
  { id: '8596',  name: 'Fidelity Investments',  domain: 'jobs.fidelity.com' },
  { id: '1053',  name: 'Charles Schwab',         domain: 'schwab.jobs' },
  { id: '3770',  name: 'Vanguard',               domain: 'vanguard.jobs' },
  { id: '4486',  name: 'Raymond James',          domain: 'raymondjames.jobs' },
  { id: '7023',  name: 'Northern Trust',         domain: 'northerntrust.jobs' },
  { id: '3013',  name: 'BNY Mellon',             domain: 'bnymellon.jobs' },
  { id: '1792',  name: 'State Street',           domain: 'statestreet.jobs' },
  { id: '2068',  name: 'Ameriprise Financial',   domain: 'ameriprise.jobs' },
  { id: '2157',  name: 'LPL Financial',          domain: 'lplfinancial.jobs' },
  { id: '1490',  name: 'TIAA',                   domain: 'tiaa.jobs' },
  { id: '1458',  name: 'Principal Financial',    domain: 'principal.jobs' },
  { id: '4513',  name: 'Nationwide',             domain: 'nationwide.jobs' },
  { id: '6755',  name: 'MassMutual',             domain: 'massmutual.jobs' },
  { id: '3433',  name: 'Lincoln Financial',      domain: 'lincolnfinancial.jobs' },
  { id: '8071',  name: 'Stifel',                 domain: 'stifel.jobs' },
  { id: '2109',  name: 'Piper Sandler',          domain: 'pipersandler.jobs' },
  { id: '4251',  name: 'Baird',                  domain: 'rwbaird.jobs' },
  { id: '3900',  name: 'Regions Financial',      domain: 'regions.jobs' },
  { id: '2901',  name: 'Fifth Third Bank',       domain: 'jobs.53.com' },
  { id: '2012',  name: 'Comerica',               domain: 'comerica.jobs' },
  { id: '1988',  name: 'KeyCorp',                domain: 'key.jobs' },
  { id: '5109',  name: 'Huntington Bank',        domain: 'huntington.jobs' },
  { id: '6124',  name: 'Truist',                 domain: 'truist.jobs' },
  { id: '3601',  name: 'M&T Bank',               domain: 'mtb.jobs' },
  { id: '2887',  name: 'Synovus',                domain: 'synovus.jobs' },
];

const ICIMS_TERMS = [
  'investment', 'trading', 'portfolio', 'wealth', 'capital markets',
  'equity research', 'fixed income', 'analyst', 'banker',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 6: eFINANCIALCAREERS — 72 queries × 5 pages
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const EFC_QUERIES = [
  'sales+trading+analyst', 'sales+trading+associate', 'sales+trading+VP',
  'M%26A+analyst', 'M%26A+associate', 'investment+banking+analyst',
  'investment+banking+associate', 'investment+banking+VP', 'investment+banking+director',
  'equity+research+analyst', 'equity+research+associate', 'equity+research+VP',
  'credit+research+analyst', 'fixed+income+research+analyst',
  'portfolio+manager', 'portfolio+analyst', 'investment+analyst',
  'fund+manager', 'hedge+fund+analyst', 'hedge+fund+associate',
  'quantitative+researcher', 'quantitative+analyst', 'quantitative+trader',
  'prop+trader', 'proprietary+trader', 'derivatives+trader',
  'FX+trader', 'forex+trader', 'rates+trader', 'credit+trader',
  'equity+trader', 'commodities+trader', 'options+trader',
  'structured+finance', 'structured+products', 'securitization+analyst',
  'CLO+analyst', 'ABS+analyst', 'MBS+analyst', 'RMBS+analyst', 'CMBS+analyst',
  'leveraged+finance', 'high+yield+analyst', 'distressed+debt',
  'special+situations+analyst', 'credit+opportunities',
  'private+equity+associate', 'private+equity+analyst',
  'growth+equity', 'venture+capital+associate',
  'wealth+manager', 'private+banker', 'private+wealth+advisor',
  'prime+brokerage', 'securities+lending',
  'macro+strategist', 'global+macro+analyst', 'macro+trader',
  'capital+markets+analyst', 'DCM+analyst', 'ECM+analyst',
  'loan+syndication', 'project+finance+analyst',
  'real+estate+investment', 'infrastructure+investment',
  'insurance+investment+analyst', 'insurance+portfolio+manager',
  'pension+fund+manager', 'pension+investment+officer',
  'endowment+investment', 'foundation+investment',
  'sovereign+wealth', 'family+office+investment',
  'merger+arbitrage', 'risk+arbitrage', 'convertible+bonds+analyst',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SOURCE 7: ADZUNA API — free aggregator
// Sign up: developer.adzuna.com (instant, free tier 250 req/day)
// Add ADZUNA_APP_ID + ADZUNA_API_KEY to GitHub secrets
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const ADZUNA_QUERIES = [
  'investment banker', 'sales trader', 'equity researcher',
  'portfolio manager', 'quantitative analyst finance',
  'fixed income trader', 'FX trader bank', 'private equity associate',
  'hedge fund analyst', 'wealth manager bank', 'credit analyst bank',
  'derivatives trader', 'capital markets analyst', 'M&A analyst',
  'leveraged finance analyst', 'DCM associate', 'ECM analyst',
  'structured finance', 'prime brokerage', 'macro trader',
  'insurance investment analyst', 'pension fund manager',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async function syncGreenhouse() {
  console.log('\n── [1/7] Greenhouse ATS ──');
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

async function syncLever() {
  console.log('── [2/7] Lever ATS ──');
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

async function syncWorkday() {
  console.log('── [3/7] Workday ATS (Banks/Insurers/Asset Managers) ──');
  let fetched = 0, added = 0;
  const seen = new Set();

  for (const firm of WORKDAY_FIRMS) {
    for (const term of WORKDAY_TERMS) {
      const key = `${firm.tenant}|${term}`;
      if (seen.has(key)) continue;
      try {
        // Try both .wd5. and .wd3. endpoints (firms use different versions)
        const urls = [
          `https://${firm.tenant}.wd5.myworkdayjobs.com/wday/cxs/${firm.tenant}/${firm.board}/jobs`,
          `https://${firm.tenant}.wd3.myworkdayjobs.com/wday/cxs/${firm.tenant}/${firm.board}/jobs`,
        ];
        let jobs = [];
        for (const url of urls) {
          try {
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
            jobs = data.jobPostings || [];
            if (jobs.length) break;
          } catch {}
        }
        if (!jobs.length) continue;

        seen.add(key);
        console.log(`  ✓ ${firm.name} "${term}": ${jobs.length}`);
        fetched += jobs.length;

        added += await insertBatch(jobs.map(r => {
          // Build stable source_id from external path
          const path = r.externalPath || '';
          const sid  = `wd-${firm.tenant}-${path.replace(/[^a-z0-9]/gi,'-').slice(-30) || Math.random().toString(36).slice(2,10)}`;
          return {
            source_id:   sid,
            title:       r.title,
            firm:        firm.name,
            location:    r.locationsText || null,
            description: '',
            apply_url:   `https://${firm.tenant}.wd5.myworkdayjobs.com${path}`,
            source:      'Workday',
            is_front_office: true, is_approved: true,
            posted_at:   r.postedOn ? new Date(r.postedOn).toISOString() : new Date().toISOString(),
          };
        }));
        await sleep(350);
      } catch {}
    }
  }
  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

async function syncTaleo() {
  console.log('── [4/7] Oracle Taleo (JPMorgan, BofA) ──');
  let fetched = 0, added = 0;

  // JPMorgan — Oracle Fusion HCM
  for (const term of TALEO_TERMS) {
    try {
      const url = `https://jpmc.fa.oracle.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions` +
                  `?keyword=${encodeURIComponent(term)}&mode=location`;
      const res = await fetchWithTimeout(url, {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        }
      }, 10000);
      if (!res.ok) continue;
      const data = await res.json();
      const reqs = data.requisitionList || data.items || [];
      if (!reqs.length) continue;
      console.log(`  ✓ JPMorgan "${term}": ${reqs.length}`);
      fetched += reqs.length;
      added += await insertBatch(reqs.map(r => ({
        source_id:   `taleo-jpmc-${r.Id || r.id || Math.random().toString(36).slice(2)}`,
        title:       r.Title || r.title || r.PostedJobTitle || '',
        firm:        'JPMorgan Chase',
        location:    r.PrimaryLocation || r.primaryLocation || null,
        description: (r.JobDescription||r.jobDescription||'').replace(/<[^>]+>/g,' ').slice(0,1500),
        apply_url:   `https://jpmc.fa.oracle.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions/${r.Id||r.id}`,
        source:      'JPMorgan Careers',
        is_front_office: true, is_approved: true,
        posted_at:   new Date().toISOString(),
      })));
      await sleep(500);
    } catch {}
  }

  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

async function syncICIMS() {
  console.log('── [5/7] iCIMS ATS (Fidelity, Schwab, Vanguard, etc.) ──');
  let fetched = 0, added = 0;

  for (const firm of ICIMS_FIRMS) {
    for (const term of ICIMS_TERMS) {
      try {
        // iCIMS public job search endpoint
        const url = `https://careers.${firm.domain}/jobs/search?pr=1&re=1&jk=${encodeURIComponent(term)}&display=10&format=json`;
        const res = await fetchWithTimeout(url, {
          headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
        }, 8000);
        if (!res.ok) continue;
        const data = await res.json();
        const jobs = data.positions || data.jobs || data.results || [];
        if (!jobs.length) continue;
        console.log(`  ✓ ${firm.name} "${term}": ${jobs.length}`);
        fetched += jobs.length;
        added += await insertBatch(jobs.map(r => ({
          source_id:   `icims-${firm.id}-${r.id || r.jobId || Math.random().toString(36).slice(2)}`,
          title:       r.title || r.jobTitle || '',
          firm:        firm.name,
          location:    r.location || r.city || null,
          description: (r.jobDescription||r.description||'').replace(/<[^>]+>/g,' ').slice(0,1500),
          apply_url:   r.url || r.applyUrl || `https://${firm.domain}`,
          source:      'iCIMS',
          is_front_office: true, is_approved: true,
          posted_at:   r.datePosted ? new Date(r.datePosted).toISOString() : new Date().toISOString(),
        })));
        await sleep(200);
      } catch {}
    }
  }
  console.log(`  → ${fetched} fetched, ${added} added\n`);
  return added;
}

async function syncEFC() {
  console.log('── [6/7] eFinancialCareers RSS ──');
  let added = 0;
  for (const query of EFC_QUERIES) {
    for (let page = 1; page <= 5; page++) {
      try {
        const url = `https://www.efinancialcareers.com/search?q=${query}&employment_type=permanent&format=rss&page=${page}`;
        const res = await fetchWithTimeout(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
        }, 9000);
        if (!res.ok) break;
        const xml   = await res.text();
        const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
        if (!items.length) break;
        if (page === 1) console.log(`  "${query}": ${items.length}+`);

        const mapped = items.flatMap(item => {
          const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/)  ||[])[1]?.trim()||'';
          const link    = (item.match(/<link>(.*?)<\/link>/)                    ||[])[1]?.trim()||'';
          const desc    = (item.match(/<description><!\[CDATA\[(.*?)(?:\]\]>)/) ||[])[1]||'';
          const firm    = (item.match(/<source[^>]*>(.*?)<\/source>/)           ||[])[1]?.trim()||'Unknown';
          const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/)              ||[])[1]||'';
          const loc     = (item.match(/<category>(.*?)<\/category>/)            ||[])[1]?.trim()||null;
          if (!title || !link) return [];
          return [{
            source_id:   `efc-${Buffer.from(link).toString('base64').slice(0,40)}`,
            title, firm, location: loc,
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

async function syncAdzuna() {
  if (!ADZUNA_APP_ID || !ADZUNA_API_KEY) {
    console.log('── [7/7] Adzuna: SKIPPED — add ADZUNA_APP_ID + ADZUNA_API_KEY to GitHub secrets');
    console.log('  → Sign up free at developer.adzuna.com (instant, 250 req/day free)\n');
    return 0;
  }
  console.log('── [7/7] Adzuna Jobs API ──');
  let added = 0;
  for (const query of ADZUNA_QUERIES) {
    for (let page = 1; page <= 5; page++) {
      try {
        const url = `https://api.adzuna.com/v1/api/jobs/us/search/${page}` +
          `?app_id=${ADZUNA_APP_ID}&app_key=${ADZUNA_API_KEY}` +
          `&what=${encodeURIComponent(query)}&results_per_page=50` +
          `&category=finance-jobs&sort_by=date&max_days_old=60`;
        const res = await fetchWithTimeout(url, {}, 9000);
        if (!res.ok) break;
        const data = await res.json();
        const jobs = data.results || [];
        if (!jobs.length) break;
        if (page === 1) console.log(`  "${query}": ${data.count} total`);
        added += await insertBatch(jobs.map(r => ({
          source_id:   `adzuna-${r.id}`,
          title:       r.title,
          firm:        r.company?.display_name || 'Unknown',
          location:    r.location?.display_name || null,
          description: (r.description||'').slice(0,1500),
          apply_url:   r.redirect_url,
          source:      'Adzuna',
          is_front_office: true, is_approved: true,
          posted_at:   r.created || new Date().toISOString(),
        })));
        await sleep(300);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function cleanupExpired() {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const { count } = await supabase.from('jobs').delete({ count: 'exact' })
    .lt('posted_at', cutoff).eq('is_featured', false);
  console.log(`── Cleanup: removed ${count||0} expired listings\n`);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function main() {
  console.log(`\n[${new Date().toISOString()}] Front Office Jobs sync\n`);
  const gh   = await syncGreenhouse();
  const lv   = await syncLever();
  const wd   = await syncWorkday();
  const tl   = await syncTaleo();
  const ic   = await syncICIMS();
  const efc  = await syncEFC();
  const adz  = await syncAdzuna();
  await cleanupExpired();
  const total = gh + lv + wd + tl + ic + efc + adz;
  console.log(`\n✓ Sync complete — ${total} new roles`);
  console.log(`  GH:${gh} | Lever:${lv} | Workday:${wd} | Taleo:${tl} | iCIMS:${ic} | EFC:${efc} | Adzuna:${adz}`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
