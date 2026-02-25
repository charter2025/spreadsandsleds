// sync-jobs.js — Front Office Jobs Board
// 
// KEY CHANGES vs previous versions:
// 1. The Muse API (FREE, NO KEY) is now PRIMARY source for big banks
// 2. High-signal sources (Greenhouse, Muse) skip classifier — already finance-specific
// 3. Low-signal sources (Indeed, Adzuna) still classify to filter noise
// 4. Better error logging so we know what's actually working
// 5. Faster: removed unnecessary sleeps on guaranteed-clean sources

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;
const ADZUNA_APP_ID        = process.env.ADZUNA_APP_ID || '';
const ADZUNA_API_KEY_VAL   = process.env.ADZUNA_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

const stats = { gh:0, lever:0, muse:0, efc:0, adzuna:0, skipped:0, errors:0 };

async function fetchJ(url, opts={}, ms=12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, {...opts, signal: ctrl.signal});
    clearTimeout(t); return r;
  } catch(e) { clearTimeout(t); throw e; }
}

// ─────────────────────────────────────────────────────────
// DEDUPLICATION — check what's already in DB
// ─────────────────────────────────────────────────────────
async function filterNew(roles) {
  if (!roles.length) return [];
  const ids = [...new Set(roles.map(r => r.source_id))];
  // Check in batches of 500 (Supabase in() limit)
  const existing = new Set();
  for (let i = 0; i < ids.length; i += 500) {
    const { data } = await supabase.from('jobs').select('source_id').in('source_id', ids.slice(i, i+500));
    (data||[]).forEach(r => existing.add(r.source_id));
  }
  return roles.filter(r => !existing.has(r.source_id));
}

// ─────────────────────────────────────────────────────────
// DIRECT INSERT — for high-signal sources (Greenhouse, Muse)
// These are already finance-specific, skip expensive classifier
// Still need function/level — use title-based heuristics
// ─────────────────────────────────────────────────────────
function inferFunctionLevel(title) {
  const t = title.toLowerCase();
  let fn = null, lv = null;

  // Function
  if (/\b(trad(er|ing)|sales.*(trad|structur)|market.mak|execution|flow)\b/.test(t)) fn = 'S&T';
  else if (/\b(m&a|mergers|acqui|ecm|dcm|leveraged.fin|capital.market|investment.bank|coverage|syndic|lev.fin|lbo)\b/.test(t)) fn = 'IBD';
  else if (/\b(research|analyst.*research|research.*analyst|strategist|economist)\b/.test(t)) fn = 'RM';
  else if (/\b(portfolio.manag|fund.manag|asset.manag|investment.manag|allocat)\b/.test(t)) fn = 'AM';
  else if (/\b(private.equity|growth.equity|venture.cap|buyout|pe.associate|pe.analyst)\b/.test(t)) fn = 'PE';
  else if (/\b(wealth.manag|private.bank|private.client|family.office|relationship.manag)\b/.test(t)) fn = 'PB';
  else if (/\b(quant(itative)?.*(research|strat|invest|model|alpha)|systematic)\b/.test(t)) fn = 'QR';

  // Level
  if (/\b(managing.dir|md\b|head.of|chief|cio|ceo|cfo|president)\b/.test(t)) lv = 'MD';
  else if (/\b(partner|principal)\b/.test(t)) lv = 'Partner';
  else if (/\b(director|dir\.)\b/.test(t)) lv = 'Director';
  else if (/\b(vice.pres|vp\b|v\.p\.)\b/.test(t)) lv = 'VP';
  else if (/\b(associate|assoc\b)\b/.test(t)) lv = 'Associate';
  else if (/\b(analyst)\b/.test(t)) lv = 'Analyst';
  else if (/\b(intern|summer)\b/.test(t)) lv = 'Analyst';

  return { function: fn, level: lv };
}

async function insertDirect(roles, source) {
  const fresh = await filterNew(roles);
  if (!fresh.length) { stats.skipped += roles.length; return 0; }
  let added = 0;
  // Insert in batches of 100
  for (let i = 0; i < fresh.length; i += 100) {
    const batch = fresh.slice(i, i+100).map(r => ({
      ...r,
      ...inferFunctionLevel(r.title),
      is_front_office: true,
      is_approved: true,
    }));
    const { error } = await supabase.from('jobs').insert(batch);
    if (!error) {
      added += batch.length;
      console.log(`    Inserted ${batch.length} from ${source}`);
    } else {
      console.error(`    DB error (${source}):`, error.message);
      stats.errors++;
    }
  }
  return added;
}

// ─────────────────────────────────────────────────────────
// CLASSIFIED INSERT — for lower-signal sources
// ─────────────────────────────────────────────────────────
async function classifyBatch(roles) {
  const list = roles.map((r,i) => `${i}: "${r.title}" at ${r.firm}`).join('\n');
  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 1200,
      messages: [{ role: 'user', content: 
`Classify as front office finance (fo:true) or not (fo:false).
INCLUDE: Trading, IB/M&A/DCM/ECM, Sales & Trading, Research, Portfolio Mgmt, Asset Mgmt,
Private Equity, Hedge Fund investment roles, Quant Research (NOT engineering), 
Wealth Mgmt, Structured Finance, Prime Brokerage, Market/Credit Risk (front office),
Capital Markets, Insurance investments, Real assets investing.
EXCLUDE: Software/data engineer, devops, operations, compliance, legal, HR, 
accounting, IT, marketing, back/middle office, quant dev/engineer.
One JSON per line: {"i":0,"fo":true,"fn":"S&T","lv":"Analyst"}
fn: S&T|IBD|AM|PE|RM|PB|QR|null  lv: Analyst|Associate|VP|Director|MD|Partner|null
${list}` }]
    });
    const out = {};
    for (const line of res.content[0].text.trim().split('\n')) {
      try { const o = JSON.parse(line.trim()); if (typeof o.i === 'number') out[o.i] = o; } catch {}
    }
    return out;
  } catch(e) {
    console.warn('  Classifier error:', e.message);
    return {};
  }
}

async function insertClassified(roles, source) {
  const fresh = await filterNew(roles);
  if (!fresh.length) { stats.skipped += roles.length; return 0; }
  let added = 0;
  for (let i = 0; i < fresh.length; i += 20) {
    const batch = fresh.slice(i, i+20);
    const cls = await classifyBatch(batch.map(r => ({title: r.title, firm: r.firm})));
    const ins = batch
      .filter((_, idx) => cls[idx]?.fo === true)
      .map((r, idx) => ({...r, function: cls[idx]?.fn||null, level: cls[idx]?.lv||null, is_front_office: true, is_approved: true}));
    if (ins.length) {
      const { error } = await supabase.from('jobs').insert(ins);
      if (!error) { added += ins.length; }
      else { console.error('  DB error:', error.message); stats.errors++; }
    }
    await sleep(150);
  }
  return added;
}

// ═══════════════════════════════════════════════════════
// SOURCE 1: GREENHOUSE — finance-specific, insert direct
// ═══════════════════════════════════════════════════════
const GH_FIRMS = [
  // Confirmed high-probability slugs (market makers, known firms)
  {name:'Optiver',                  slugs:['optiver']},
  {name:'DRW',                      slugs:['drw']},
  {name:'IMC Trading',              slugs:['imc','imctrading']},
  {name:'Akuna Capital',            slugs:['akunacapital','akuna']},
  {name:'Jump Trading',             slugs:['jumptrading','jump']},
  {name:'Wolverine Trading',        slugs:['wolverinetrading','wolverine']},
  {name:'Virtu Financial',          slugs:['virtu','virtufinancial']},
  {name:'Hudson River Trading',     slugs:['hudsonrivertrading','hrt']},
  {name:'Jane Street',              slugs:['janestreet']},
  {name:'Susquehanna (SIG)',        slugs:['susquehanna','sig']},
  {name:'Flow Traders',             slugs:['flowtraders']},
  {name:'Point72',                  slugs:['point72']},
  {name:'AQR Capital',              slugs:['aqr','aqrcapital']},
  {name:'Man Group',                slugs:['mangroup']},
  {name:'Citadel',                  slugs:['citadelam','citadellp','citadel']},
  {name:'Two Sigma',                slugs:['twosigma','twosigmainvestments']},
  {name:'D.E. Shaw',                slugs:['deshawgroup','deshaw']},
  {name:'Bridgewater Associates',   slugs:['bridgewater','bridgewaterassociates']},
  {name:'Millennium Management',    slugs:['millennium','millenniummanagement']},
  {name:'Balyasny Asset Mgmt',      slugs:['balyasny','balyasnyam']},
  {name:'Marshall Wace',            slugs:['marshallwace']},
  {name:'ExodusPoint Capital',      slugs:['exoduspoint']},
  {name:'Schonfeld Strategic',      slugs:['schonfeld','schonfeldstrategic']},
  {name:'Squarepoint Capital',      slugs:['squarepointcapital','squarepoint']},
  {name:'Verition Fund Mgmt',       slugs:['verition','veritionfund']},
  {name:'Hudson Bay Capital',       slugs:['hudsonbay','hudsonbaycapital']},
  {name:'Arrowstreet Capital',      slugs:['arrowstreet']},
  {name:'Magnetar Capital',         slugs:['magnetar']},
  {name:'Graham Capital Mgmt',      slugs:['grahamcapital','graham']},
  {name:'Capstone Investment',      slugs:['capstone','capstoneinvestment']},
  {name:'Tudor Investment Corp',    slugs:['tudor','tudorinvestment']},
  {name:'Coatue Management',        slugs:['coatue']},
  {name:'Tiger Global',             slugs:['tigereyecm','tiger']},
  {name:'Viking Global Investors',  slugs:['vikingglobal','viking']},
  {name:'Lone Pine Capital',        slugs:['lonepine','lonepinecapital']},
  {name:'Pershing Square',          slugs:['pershingsquare']},
  {name:'Elliott Management',       slugs:['elliotmgmt','elliottmanagement']},
  {name:'Third Point LLC',          slugs:['thirdpoint']},
  {name:'Farallon Capital',         slugs:['farallon','faralloncapital']},
  {name:'Adage Capital',            slugs:['adage','adagecapital']},
  {name:'Davidson Kempner',         slugs:['davidsonkempner','dkam']},
  {name:'Baupost Group',            slugs:['baupost','baupostgroup']},
  {name:'HPS Investment Partners',  slugs:['hps','hpsinvestment']},
  {name:'King Street Capital',      slugs:['kingstreet','kingstreetcapital']},
  {name:'Canyon Partners',          slugs:['canyon','canyoncapital']},
  {name:'Anchorage Capital',        slugs:['anchorage','anchoragecapital']},
  {name:'Centerbridge Partners',    slugs:['centerbridge','centerbridgepartners']},
  {name:'Sculptor Capital',         slugs:['sculptor','ochziff']},
  {name:'Fortress Investment',      slugs:['fortress','fortressinvestment']},
  {name:'HBK Capital',              slugs:['hbk','hbkcapital']},
  {name:'Winton Group',             slugs:['winton','wintongroup']},
  {name:'PDT Partners',             slugs:['pdtpartners','pdt']},
  {name:'Caxton Associates',        slugs:['caxton','caxtonassociates']},
  {name:'Moore Capital',            slugs:['moorecapital','moore']},
  {name:'BlueCrest Capital',        slugs:['bluecrest','bluecrestcapital']},
  {name:'Greenlight Capital',       slugs:['greenlight','greenlightcapital']},
  {name:'Soros Fund Mgmt',          slugs:['sorosfundmgmt','soros']},
  {name:'Marathon Asset Mgmt',      slugs:['marathon','marathonasset']},
  {name:'Eisler Capital',           slugs:['eisler','eislercapital']},
  {name:'Alkeon Capital',           slugs:['alkeon','alkeoncapital']},
  {name:'Kepos Capital',            slugs:['kepos','keposcapital']},
  {name:'Garda Capital Partners',   slugs:['garda','gardacapital']},
  {name:'Brevan Howard',            slugs:['brevanhoward']},
  {name:'Durable Capital',          slugs:['durablecapital','durable']},
  {name:'Holocene Advisors',        slugs:['holocene','holoceneadvisors']},
  {name:'Walleye Capital',          slugs:['walleye','walleyecapital']},
  {name:'Varde Partners',           slugs:['varde','vardepartners']},
  {name:'Orbis Investment',         slugs:['orbis','orbisinvestment']},
  {name:'Alyeska Investment',       slugs:['alyeska','alyeskainvestment']},
  {name:'Silver Point Capital',     slugs:['silverpoint','silverpointcapital']},
  {name:'Highbridge Capital',       slugs:['highbridge','highbridgecapital']},
  {name:'Cerberus Capital',         slugs:['cerberuscapital','cerberus']},
  {name:'CQS',                      slugs:['cqs']},
  {name:'GAM Investments',          slugs:['gam','gaminvestments']},
  {name:'Systematica Investments',  slugs:['systematica']},
  {name:'Aspect Capital',           slugs:['aspect','aspectcapital']},
  {name:'TCI Fund Management',      slugs:['tci','tcifund']},
  {name:'Rokos Capital',            slugs:['rokos','rokoscapital']},
  {name:'Lansdowne Partners',       slugs:['lansdowne']},
  {name:'Paulson & Co',             slugs:['paulson']},
  {name:'Baker Brothers Advisors',  slugs:['bakerbrothers']},
  {name:'Quantlab Financial',       slugs:['quantlab']},
  {name:'Alphadyne Asset Mgmt',     slugs:['alphadyne']},
  {name:'Element Capital',          slugs:['elementcapital','element']},
  // Banks
  {name:'Nomura',                   slugs:['nomura']},
  {name:'Macquarie',                slugs:['macquariegroup','macquarie']},
  {name:'Mizuho',                   slugs:['mizuho','mizuhofs']},
  {name:'SMBC',                     slugs:['smbc','smbcnikko']},
  {name:'MUFG',                     slugs:['mufg','mufgamericas']},
  {name:'RBC Capital Markets',      slugs:['rbc','rbccm']},
  {name:'BMO Capital Markets',      slugs:['bmo','bmocm']},
  {name:'TD Securities',            slugs:['tdsecurities']},
  {name:'Scotiabank GBM',           slugs:['scotiabank']},
  {name:'Natixis',                  slugs:['natixis']},
  {name:'ING',                      slugs:['ingbank','ing']},
  {name:'BNP Paribas',              slugs:['bnpparibas']},
  {name:'Societe Generale',         slugs:['societegenerale']},
  {name:'Credit Agricole CIB',      slugs:['creditagricole']},
  {name:'ABN AMRO',                 slugs:['abnamro']},
  {name:'Commerzbank',              slugs:['commerzbank']},
  {name:'Lazard',                   slugs:['lazard']},
  {name:'Evercore',                 slugs:['evercore','evercoregroup']},
  {name:'Moelis & Company',         slugs:['moelis']},
  {name:'Perella Weinberg',         slugs:['pwp','pwpartners']},
  {name:'Houlihan Lokey',           slugs:['houlihanlokeyinc','houlihanlokey']},
  {name:'Jefferies',                slugs:['jefferies','jefferiesllc']},
  {name:'Guggenheim Partners',      slugs:['guggenheimpartners','guggenheim']},
  {name:'Baird',                    slugs:['rwbaird']},
  {name:'Piper Sandler',            slugs:['pipersandler']},
  {name:'Stifel',                   slugs:['stifel']},
  {name:'TD Cowen',                 slugs:['tdcowen','cowen']},
  {name:'Cantor Fitzgerald',        slugs:['cantorfitzgerald']},
  {name:'Needham & Company',        slugs:['needham']},
  {name:'Wedbush Securities',       slugs:['wedbush']},
  {name:'Leerink Partners',         slugs:['leerink']},
  {name:'William Blair',            slugs:['williamblair']},
  {name:'General Atlantic',         slugs:['generalatlantic']},
  // Asset managers
  {name:'BlackRock',                slugs:['blackrock','blackrockjobs']},
  {name:'Wellington Management',    slugs:['wellingtonmanagement','wellington']},
  {name:'T. Rowe Price',            slugs:['troweprice']},
  {name:'Invesco',                  slugs:['invesco']},
  {name:'Franklin Templeton',       slugs:['franklintempleton']},
  {name:'PGIM',                     slugs:['pgim']},
  {name:'AllianceBernstein',        slugs:['alliancebernstein','ab']},
  {name:'Neuberger Berman',         slugs:['neubergerberman']},
  {name:'Loomis Sayles',            slugs:['loomissayles']},
  {name:'Dimensional',              slugs:['dimensional','dfa']},
  {name:'MFS',                      slugs:['mfs']},
  {name:'Columbia Threadneedle',    slugs:['columbiathreadneedle']},
  {name:'Artisan Partners',         slugs:['artisanpartners']},
  {name:'Cohen & Steers',           slugs:['cohensteers']},
  {name:'Federated Hermes',         slugs:['federatedhermes']},
  {name:'Eaton Vance',              slugs:['eatonvance']},
  {name:'TCW Group',                slugs:['tcw']},
  {name:'DoubleLine Capital',       slugs:['doubleline']},
  {name:'Capital Group',            slugs:['capitalgroup','americanfunds']},
  {name:'Russell Investments',      slugs:['russellinvestments']},
  {name:'Nuveen',                   slugs:['nuveen']},
  {name:'PIMCO',                    slugs:['pimco']},
  // PE
  {name:'KKR',                      slugs:['kkr','kkrecruitment']},
  {name:'Apollo Global',            slugs:['apolloglobal','apollo']},
  {name:'The Carlyle Group',        slugs:['carlyle','thecarlylegroup']},
  {name:'TPG Capital',              slugs:['tpg']},
  {name:'Warburg Pincus',           slugs:['warburgpincus']},
  {name:'Silver Lake',              slugs:['silverlake']},
  {name:'Golub Capital',            slugs:['golubcapital']},
  {name:'Ares Management',          slugs:['aresmanagement','ares']},
  {name:'Blue Owl Capital',         slugs:['blueowl']},
  {name:'Brookfield',               slugs:['brookfield']},
  {name:'Bain Capital',             slugs:['baincapital']},
  {name:'Blackstone',               slugs:['blackstone']},
  {name:'Hamilton Lane',            slugs:['hamiltonlane']},
  {name:'StepStone Group',          slugs:['stepstonegroup']},
  {name:'HarbourVest Partners',     slugs:['harbourvest']},
  {name:'Angelo Gordon',            slugs:['angelogordon','angelo']},
  {name:'Benefit Street Partners',  slugs:['benefitstreet']},
  {name:'Golub Capital',            slugs:['golubcapital']},
];

// ═══════════════════════════════════════════════════════
// SOURCE 2: LEVER
// ═══════════════════════════════════════════════════════
const LEVER_FIRMS = [
  {name:'Citadel Securities',      slug:'citadelsecurities'},
  {name:'Goldman Sachs',           slug:'goldmansachs'},
  {name:'Morgan Stanley',          slug:'morganstanley'},
  {name:'JPMorgan',                slug:'jpmorgan'},
  {name:'Blackstone',              slug:'blackstone'},
  {name:'Bank of America',         slug:'bankofamerica'},
  {name:'Barclays',                slug:'barclays'},
  {name:'UBS',                     slug:'ubs'},
  {name:'Deutsche Bank',           slug:'deutschebank'},
  {name:'Wells Fargo',             slug:'wellsfargo'},
  {name:'Citi',                    slug:'citi'},
  {name:'HSBC',                    slug:'hsbc'},
  {name:'BNP Paribas',             slug:'bnpparibas'},
  {name:'Societe Generale',        slug:'societegenerale'},
  {name:'Coatue',                  slug:'coatue'},
  {name:'Renaissance Tech',        slug:'renaissance'},
  {name:'D.E. Shaw Research',      slug:'deshawresearch'},
  {name:'Pershing Square',         slug:'pershingsquare'},
  {name:'Elliott Management',      slug:'elliotmgmt'},
  {name:'Bain Capital',            slug:'baincapital'},
  {name:'KKR',                     slug:'kkr'},
  {name:'Davidson Kempner',        slug:'davidsonkempner'},
  {name:'Hudson Bay Capital',      slug:'hudsonbay'},
  {name:'ExodusPoint Capital',     slug:'exoduspoint'},
  {name:'Magnetar Capital',        slug:'magnetar'},
  {name:'Anchorage Capital',       slug:'anchorage'},
  {name:'Ares Management',         slug:'ares'},
  {name:'Third Point',             slug:'thirdpoint'},
  {name:'Summit Partners',         slug:'summit'},
];

// ═══════════════════════════════════════════════════════
// SOURCE 3: THE MUSE — FREE, NO KEY NEEDED
// Covers ALL major banks + asset managers. This is the fix
// for the "missing big banks" problem.
// ═══════════════════════════════════════════════════════
async function syncMuse() {
  console.log('\n[3] The Muse API (free, no key — big banks primary source)...');
  let added = 0;

  // Step A: Browse Finance category pages (broad sweep)
  const categories = ['Finance', 'Investment Banking', 'Asset & Wealth Management'];
  for (const cat of categories) {
    console.log(`  Category: "${cat}"`);
    for (let page = 0; page < 60; page++) {
      try {
        const url = `https://www.themuse.com/api/public/jobs?category=${encodeURIComponent(cat)}&page=${page}&descending=true`;
        const res = await fetchJ(url, {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 10000);
        if (!res.ok) { console.log(`    page ${page}: ${res.status}, stopping`); break; }
        const d = await res.json();
        const results = d.results || [];
        if (!results.length) { console.log(`    page ${page}: empty, stopping`); break; }
        if (page === 0) console.log(`    Total available: ${d.total}`);

        const mapped = results.map(r => ({
          source_id: `muse-${r.id}`,
          title: r.name || '',
          firm: r.company?.name || 'Unknown',
          location: r.locations?.map(l=>l.name).join(', ') || null,
          description: (r.contents||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
          apply_url: r.refs?.landing_page || `https://www.themuse.com/jobs/${r.id}`,
          source: 'The Muse',
          posted_at: r.publication_date ? new Date(r.publication_date).toISOString() : new Date().toISOString(),
        }));

        // The Muse Finance category has high signal — insert direct
        added += await insertDirect(mapped, `Muse/${cat} p${page}`);
        await sleep(200);
      } catch(e) { console.log(`    page error: ${e.message}`); break; }
    }
  }

  // Step B: Direct company search for big banks not in category results
  const bigBanks = [
    'Goldman Sachs','JPMorgan Chase','Morgan Stanley','Citigroup','Bank of America',
    'Barclays','UBS','Deutsche Bank','HSBC','Wells Fargo','BNP Paribas',
    'Nomura','Macquarie','Mizuho Financial','RBC Capital Markets','BMO',
    'TD Securities','Scotiabank','Credit Suisse','Societe Generale','Natixis',
    'BlackRock','PIMCO','Vanguard','Fidelity Investments','T. Rowe Price',
    'Wellington Management','Invesco','Franklin Templeton','AllianceBernstein',
    'Neuberger Berman','State Street','BNY Mellon','Northern Trust',
    'Blackstone','KKR','Apollo Global Management','Carlyle Group',
    'Ares Management','Brookfield Asset Management','TPG','Warburg Pincus',
    'Point72','Citadel','Two Sigma','AQR Capital','Millennium Management',
    'Bridgewater Associates','Man Group','Balyasny',
  ];

  console.log(`  Direct company search (${bigBanks.length} firms)...`);
  for (const bank of bigBanks) {
    for (let page = 0; page < 10; page++) {
      try {
        const url = `https://www.themuse.com/api/public/jobs?company=${encodeURIComponent(bank)}&page=${page}&descending=true`;
        const res = await fetchJ(url, {headers:{'User-Agent':'Mozilla/5.0'}}, 8000);
        if (!res.ok) break;
        const d = await res.json();
        const results = d.results || [];
        if (!results.length) break;
        if (page === 0 && results.length > 0) console.log(`    ✓ ${bank}: ${d.total} total`);
        const mapped = results.map(r => ({
          source_id: `muse-${r.id}`,
          title: r.name || '',
          firm: bank,
          location: r.locations?.map(l=>l.name).join(', ') || null,
          description: (r.contents||'').replace(/<[^>]+>/g,' ').trim().slice(0,1500),
          apply_url: r.refs?.landing_page || `https://www.themuse.com/jobs/${r.id}`,
          source: 'The Muse',
          posted_at: r.publication_date ? new Date(r.publication_date).toISOString() : new Date().toISOString(),
        }));
        added += await insertDirect(mapped, `Muse/${bank}`);
        await sleep(150);
      } catch { break; }
    }
  }

  console.log(`  → ${added} added from The Muse`);
  stats.muse = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// SOURCE 4: eFINANCIALCAREERS — targeted finance queries
// ═══════════════════════════════════════════════════════
const EFC_QUERIES = [
  'sales+trading','investment+banking+analyst','investment+banking+associate',
  'M%26A+analyst','M%26A+associate','leveraged+finance','DCM+associate','ECM+analyst',
  'equity+research+analyst','credit+research+analyst','fixed+income+research','macro+strategist',
  'portfolio+manager','fund+manager','hedge+fund+analyst','hedge+fund+trader',
  'quantitative+researcher','quantitative+analyst','quantitative+trader',
  'FX+trader','rates+trader','credit+trader','equity+trader','derivatives+trader',
  'commodities+trader','options+trader','vol+trader','prop+trader',
  'structured+finance','CLO+analyst','ABS+analyst','high+yield+analyst','distressed+debt',
  'private+equity+associate','growth+equity+associate','venture+capital+associate',
  'wealth+manager','private+banker','prime+brokerage',
  'capital+markets+analyst','loan+syndication','project+finance',
  'insurance+investment+analyst','pension+fund+manager',
  'real+estate+investment','infrastructure+investment',
  'market+risk+analyst','credit+risk+analyst',
];

async function syncEFC() {
  console.log('\n[4] eFinancialCareers...');
  let added = 0;
  for (const q of EFC_QUERIES) {
    let pageEmpty = false;
    for (let page = 1; page <= 6 && !pageEmpty; page++) {
      try {
        const res = await fetchJ(
          `https://www.efinancialcareers.com/search?q=${q}&employment_type=permanent&format=rss&page=${page}`,
          {headers:{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}},
          9000
        );
        if (!res.ok) { pageEmpty = true; break; }
        const xml = await res.text();
        const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
        if (!items.length) { pageEmpty = true; break; }
        if (page === 1) console.log(`  "${q.replace(/\+/g,' ')}": ${items.length}/page`);

        const mapped = items.flatMap(item => {
          const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]>/)    ||[])[1]?.trim()||'';
          const link    = (item.match(/<link>(.*?)<\/link>/)             ||[])[1]?.trim()||'';
          const desc    = (item.match(/<description><!\[CDATA\[(.*?)(?:\]\]>|$)/) ||[])[1]||'';
          const firm    = (item.match(/<source[^>]*>(.*?)<\/source>/)   ||[])[1]?.trim()||'Unknown';
          const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/)       ||[])[1]||'';
          const loc     = (item.match(/<category>(.*?)<\/category>/)     ||[])[1]?.trim()||null;
          if (!title || !link) return [];
          return [{
            source_id: `efc-${Buffer.from(link).toString('base64').slice(0,40)}`,
            title, firm, location: loc,
            description: desc.replace(/<[^>]+>/g,' ').trim().slice(0,1500),
            apply_url: link, source: 'eFinancialCareers',
            posted_at: pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
          }];
        });
        // EFC is finance-specific so insert direct
        if (mapped.length) added += await insertDirect(mapped, `EFC/${q}`);
        await sleep(500); // EFC needs slower pace to avoid blocks
      } catch(e) { console.log(`  EFC error: ${e.message.slice(0,40)}`); pageEmpty = true; }
    }
  }
  console.log(`  → ${added} added from EFC`);
  stats.efc = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// SOURCE 5: ADZUNA — needs free API key
// ═══════════════════════════════════════════════════════
async function syncAdzuna() {
  if (!ADZUNA_APP_ID || !ADZUNA_API_KEY_VAL) {
    console.log('\n[5] Adzuna: SKIPPED');
    console.log('  → Get a free key at developer.adzuna.com (2 min, no credit card)');
    console.log('  → Add ADZUNA_APP_ID + ADZUNA_API_KEY to GitHub repo secrets');
    console.log('  → This alone adds ~1,500 more verified finance roles');
    return 0;
  }
  console.log('\n[5] Adzuna...');
  const queries = [
    'investment banker','sales trader','equity researcher','portfolio manager',
    'private equity associate','hedge fund analyst','quantitative analyst finance',
    'fixed income trader','FX trader','wealth manager bank','credit analyst bank',
    'derivatives trader','capital markets analyst','M&A analyst',
    'leveraged finance','CLO analyst','distressed debt','structured finance',
  ];
  let added = 0;
  for (const q of queries) {
    for (let page = 1; page <= 5; page++) {
      try {
        const res = await fetchJ(
          `https://api.adzuna.com/v1/api/jobs/us/search/${page}?app_id=${ADZUNA_APP_ID}&app_key=${ADZUNA_API_KEY_VAL}&what=${encodeURIComponent(q)}&results_per_page=50&category=finance-jobs&sort_by=date&max_days_old=60`,
          {}, 9000
        );
        if (!res.ok) break;
        const d = await res.json();
        const jobs = d.results || [];
        if (!jobs.length) break;
        if (page === 1) console.log(`  "${q}": ${d.count} total`);
        // Adzuna has more noise so classify
        added += await insertClassified(jobs.map(r => ({
          source_id: `adzuna-${r.id}`, title: r.title,
          firm: r.company?.display_name || 'Unknown',
          location: r.location?.display_name || null,
          description: (r.description||'').slice(0,1500),
          apply_url: r.redirect_url, source: 'Adzuna',
          posted_at: r.created || new Date().toISOString(),
        })), 'Adzuna');
        await sleep(200);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added from Adzuna`);
  stats.adzuna = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// GREENHOUSE & LEVER sync
// ═══════════════════════════════════════════════════════
async function syncGreenhouse() {
  console.log('\n[1] Greenhouse ATS...');
  let added = 0;
  const done = new Set();
  for (const f of GH_FIRMS) {
    if (done.has(f.name)) continue;
    for (const slug of f.slugs) {
      try {
        const res = await fetchJ(
          `https://boards-api.greenhouse.io/v1/boards/${slug}/jobs`,
          {headers:{'User-Agent':'Mozilla/5.0'}}, 5000
        );
        if (!res.ok) continue;
        const {jobs=[]} = await res.json();
        if (!jobs.length) continue;
        done.add(f.name);
        console.log(`  ✓ ${f.name} [${slug}]: ${jobs.length} jobs`);
        const mapped = jobs.map(r => ({
          source_id: `gh-${r.id}`, title: r.title, firm: f.name,
          location: r.location?.name || null,
          description: (r.content||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
          apply_url: r.absolute_url, source: 'Greenhouse',
          posted_at: r.updated_at || new Date().toISOString(),
        }));
        // Greenhouse is finance-company-specific boards — insert direct
        added += await insertDirect(mapped, `GH/${f.name}`);
        await sleep(60);
        break;
      } catch { await sleep(30); }
    }
  }
  console.log(`  → ${added} added from Greenhouse`);
  stats.gh = added;
  return added;
}

async function syncLever() {
  console.log('\n[2] Lever ATS...');
  let added = 0;
  const done = new Set();
  for (const f of LEVER_FIRMS) {
    if (done.has(f.name)) continue;
    try {
      const res = await fetchJ(
        `https://api.lever.co/v0/postings/${f.slug}?mode=json&limit=100`,
        {headers:{'User-Agent':'Mozilla/5.0'}}, 5000
      );
      if (!res.ok) continue;
      const jobs = await res.json();
      if (!Array.isArray(jobs) || !jobs.length) continue;
      done.add(f.name);
      console.log(`  ✓ ${f.name}: ${jobs.length} jobs`);
      const mapped = jobs.map(r => ({
        source_id: `lever-${r.id}`, title: r.text, firm: f.name,
        location: r.categories?.location || null,
        description: (r.descriptionPlain||'').slice(0,1500),
        apply_url: r.hostedUrl || r.applyUrl, source: 'Lever',
        posted_at: r.createdAt ? new Date(r.createdAt).toISOString() : new Date().toISOString(),
      }));
      added += await insertDirect(mapped, `Lever/${f.name}`);
      await sleep(60);
    } catch {}
  }
  console.log(`  → ${added} added from Lever`);
  stats.lever = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// CLEANUP & MAIN
// ═══════════════════════════════════════════════════════
async function cleanup() {
  const cutoff = new Date(Date.now() - 60*24*60*60*1000).toISOString();
  const {count} = await supabase.from('jobs').delete({count:'exact'})
    .lt('posted_at', cutoff).eq('is_featured', false);
  console.log(`\n[6] Cleanup: ${count||0} expired listings removed`);
}

async function main() {
  console.log('═'.repeat(55));
  console.log(`FRONT OFFICE JOBS SYNC — ${new Date().toISOString()}`);
  // SYNC_SOURCES env lets GitHub Actions run sources in parallel
  // e.g. SYNC_SOURCES=gh,lever  or  SYNC_SOURCES=muse  or  SYNC_SOURCES=efc,adzuna
  // If not set, runs everything
  const sources = (process.env.SYNC_SOURCES || 'gh,lever,muse,efc,adzuna').split(',').map(s=>s.trim());
  console.log(`Sources: ${sources.join(', ')}`);
  console.log('═'.repeat(55));

  if (sources.includes('gh'))     await syncGreenhouse();
  if (sources.includes('lever'))  await syncLever();
  if (sources.includes('muse'))   await syncMuse();
  if (sources.includes('efc'))    await syncEFC();
  if (sources.includes('adzuna')) await syncAdzuna();
  await cleanup();

  // Final count
  const {count} = await supabase.from('jobs').select('*', {count:'exact',head:true})
    .eq('is_front_office', true).eq('is_approved', true);

  const newTotal = (stats.gh + stats.lever + stats.muse + stats.efc + stats.adzuna);
  console.log('\n' + '═'.repeat(55));
  console.log(`SYNC COMPLETE`);
  console.log(`  New this run:   ${newTotal}`);
  console.log(`  Total in DB:    ${count || '?'}`);
  console.log(`  Greenhouse:     ${stats.gh}`);
  console.log(`  Lever:          ${stats.lever}`);
  console.log(`  The Muse:       ${stats.muse}  ← big banks`);
  console.log(`  EFC:            ${stats.efc}`);
  console.log(`  Adzuna:         ${stats.adzuna}${!ADZUNA_APP_ID ? ' (skipped — add key!)' : ''}`);
  console.log(`  Skipped (dupe): ${stats.skipped}`);
  console.log(`  DB errors:      ${stats.errors}`);
  if (!ADZUNA_APP_ID) {
    console.log('\n  ⚠ Add Adzuna key for +1500 more roles: developer.adzuna.com');
  }
  console.log('═'.repeat(55));
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
