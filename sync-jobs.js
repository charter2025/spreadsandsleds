import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;
const ADZUNA_APP_ID        = process.env.ADZUNA_APP_ID || '';
// Try both naming conventions for the Adzuna key
const ADZUNA_API_KEY_VAL   = process.env.ADZUNA_API_KEY || process.env.ADZUNA_APP_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));
const stats    = { gh:0, lever:0, muse:0, efc:0, adzuna:0, skipped:0, errors:0 };

async function fetchJ(url, opts={}, ms=12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, {...opts, signal: ctrl.signal});
    clearTimeout(t); return r;
  } catch(e) { clearTimeout(t); throw e; }
}

// ─────────────────────────────────────────────────────────
// DEDUP — only check source_id, upsert on conflict
// KEY FIX: use upsert so updated jobs replace stale ones
// ─────────────────────────────────────────────────────────
async function upsertBatch(roles) {
  if (!roles.length) return 0;
  let added = 0;
  for (let i = 0; i < roles.length; i += 100) {
    const batch = roles.slice(i, i+100);
    const { error, count } = await supabase
      .from('jobs')
      .upsert(batch, { onConflict: 'source_id', ignoreDuplicates: true })
      .select('source_id');
    if (!error) {
      added += batch.length;
    } else {
      // If upsert not supported, fall back to insert with dedup
      const ids = batch.map(r => r.source_id);
      const { data: ex } = await supabase.from('jobs').select('source_id').in('source_id', ids);
      const seen = new Set((ex||[]).map(e => e.source_id));
      const fresh = batch.filter(r => !seen.has(r.source_id));
      if (fresh.length) {
        const { error: e2 } = await supabase.from('jobs').insert(fresh);
        if (!e2) added += fresh.length;
        else { console.error('  DB error:', e2.message); stats.errors++; }
      } else {
        stats.skipped += batch.length;
      }
    }
  }
  return added;
}

function inferFunctionLevel(title) {
  const t = (title||'').toLowerCase();
  let fn = null, lv = null;
  if (/\b(trad(er|ing)|sales.*(trad|structur)|market.mak|execution)\b/.test(t)) fn = 'S&T';
  else if (/\b(m&a|mergers|acqui|ecm|dcm|leveraged.fin|capital.market|investment.bank|coverage|syndic|lbo)\b/.test(t)) fn = 'IBD';
  else if (/\b(research|strategist|economist)\b/.test(t)) fn = 'RM';
  else if (/\b(portfolio.manag|fund.manag|asset.manag|investment.manag|allocat)\b/.test(t)) fn = 'AM';
  else if (/\b(private.equity|growth.equity|venture.cap|buyout)\b/.test(t)) fn = 'PE';
  else if (/\b(wealth.manag|private.bank|private.client|family.office)\b/.test(t)) fn = 'PB';
  else if (/\b(quant(itative)?.*(research|strat|invest|alpha)|systematic)\b/.test(t)) fn = 'QR';
  if (/\b(managing.dir|md\b|head.of|chief|cio|ceo)\b/.test(t)) lv = 'MD';
  else if (/\b(partner|principal)\b/.test(t)) lv = 'Partner';
  else if (/\b(director)\b/.test(t)) lv = 'Director';
  else if (/\b(vice.pres|vp\b)\b/.test(t)) lv = 'VP';
  else if (/\b(associate)\b/.test(t)) lv = 'Associate';
  else if (/\b(analyst|intern)\b/.test(t)) lv = 'Analyst';
  return { function: fn, level: lv };
}

async function insertDirect(roles, source) {
  if (!roles.length) return 0;
  const batch = roles.map(r => ({
    ...r,
    ...inferFunctionLevel(r.title),
    is_front_office: true,
    is_approved: true,
  }));
  const added = await upsertBatch(batch);
  if (added > 0) console.log(`    Inserted ${added} from ${source}`);
  return added;
}

async function classifyBatch(roles) {
  const list = roles.map((r,i) => `${i}: "${r.title}" at ${r.firm}`).join('\n');
  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 1200,
      messages: [{ role: 'user', content:
`Be STRICT. When in doubt, fo:false.

FRONT OFFICE (fo:true) ONLY — roles that make investment decisions or generate revenue:
Trader, Sales Trader, Market Maker, Prop Trader, Vol Trader, Flow Trader
Institutional Sales, Coverage Banker, Derivatives Sales, FX Sales
M&A, DCM, ECM, LevFin, Restructuring, Sponsor Coverage, Industry Coverage
Equity Research, Credit Research, Macro Strategist, Fixed Income Research, Rates Strategist
Portfolio Manager, Fund Manager, Investment Manager, CIO, Allocator, Investment Analyst
PE Associate/Analyst, Growth Equity, VC (investment roles only)
Quantitative Researcher, Quant Strategist, Alpha Researcher, Systematic Trader
Private Banker, Wealth Advisor, Family Office Investment, Relationship Manager (investments)
CLO/ABS/MBS/Structured Finance Analyst, High Yield, Distressed Debt, Direct Lending

ALWAYS fo:false — no exceptions:
- Tech/Engineering: software engineer, developer, SWE, DevOps, SRE, IT, quant developer/engineer, product manager, cybersecurity, data engineer, ML engineer, infrastructure
- Operations/Middle office: trade support, settlements, reconciliation, fund accounting, collateral, custody, KYC, AML, onboarding, treasury ops, trade processing
- Risk (non-trading): operational risk, model risk, model validation, regulatory risk, enterprise risk, internal audit, SOX
- Compliance, legal, paralegal, attorney, counsel
- HR, recruiting, talent acquisition, people operations
- Finance/accounting, FP&A, controller, tax, audit
- Marketing, communications, PR, events, brand
- Executive assistant, admin assistant, office manager, receptionist, facilities, administrative
- Project manager, program manager, business analyst (non-investment), data analyst, BI, reporting
- Investor relations (corporate IR), corporate strategy, chief of staff
- Vague titles alone: just "Analyst", "Associate", "VP", "Manager", "Specialist", "Consultant"

One JSON per line, no markdown:
{"i":0,"fo":true,"fn":"S&T","lv":"Analyst"}
fn: S&T|IBD|AM|PE|RM|PB|QR|null  lv: Analyst|Associate|VP|Director|MD|Partner|null
${list}` }]
    });
    const out = {};
    for (const line of res.content[0].text.trim().split('\n')) {
      try { const o = JSON.parse(line.trim()); if (typeof o.i === 'number') out[o.i] = o; } catch {}
    }
    return out;
  } catch(e) { console.warn('  Classifier err:', e.message); return {}; }
}

async function insertClassified(roles, source) {
  if (!roles.length) return 0;
  // Dedup first before classifying (saves API calls)
  const ids = roles.map(r => r.source_id);
  const existing = new Set();
  for (let i = 0; i < ids.length; i += 500) {
    const { data } = await supabase.from('jobs').select('source_id').in('source_id', ids.slice(i,i+500));
    (data||[]).forEach(r => existing.add(r.source_id));
  }
  const fresh = roles.filter(r => !existing.has(r.source_id));
  if (!fresh.length) { stats.skipped += roles.length; return 0; }

  let added = 0;
  for (let i = 0; i < fresh.length; i += 20) {
    const batch = fresh.slice(i, i+20);
    const cls = await classifyBatch(batch.map(r => ({title:r.title, firm:r.firm})));
    const ins = batch
      .filter((_,idx) => cls[idx]?.fo === true)
      .map((r,idx) => ({...r, function:cls[idx]?.fn||null, level:cls[idx]?.lv||null, is_front_office:true, is_approved:true}));
    if (ins.length) {
      const { error } = await supabase.from('jobs').insert(ins);
      if (!error) added += ins.length;
      else { console.error('  DB err:', error.message); stats.errors++; }
    }
    await sleep(150);
  }
  if (added > 0) console.log(`    Classified+inserted ${added} from ${source}`);
  return added;
}

// ═══════════════════════════════════════════════════════
// GREENHOUSE
// ═══════════════════════════════════════════════════════
const GH_FIRMS = [
  // ── CONFIRMED WORKING (verified by discover-firms.js) ──
  // Hedge Funds / Market Makers
  {name:'Man Group',               slugs:['mangroup']},
  {name:'Point72',                 slugs:['point72']},
  {name:'AQR Capital',             slugs:['aqr']},
  {name:'Schonfeld Strategic',     slugs:['schonfeld']},
  {name:'ExodusPoint Capital',     slugs:['exoduspoint']},
  {name:'Squarepoint Capital',     slugs:['squarepointcapital']},
  {name:'Magnetar Capital',        slugs:['magnetar']},
  {name:'Winton Group',            slugs:['winton']},
  {name:'PDT Partners',            slugs:['pdtpartners']},
  {name:'Lone Pine Capital',       slugs:['lonepinecapital']},
  {name:'Durable Capital',         slugs:['durable']},
  {name:'IMC Trading',             slugs:['imc']},
  {name:'Akuna Capital',           slugs:['akunacapital']},
  {name:'Jump Trading',            slugs:['jumptrading']},
  {name:'Virtu Financial',         slugs:['virtu']},
  {name:'Jane Street',             slugs:['janestreet']},
  {name:'Flow Traders',            slugs:['flowtraders']},
  {name:'Geneva Trading',          slugs:['genevatrading']},
  // PE / Credit
  {name:'Apollo Global',           slugs:['apollo']},
  {name:'General Atlantic',        slugs:['generalatlantic']},
  {name:'Vector Capital',          slugs:['vectorcapital']},
  {name:'Pantheon Ventures',       slugs:['pantheon']},
  {name:'Sixth Street Partners',   slugs:['sixthstreet']},
  // Asset Managers
  {name:'Cohen & Steers',          slugs:['cohensteers']},
  {name:'Artisan Partners',        slugs:['artisanpartners']},
  {name:'William Blair',           slugs:['williamblair']},
  // VC (investment roles only)
  {name:'Andreessen Horowitz',     slugs:['a16z']},
  {name:'General Catalyst',        slugs:['generalcatalyst']},
  {name:'Flourish Ventures',       slugs:['flourish']},
];

// ═══════════════════════════════════════════════════════
// LEVER
// ═══════════════════════════════════════════════════════
const LEVER_FIRMS = [
  {name:'Anchorage Capital',       slug:'anchorage'},
  // Banks confirmed from previous runs
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
  {name:'Davidson Kempner',        slug:'davidsonkempner'},
  {name:'Hudson Bay Capital',      slug:'hudsonbay'},
  {name:'ExodusPoint Capital',     slug:'exoduspoint'},
  {name:'Magnetar Capital',        slug:'magnetar'},
  {name:'Ares Management',         slug:'ares'},
  {name:'Third Point',             slug:'thirdpoint'},
  {name:'Summit Partners',         slug:'summit'},
  {name:'Two Sigma',               slug:'twosigma'},
  {name:'Citadel',                 slug:'citadel'},
  {name:'Millennium Management',   slug:'millennium'},
  {name:'Balyasny',                slug:'balyasny'},
  {name:'Schonfeld',               slug:'schonfeld'},
  {name:'Bridgewater',             slug:'bridgewater'},
  {name:'KKR',                     slug:'kkr'},
  {name:'Bain Capital',            slug:'baincapital'},
  {name:'Elliott Management',      slug:'elliotmgmt'},
  {name:'Pershing Square',         slug:'pershingsquare'},
  {name:'Coatue',                  slug:'coatue'},
  {name:'Renaissance Tech',        slug:'renaissance'},
];

// ═══════════════════════════════════════════════════════
// THE MUSE — finance category sweep
// ═══════════════════════════════════════════════════════
async function syncMuse() {
  console.log('\n[3] The Muse API...');
  let added = 0;

  // Category sweep
  for (const cat of ['Finance', 'Investment Banking', 'Asset & Wealth Management']) {
    console.log(`  Sweeping "${cat}"...`);
    for (let page = 0; page < 50; page++) {
      try {
        const url = `https://www.themuse.com/api/public/jobs?category=${encodeURIComponent(cat)}&page=${page}`;
        const res = await fetchJ(url, {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 10000);
        if (!res.ok) { console.log(`    ${res.status} on page ${page}, stopping`); break; }
        const d = await res.json();
        const results = d.results || [];
        if (!results.length) break;
        if (page === 0) console.log(`    Total available: ${d.total}`);
        const mapped = results.map(r => ({
          source_id: `muse-${r.id}`,
          title: r.name || '',
          firm: r.company?.name || 'Unknown',
          location: r.locations?.map(l=>l.name).join(', ') || null,
          description: (r.contents||'').replace(/<[^>]+>/g,' ').trim().slice(0,1500),
          apply_url: r.refs?.landing_page || `https://www.themuse.com/jobs/${r.id}`,
          source: 'The Muse',
          posted_at: r.publication_date ? new Date(r.publication_date).toISOString() : new Date().toISOString(),
        }));
        added += await insertClassified(mapped, `Muse/${cat}`);
        await sleep(200);
      } catch(e) { console.log(`    Error: ${e.message}`); break; }
    }
  }

  // Direct company lookup for major banks
  const banks = [
    // Big Banks
    'Goldman Sachs','JPMorgan Chase','Morgan Stanley','Citigroup','Bank of America',
    'Barclays','UBS','Deutsche Bank','HSBC','Wells Fargo','BNP Paribas','Nomura',
    'Macquarie','Mizuho Financial Group','SMBC','MUFG','RBC Capital Markets',
    'BMO Capital Markets','TD Securities','Scotiabank','Natixis','ING','Societe Generale',
    'Credit Agricole','ABN AMRO','Commerzbank','Standard Chartered','Rabobank',
    // Asset Managers
    'BlackRock','PIMCO','Vanguard','Fidelity Investments','T. Rowe Price',
    'Wellington Management','Capital Group','Invesco','Franklin Templeton',
    'AllianceBernstein','Neuberger Berman','MFS Investment Management',
    'Dimensional Fund Advisors','Nuveen','Federated Hermes','Loomis Sayles',
    'Eaton Vance','Cohen & Steers','Artisan Partners','DoubleLine Capital',
    'TCW Group','PGIM','Columbia Threadneedle','Janus Henderson','Schroders',
    'Abrdn','M&G Investments','Aviva Investors','Jupiter Asset Management',
    'Baillie Gifford','Ninety One','Investec','Legal & General Investment Management',
    'Amundi','DWS Group','Union Investment','Deka Investments',
    'State Street Global Advisors','BNY Mellon Investment Management',
    'Northern Trust Asset Management','Principal Global Investors',
    'American Century Investments','Lord Abbett','Calamos Investments',
    'WisdomTree','VanEck','Sprott','GQG Partners','Brown Advisory',
    'Pzena Investment Management','Epoch Investment Partners','Silvercrest Asset Management',
    // Custody / Prime
    'State Street','BNY Mellon','Northern Trust',
    // PE / Alternatives
    'Blackstone','KKR','Apollo Global Management','Carlyle Group','TPG',
    'Warburg Pincus','Silver Lake','Bain Capital','General Atlantic',
    'Advent International','CVC Capital Partners','EQT','Permira','Apax Partners',
    'BC Partners','Leonard Green & Partners','Francisco Partners','Thoma Bravo',
    'Vista Equity Partners','Insight Partners','TA Associates','Veritas Capital',
    'Golden Gate Capital','Clearlake Capital','Hg Capital','Nordic Capital',
    'Ardian','Eurazeo','ICG','Partners Group','Coller Capital',
    'Lexington Partners','Pantheon Ventures','Adams Street Partners',
    'Hamilton Lane','StepStone Group','HarbourVest Partners',
    'Brookfield Asset Management','Stonepeak','IFM Investors',
    'Starwood Capital Group','Prologis','LaSalle Investment Management',
    'CBRE Investment Management','Clarion Partners',
    // Hedge Funds (those with Muse presence)
    'Bridgewater Associates','Citadel','Point72','Two Sigma','Man Group',
    'Millennium Management','Balyasny Asset Management','Marshall Wace',
    'Schonfeld Strategic Advisors','AQR Capital Management',
    'D.E. Shaw','Elliott Management','Davidson Kempner Capital Management',
    'Anchorage Capital Group','Third Point','Viking Global Investors',
    'Coatue Management','Tiger Global Management',
    // Credit / Direct Lending
    'Ares Management','Blue Owl Capital','HPS Investment Partners',
    'Golub Capital','Benefit Street Partners','Barings','Crescent Capital',
    'Monroe Capital','Antares Capital','Twin Brook Capital Partners',
    'Owl Rock Capital','Hercules Capital','TCP Capital','Prospect Capital',
    'FS KKR Capital','Kayne Anderson','Sixth Street Partners',
    // Boutique Banks
    'Lazard','Evercore','Moelis & Company','Perella Weinberg Partners',
    'Houlihan Lokey','Jefferies','Guggenheim Partners','Baird',
    'Piper Sandler','Stifel','Cowen','Cantor Fitzgerald','Needham & Company',
    'William Blair','Leerink Partners','Wedbush Securities','Raymond James',
    'Robert W. Baird','KeyBanc Capital Markets','Fifth Third Securities',
    'Oppenheimer & Co','Maxim Group','B. Riley Financial',
    // Insurance / Pension (large investment arms)
    'MetLife Investment Management','Prudential Financial','New York Life Investments',
    'TIAA','Sun Life Investment Management','Manulife Investment Management',
    'Pacific Life','Lincoln Financial Group','Voya Investment Management',
    'Nationwide Financial','Allstate Investments','AIG Investments',
    'Transamerica Capital','Protective Life','Unum Group',
    // VC
    'Sequoia Capital','Andreessen Horowitz','Accel','Bessemer Venture Partners',
    'Lightspeed Venture Partners','General Catalyst','Insight Partners',
    'Ribbit Capital','QED Investors','Nyca Partners',
  ];
  for (const bank of banks) {
    for (let page = 0; page < 50; page++) {
      try {
        const res = await fetchJ(
          `https://www.themuse.com/api/public/jobs?company=${encodeURIComponent(bank)}&page=${page}`,
          {headers:{'User-Agent':'Mozilla/5.0'}}, 8000
        );
        if (!res.ok) break;
        const d = await res.json();
        const results = d.results || [];
        if (!results.length) break;
        if (page === 0) console.log(`    ✓ ${bank}: ${d.total} total`);
        added += await insertClassified(results.map(r => ({
          source_id: `muse-${r.id}`, title: r.name||'', firm: bank,
          location: r.locations?.map(l=>l.name).join(', ')||null,
          description: (r.contents||'').replace(/<[^>]+>/g,' ').trim().slice(0,1500),
          apply_url: r.refs?.landing_page||`https://www.themuse.com/jobs/${r.id}`,
          source: 'The Muse',
          posted_at: r.publication_date ? new Date(r.publication_date).toISOString() : new Date().toISOString(),
        })), `Muse/${bank}`);
        await sleep(150);
      } catch { break; }
    }
  }

  console.log(`  → ${added} added from The Muse`);
  stats.muse = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// EFC
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
];

async function syncEFC() {
  console.log('\n[4] eFinancialCareers...');
  let added = 0;
  for (const q of EFC_QUERIES) {
    for (let page = 1; page <= 6; page++) {
      try {
        const res = await fetchJ(
          `https://www.efinancialcareers.com/search?q=${q}&employment_type=permanent&format=rss&page=${page}`,
          {headers:{'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}},
          9000
        );
        if (!res.ok) break;
        const xml = await res.text();
        const items = xml.match(/<item>([\s\S]*?)<\/item>/g)||[];
        if (!items.length) break;
        if (page===1) console.log(`  "${q.replace(/\+/g,' ')}": ${items.length}/page`);
        const mapped = items.flatMap(item => {
          const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]>/)||[])[1]?.trim()||'';
          const link    = (item.match(/<link>(.*?)<\/link>/)||[])[1]?.trim()||'';
          const desc    = (item.match(/<description><!\[CDATA\[(.*?)(?:\]\]>|$)/)||[])[1]||'';
          const firm    = (item.match(/<source[^>]*>(.*?)<\/source>/)||[])[1]?.trim()||'Unknown';
          const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/)||[])[1]||'';
          const loc     = (item.match(/<category>(.*?)<\/category>/)||[])[1]?.trim()||null;
          if (!title||!link) return [];
          return [{
            source_id:`efc-${Buffer.from(link).toString('base64').slice(0,40)}`,
            title, firm, location:loc,
            description:desc.replace(/<[^>]+>/g,' ').trim().slice(0,1500),
            apply_url:link, source:'eFinancialCareers',
            posted_at:pubDate?new Date(pubDate).toISOString():new Date().toISOString(),
          }];
        });
        if (mapped.length) added += await insertDirect(mapped, `EFC`);
        await sleep(500);
      } catch(e) { console.log(`  EFC err: ${e.message.slice(0,40)}`); break; }
    }
  }
  console.log(`  → ${added} added from EFC`);
  stats.efc = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// ADZUNA
// ═══════════════════════════════════════════════════════
async function syncAdzuna() {
  console.log('\n[5] Adzuna...');
  console.log(`  APP_ID: ${ADZUNA_APP_ID ? ADZUNA_APP_ID.slice(0,6)+'...' : 'MISSING'}`);
  console.log(`  API_KEY: ${ADZUNA_API_KEY_VAL ? ADZUNA_API_KEY_VAL.slice(0,6)+'...' : 'MISSING'}`);

  if (!ADZUNA_APP_ID || !ADZUNA_API_KEY_VAL) {
    console.log('  → SKIPPED: Check GitHub secrets have ADZUNA_APP_ID and ADZUNA_API_KEY');
    return 0;
  }

  const queries = [
    'investment banker','sales trader','equity researcher','portfolio manager',
    'private equity associate','hedge fund analyst','quantitative analyst',
    'fixed income trader','FX trader','wealth manager','credit analyst',
    'derivatives trader','capital markets analyst','M&A analyst',
    'leveraged finance','CLO analyst','distressed debt','structured finance',
    'investment banking associate','equity research analyst',
  ];
  let added = 0;
  for (const q of queries) {
    for (let page = 1; page <= 5; page++) {
      try {
        const url = `https://api.adzuna.com/v1/api/jobs/us/search/${page}?app_id=${ADZUNA_APP_ID}&app_key=${ADZUNA_API_KEY_VAL}&what=${encodeURIComponent(q)}&results_per_page=50&sort_by=date&max_days_old=60`;
        const res = await fetchJ(url, {}, 9000);
        if (!res.ok) {
          const txt = await res.text();
          console.log(`  Adzuna "${q}" p${page}: ${res.status} — ${txt.slice(0,100)}`);
          break;
        }
        const d = await res.json();
        const jobs = d.results||[];
        if (!jobs.length) break;
        if (page===1) console.log(`  "${q}": ${d.count} total`);
        added += await insertClassified(jobs.map(r => ({
          source_id:`adzuna-${r.id}`, title:r.title,
          firm:r.company?.display_name||'Unknown',
          location:r.location?.display_name||null,
          description:(r.description||'').slice(0,1500),
          apply_url:r.redirect_url, source:'Adzuna',
          posted_at:r.created||new Date().toISOString(),
        })), 'Adzuna');
        await sleep(200);
      } catch(e) { console.log(`  Adzuna err: ${e.message}`); break; }
    }
  }
  console.log(`  → ${added} added from Adzuna`);
  stats.adzuna = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// GREENHOUSE
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
        added += await insertDirect(jobs.map(r => ({
          source_id:`gh-${r.id}`, title:r.title, firm:f.name,
          location:r.location?.name||null,
          description:(r.content||'').replace(/<[^>]+>/g,' ').trim().slice(0,1500),
          apply_url:r.absolute_url, source:'Greenhouse',
          posted_at:r.updated_at||new Date().toISOString(),
        })), `GH/${f.name}`);
        await sleep(60);
        break;
      } catch { await sleep(30); }
    }
  }
  console.log(`  → ${added} added from Greenhouse`);
  stats.gh = added;
  return added;
}

// ═══════════════════════════════════════════════════════
// LEVER
// ═══════════════════════════════════════════════════════
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
      if (!Array.isArray(jobs)||!jobs.length) continue;
      done.add(f.name);
      console.log(`  ✓ ${f.name}: ${jobs.length} jobs`);
      added += await insertDirect(jobs.map(r => ({
        source_id:`lever-${r.id}`, title:r.text, firm:f.name,
        location:r.categories?.location||null,
        description:(r.descriptionPlain||'').slice(0,1500),
        apply_url:r.hostedUrl||r.applyUrl, source:'Lever',
        posted_at:r.createdAt?new Date(r.createdAt).toISOString():new Date().toISOString(),
      })), `Lever/${f.name}`);
      await sleep(60);
    } catch {}
  }
  console.log(`  → ${added} added from Lever`);
  stats.lever = added;
  return added;
}

async function cleanup() {
  const cutoff = new Date(Date.now()-60*24*60*60*1000).toISOString();
  const {count} = await supabase.from('jobs').delete({count:'exact'})
    .lt('posted_at',cutoff).eq('is_featured',false);
  console.log(`\n[6] Cleanup: ${count||0} expired listings removed`);
}

async function main() {
  console.log('═'.repeat(55));
  console.log(`FRONT OFFICE JOBS SYNC — ${new Date().toISOString()}`);
  const sources = (process.env.SYNC_SOURCES||'gh,lever,muse,efc,adzuna').split(',').map(s=>s.trim());
  console.log(`Sources: ${sources.join(', ')}`);
  console.log('═'.repeat(55));

  if (sources.includes('gh'))     await syncGreenhouse();
  if (sources.includes('lever'))  await syncLever();
  if (sources.includes('muse'))   await syncMuse();
  if (sources.includes('efc'))    await syncEFC();
  if (sources.includes('adzuna')) await syncAdzuna();
  await cleanup();

  const {count} = await supabase.from('jobs').select('*',{count:'exact',head:true})
    .eq('is_front_office',true).eq('is_approved',true);
  const newTotal = stats.gh+stats.lever+stats.muse+stats.efc+stats.adzuna;
  console.log('\n'+'═'.repeat(55));
  console.log(`SYNC COMPLETE`);
  console.log(`  New this run:   ${newTotal}`);
  console.log(`  Total in DB:    ${count||'?'}`);
  console.log(`  Greenhouse:     ${stats.gh}`);
  console.log(`  Lever:          ${stats.lever}`);
  console.log(`  The Muse:       ${stats.muse}`);
  console.log(`  EFC:            ${stats.efc}`);
  console.log(`  Adzuna:         ${stats.adzuna}`);
  console.log(`  Skipped(dupe):  ${stats.skipped}`);
  console.log(`  DB errors:      ${stats.errors}`);
  console.log('═'.repeat(55));
}

main().catch(e=>{ console.error('Fatal:', e); process.exit(1); });
