// sync-jobs.js — Front Office Jobs Board
// Complete hedge fund list + big 50 banks + asset managers
// Sources: Greenhouse · Lever · Workday · Custom portals · EFC · Adzuna

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;
const ADZUNA_APP_ID        = process.env.ADZUNA_APP_ID || '';
const ADZUNA_API_KEY       = process.env.ADZUNA_API_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

async function fetchJ(url, opts = {}, ms = 10000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { ...opts, signal: ctrl.signal });
    clearTimeout(t); return r;
  } catch(e) { clearTimeout(t); throw e; }
}

// ─────────────────────────────────────────────────────────────
// CLASSIFIER
// ─────────────────────────────────────────────────────────────
async function classifyBatch(roles) {
  const list = roles.map((r,i) => `${i}: "${r.title}" at ${r.firm}`).join('\n');
  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 1200,
      messages: [{ role: 'user', content: `Classify each job as genuine front office finance (fo:true) or not (fo:false).

INCLUDE: Trading, Investment Banking, Sales & Trading, Research (equity/credit/macro), Portfolio Management,
Asset Management, Private Equity, Hedge Fund investment roles, Quant Research (alpha generation),
Wealth Management, Structured Finance, Prime Brokerage, Market/Credit/Trading Risk,
Insurance investments, Pension fund investments, Sovereign wealth, Real assets investing.

EXCLUDE: Software engineer, developer, SRE, DevOps, data engineer, ML engineer, quant developer,
quant engineer, operations, back office, compliance, legal, KYC, HR, FP&A, accounting, IT,
cybersecurity, marketing, operational risk, non-financial risk.

One JSON per line: {"i":0,"fo":true,"fn":"S&T","lv":"VP"}
fn: S&T|IBD|AM|PE|RM|PB|QR|null   lv: Analyst|Associate|VP|Director|MD|Partner|null

${list}` }]
    });
    const out = {};
    for (const line of res.content[0].text.trim().split('\n')) {
      try { const o = JSON.parse(line.trim()); if (typeof o.i==='number') out[o.i]={is_front_office:!!o.fo,function:o.fn||null,level:o.lv||null}; } catch {}
    }
    roles.forEach((_,i)=>{ if(!out[i]) out[i]={is_front_office:false,function:null,level:null}; });
    return out;
  } catch(e) {
    console.warn('  classify err:', e.message);
    const fb={}; roles.forEach((_,i)=>{fb[i]={is_front_office:false,function:null,level:null};}); return fb;
  }
}

async function insertBatch(roles) {
  if (!roles.length) return 0;
  const ids = [...new Set(roles.map(r=>r.source_id))];
  const { data: ex } = await supabase.from('jobs').select('source_id').in('source_id', ids);
  const seen = new Set((ex||[]).map(e=>e.source_id));
  const fresh = roles.filter(r=>!seen.has(r.source_id));
  if (!fresh.length) return 0;
  let added = 0;
  for (let i=0; i<fresh.length; i+=20) {
    const batch = fresh.slice(i,i+20);
    const cls = await classifyBatch(batch.map(r=>({title:r.title,firm:r.firm})));
    const ins = batch.filter((_,idx)=>cls[idx]?.is_front_office).map((r,idx)=>({...r,function:cls[idx].function,level:cls[idx].level}));
    if (ins.length) {
      const { error } = await supabase.from('jobs').insert(ins);
      if (!error) { added+=ins.length; ins.forEach(j=>console.log(`    + ${j.title} @ ${j.firm}`)); }
      else console.error('  db err:', error.message);
    }
    await sleep(200);
  }
  return added;
}

// ─────────────────────────────────────────────────────────────
// GREENHOUSE FIRMS — all 100+ hedge funds from the list + banks
// We try multiple slug variants per firm; first one with jobs wins
// ─────────────────────────────────────────────────────────────
const GH_FIRMS = [
  // ── HEDGE FUNDS FROM THE LIST ──
  { name: 'Citadel',                     slugs: ['citadelam','citadellp','citadelllc','citadel'] },
  { name: 'Bridgewater Associates',      slugs: ['bridgewater','bridgewaterassociates'] },
  { name: 'Millennium Management',       slugs: ['millennium','millenniummanagement'] },
  { name: 'Balyasny Asset Management',   slugs: ['balyasny','balyasnyam','balyasnyassetmanagement'] },
  { name: 'Man Group',                   slugs: ['mangroup','man'] },
  { name: 'Man AHL',                     slugs: ['manahl','man-ahl'] },
  { name: 'Two Sigma',                   slugs: ['twosigma','twosigmainvestments','twosigmaadvisers'] },
  { name: 'Renaissance Technologies',   slugs: ['renaissance','renaissancetechnologies','rentec'] },
  { name: 'D.E. Shaw',                   slugs: ['deshawgroup','deshaw','deshawco'] },
  { name: 'AQR Capital Management',      slugs: ['aqr','aqrcapital','aqrcapitalmanagement'] },
  { name: 'Point72 Asset Management',    slugs: ['point72','point72assetmanagement'] },
  { name: 'Elliott Investment Management', slugs: ['elliotmgmt','elliottmanagement','elliott'] },
  { name: 'Brevan Howard',              slugs: ['brevanhoward','brevan'] },
  { name: 'Marshall Wace',              slugs: ['marshallwace','mwam'] },
  { name: 'Capula Investment',          slugs: ['capula','capulainvestment','capulacapital'] },
  { name: 'Schonfeld Strategic Advisors', slugs: ['schonfeld','schonfeldstrategic','schonfeldgroup'] },
  { name: 'ExodusPoint Capital',        slugs: ['exoduspoint','exoduspointcapital'] },
  { name: 'Verition Fund Management',   slugs: ['verition','veritionfund'] },
  { name: 'Garda Capital Partners',     slugs: ['garda','gardacapital','gardacapitalpartners'] },
  { name: 'Squarepoint Capital',        slugs: ['squarepointcapital','squarepoint'] },
  { name: 'Hudson Bay Capital',         slugs: ['hudsonbay','hudsonbaycapital'] },
  { name: 'Arrowstreet Capital',        slugs: ['arrowstreet','arrowstreetcapital'] },
  { name: 'Fortress Investment Group',  slugs: ['fortress','fortressinvestment','fortressinvestmentgroup'] },
  { name: 'Farallon Capital',           slugs: ['farallon','faralloncapital','faralloncm'] },
  { name: 'Adage Capital Management',   slugs: ['adage','adagecapital','adagecapitalmanagement'] },
  { name: 'Alphadyne Asset Management', slugs: ['alphadyne','alphadyneasset'] },
  { name: 'Element Capital',           slugs: ['elementcapital','element','elementcm'] },
  { name: 'Davidson Kempner',          slugs: ['davidsonkempner','dkam','davidsonkemper'] },
  { name: 'Tudor Investment Corp',      slugs: ['tudor','tudorinvestment','tudorinvestmentcorp'] },
  { name: 'Baupost Group',             slugs: ['baupost','baupostgroup'] },
  { name: 'Lone Pine Capital',          slugs: ['lonepine','lonepinecapital'] },
  { name: 'Viking Global Investors',    slugs: ['vikingglobal','viking','vikinglobal'] },
  { name: 'Coatue Management',         slugs: ['coatue','coatuemanagement'] },
  { name: 'Tiger Global Management',   slugs: ['tigereyecm','tiger','tigerlobal','tigerglobal'] },
  { name: 'Pershing Square Capital',    slugs: ['pershingsquare','pershingsquarecapital'] },
  { name: 'Appaloosa Management',      slugs: ['appaloosa','appaloosamanagement'] },
  { name: 'Magnetar Capital',          slugs: ['magnetar','magnetarcapital'] },
  { name: 'Alyeska Investment Group',  slugs: ['alyeska','alyeskainvestment'] },
  { name: 'Silver Point Capital',      slugs: ['silverpoint','silverpointcapital'] },
  { name: 'Cerberus Capital',          slugs: ['cerberuscapital','cerberus'] },
  { name: 'Centerbridge Partners',     slugs: ['centerbridge','centerbridgepartners'] },
  { name: 'Strategic Value Partners',  slugs: ['svp','strategicvalue','strategicvaluepartners'] },
  { name: 'HBK Capital Management',    slugs: ['hbk','hbkcapital','hbkcapitalmanagement'] },
  { name: 'Winton Group',             slugs: ['winton','wintongroup','wintoncapital'] },
  { name: 'Graham Capital Management', slugs: ['grahamcapital','graham','grahamcm'] },
  { name: 'PDT Partners',             slugs: ['pdtpartners','pdt'] },
  { name: 'Capstone Investment Advisors', slugs: ['capstone','capstoneinvestment','capstoneia'] },
  { name: 'Caxton Associates',         slugs: ['caxton','caxtonassociates'] },
  { name: 'Haidar Capital',           slugs: ['haidar','haidarcapital'] },
  { name: 'Paloma Partners',           slugs: ['paloma','palomapartners'] },
  { name: 'Walleye Capital',           slugs: ['walleye','walleyecapital','walleyetrading'] },
  { name: 'Holocene Advisors',         slugs: ['holocene','holoceneadvisors'] },
  { name: 'Durable Capital Partners',  slugs: ['durablecapital','durable','durablecapitalpartners'] },
  { name: 'Orbis Investment',          slugs: ['orbis','orbisinvestment'] },
  { name: 'Varde Partners',            slugs: ['varde','vardepartners'] },
  { name: 'HPS Investment Partners',   slugs: ['hps','hpsinvestment','hpsip'] },
  { name: 'King Street Capital',       slugs: ['kingstreet','kingstreetcapital','kscm'] },
  { name: 'BlueCrest Capital',         slugs: ['bluecrest','bluecrestcapital'] },
  { name: 'Moore Capital Management',  slugs: ['moorecapital','moore','moorecm'] },
  { name: 'Third Point LLC',           slugs: ['thirdpoint','thirdpointllc'] },
  { name: 'York Capital Management',   slugs: ['yorkcapital','york','yorkcm'] },
  { name: 'Och-Ziff / Sculptor',      slugs: ['sculptor','ochziff','och-ziff'] },
  { name: 'Anchorage Capital',         slugs: ['anchorage','anchoragecapital','anchoragecapitalgroup'] },
  { name: 'Canyon Partners',           slugs: ['canyon','canyoncapital','canyonpartners'] },
  { name: 'TCI Fund Management',       slugs: ['tci','tcifund','tcifundmanagement'] },
  { name: 'Lansdowne Partners',        slugs: ['lansdowne','lansdownepartners'] },
  { name: 'Egerton Capital',           slugs: ['egerton','egertoncapital'] },
  { name: 'Rokos Capital',             slugs: ['rokos','rokoscapital'] },
  { name: 'CQS',                       slugs: ['cqs','cqsgroup'] },
  { name: 'Boussard & Gavaudan',       slugs: ['boussard','boussardgavaudan','bgam'] },
  { name: 'GAM Investments',           slugs: ['gam','gaminvestments'] },
  { name: 'Aspect Capital',            slugs: ['aspect','aspectcapital'] },
  { name: 'Systematica Investments',   slugs: ['systematica','systematicainvestments'] },
  { name: 'Duquesne Family Office',    slugs: ['duquesne','duquesnefamilyoffice','duquesnecapital'] },
  { name: 'Greenlight Capital',        slugs: ['greenlight','greenlightcapital'] },
  { name: 'Soros Fund Management',     slugs: ['sorosfundmgmt','sorosfund','soros'] },
  { name: 'Paulson & Co',              slugs: ['paulson','paulsonco'] },
  { name: 'Marathon Asset Management', slugs: ['marathon','marathonasset','marathonam'] },
  { name: 'Claren Road Asset Mgmt',   slugs: ['clarenroad','clarenroadasset'] },
  { name: 'Highbridge Capital',        slugs: ['highbridge','highbridgecapital'] },
  { name: 'Stone Ridge Asset Mgmt',    slugs: ['stoneridge','stoneridgeasset'] },
  { name: 'Eisler Capital',            slugs: ['eisler','eislercapital'] },
  { name: 'Alkeon Capital',            slugs: ['alkeon','alkeoncapital'] },
  { name: 'Baker Brothers Advisors',   slugs: ['bakerbrothers','bakerbros'] },
  { name: 'Scopia Capital',            slugs: ['scopia','scopiacapital'] },
  { name: 'Redwood Capital',           slugs: ['redwoodcapital','redwood'] },
  { name: 'Tourbillon Capital',        slugs: ['tourbillon','tourbilloncapital'] },
  { name: 'Tiger Management',          slugs: ['tigermanagement','tigerll'] },
  { name: 'Kepos Capital',             slugs: ['kepos','keposcapital'] },
  { name: 'Quantlab Financial',        slugs: ['quantlab','quantlabfinancial'] },
  // Market makers
  { name: 'Optiver',                   slugs: ['optiver'] },
  { name: 'DRW',                       slugs: ['drw','drwtrading'] },
  { name: 'IMC Trading',               slugs: ['imc','imctrading'] },
  { name: 'Akuna Capital',             slugs: ['akunacapital','akuna'] },
  { name: 'Jump Trading',              slugs: ['jumptrading','jump'] },
  { name: 'Wolverine Trading',         slugs: ['wolverinetrading','wolverine'] },
  { name: 'Virtu Financial',           slugs: ['virtu','virtufinancial'] },
  { name: 'Hudson River Trading',      slugs: ['hudsonrivertrading','hrt'] },
  { name: 'Jane Street',               slugs: ['janestreet'] },
  { name: 'Susquehanna (SIG)',         slugs: ['susquehanna','sig'] },
  { name: 'Flow Traders',              slugs: ['flowtraders'] },
  // Banks / Boutiques
  { name: 'Nomura',                    slugs: ['nomura'] },
  { name: 'Macquarie',                 slugs: ['macquariegroup','macquarie'] },
  { name: 'Mizuho',                    slugs: ['mizuho','mizuhofs'] },
  { name: 'SMBC',                      slugs: ['smbc','smbcnikko'] },
  { name: 'MUFG',                      slugs: ['mufg','mufgamericas'] },
  { name: 'Daiwa Capital Markets',     slugs: ['daiwa','daiwacm'] },
  { name: 'RBC Capital Markets',       slugs: ['rbc','rbccm'] },
  { name: 'BMO Capital Markets',       slugs: ['bmo','bmocm'] },
  { name: 'TD Securities',             slugs: ['tdsecurities'] },
  { name: 'Scotiabank GBM',           slugs: ['scotiabank'] },
  { name: 'Natixis CIB',              slugs: ['natixis'] },
  { name: 'ING',                       slugs: ['ingbank'] },
  { name: 'BNP Paribas',              slugs: ['bnpparibas'] },
  { name: 'Societe Generale',          slugs: ['societegenerale'] },
  { name: 'Credit Agricole CIB',       slugs: ['creditagricole'] },
  { name: 'Rabobank',                  slugs: ['rabobank'] },
  { name: 'ABN AMRO',                  slugs: ['abnamro'] },
  { name: 'Commerzbank',               slugs: ['commerzbank'] },
  { name: 'Lazard',                    slugs: ['lazard'] },
  { name: 'Evercore',                  slugs: ['evercore','evercoregroup'] },
  { name: 'Moelis & Company',          slugs: ['moelis'] },
  { name: 'Perella Weinberg',          slugs: ['pwp','pwpartners'] },
  { name: 'Houlihan Lokey',            slugs: ['houlihanlokeyinc','houlihanlokey'] },
  { name: 'Jefferies',                 slugs: ['jefferies','jefferiesllc'] },
  { name: 'Guggenheim Partners',       slugs: ['guggenheimpartners','guggenheim'] },
  { name: 'Baird',                     slugs: ['rwbaird'] },
  { name: 'Piper Sandler',             slugs: ['pipersandler'] },
  { name: 'Stifel',                    slugs: ['stifel'] },
  { name: 'TD Cowen',                  slugs: ['tdcowen','cowen'] },
  { name: 'Oppenheimer',               slugs: ['oppenheimer'] },
  { name: 'Cantor Fitzgerald',         slugs: ['cantorfitzgerald'] },
  { name: 'Needham & Company',         slugs: ['needham'] },
  { name: 'Wedbush Securities',        slugs: ['wedbush'] },
  { name: 'Leerink Partners',          slugs: ['leerink'] },
  { name: 'JMP Securities',            slugs: ['jmp'] },
  { name: 'Roth Capital',              slugs: ['rothcapital'] },
  { name: 'KeyBanc Capital Markets',   slugs: ['keybanc','keybancapital'] },
  { name: 'Truist Securities',         slugs: ['truist'] },
  { name: 'William Blair',             slugs: ['williamblair'] },
  { name: 'General Atlantic',          slugs: ['generalatlantic'] },
  // Asset managers
  { name: 'BlackRock',                 slugs: ['blackrock','blackrockjobs'] },
  { name: 'Wellington Management',     slugs: ['wellingtonmanagement','wellington'] },
  { name: 'T. Rowe Price',             slugs: ['troweprice'] },
  { name: 'Invesco',                   slugs: ['invesco'] },
  { name: 'Franklin Templeton',        slugs: ['franklintempleton'] },
  { name: 'PGIM',                      slugs: ['pgim'] },
  { name: 'Western Asset Mgmt',        slugs: ['westernasset'] },
  { name: 'AllianceBernstein',         slugs: ['alliancebernstein','ab'] },
  { name: 'Neuberger Berman',          slugs: ['neubergerberman'] },
  { name: 'Loomis Sayles',             slugs: ['loomissayles'] },
  { name: 'Dimensional',               slugs: ['dimensional','dfa'] },
  { name: 'MFS Investment Management', slugs: ['mfs'] },
  { name: 'Columbia Threadneedle',     slugs: ['columbiathreadneedle'] },
  { name: 'Artisan Partners',          slugs: ['artisanpartners'] },
  { name: 'Cohen & Steers',            slugs: ['cohensteers'] },
  { name: 'Lazard Asset Management',   slugs: ['lazardasset'] },
  { name: 'Federated Hermes',          slugs: ['federatedhermes'] },
  { name: 'Eaton Vance',               slugs: ['eatonvance'] },
  { name: 'TCW Group',                 slugs: ['tcw'] },
  { name: 'DoubleLine Capital',        slugs: ['doubleline'] },
  { name: 'Capital Group',             slugs: ['capitalgroup','americanfunds'] },
  { name: 'Russell Investments',       slugs: ['russellinvestments'] },
  { name: 'Nuveen',                    slugs: ['nuveen'] },
  { name: 'PIMCO',                     slugs: ['pimco'] },
  // PE
  { name: 'KKR',                       slugs: ['kkr','kkrecruitment'] },
  { name: 'Apollo Global Management',  slugs: ['apolloglobal','apollo'] },
  { name: 'The Carlyle Group',         slugs: ['carlyle','thecarlylegroup'] },
  { name: 'TPG Capital',               slugs: ['tpg'] },
  { name: 'Warburg Pincus',            slugs: ['warburgpincus'] },
  { name: 'Silver Lake',               slugs: ['silverlake'] },
  { name: 'Golub Capital',             slugs: ['golubcapital'] },
  { name: 'Ares Management',           slugs: ['aresmanagement','ares'] },
  { name: 'Blue Owl Capital',          slugs: ['blueowl'] },
  { name: 'Brookfield',                slugs: ['brookfield'] },
  { name: 'Leonard Green & Partners',  slugs: ['leonardgreen'] },
  { name: 'Bain Capital',              slugs: ['baincapital'] },
  { name: 'Thoma Bravo',              slugs: ['thomabravo'] },
  { name: 'Vista Equity Partners',     slugs: ['vistaequity'] },
  { name: 'Hamilton Lane',             slugs: ['hamiltonlane'] },
  { name: 'StepStone Group',           slugs: ['stepstonegroup'] },
  { name: 'Pantheon Ventures',         slugs: ['pantheon'] },
  { name: 'HarbourVest Partners',      slugs: ['harbourvest'] },
  { name: 'Berkshire Partners',        slugs: ['berkshirepartners'] },
  { name: 'GTCR',                      slugs: ['gtcr'] },
  { name: 'Advent International',      slugs: ['advent'] },
  { name: 'TA Associates',             slugs: ['ta','taassociates'] },
  { name: 'Charlesbank Capital',       slugs: ['charlesbank'] },
  { name: 'Stone Point Capital',       slugs: ['stonepoint'] },
  { name: 'Kayne Anderson',            slugs: ['kayneanderson'] },
  { name: 'Castle Lake',               slugs: ['castlelake'] },
  { name: 'Angelo Gordon',             slugs: ['angelogordon','angelo'] },
  { name: 'Benefit Street Partners',   slugs: ['benefitstreet'] },
  { name: 'Blackstone',                slugs: ['blackstone','blackstonecredit'] },
];

// ─────────────────────────────────────────────────────────────
// LEVER FIRMS
// ─────────────────────────────────────────────────────────────
const LEVER_FIRMS = [
  { name: 'Citadel Securities',      slug: 'citadelsecurities' },
  { name: 'Blackstone',             slug: 'blackstone' },
  { name: 'Goldman Sachs',          slug: 'goldmansachs' },
  { name: 'Morgan Stanley',         slug: 'morganstanley' },
  { name: 'JPMorgan',               slug: 'jpmorgan' },
  { name: 'JPMorgan Chase',         slug: 'jpmorganchase' },
  { name: 'Coatue',                 slug: 'coatue' },
  { name: 'Renaissance Technologies', slug: 'renaissance' },
  { name: 'D.E. Shaw Research',     slug: 'deshawresearch' },
  { name: 'Pershing Square',        slug: 'pershingsquare' },
  { name: 'Third Point',            slug: 'thirdpoint' },
  { name: 'Glenview Capital',       slug: 'glenviewcapital' },
  { name: 'Adage Capital',          slug: 'adagecapital' },
  { name: 'Soros Fund Management',  slug: 'sorosfundmgmt' },
  { name: 'Bain Capital',           slug: 'baincapital' },
  { name: 'Summit Partners',        slug: 'summit' },
  { name: 'Insight Partners',       slug: 'insight' },
  { name: 'Elliott Management',     slug: 'elliotmgmt' },
  { name: 'Barclays',               slug: 'barclays' },
  { name: 'UBS',                    slug: 'ubs' },
  { name: 'Deutsche Bank',          slug: 'deutschebank' },
  { name: 'Wells Fargo',            slug: 'wellsfargo' },
  { name: 'Bank of America',        slug: 'bankofamerica' },
  { name: 'Citi',                   slug: 'citi' },
  { name: 'HSBC',                   slug: 'hsbc' },
  { name: 'BNP Paribas',           slug: 'bnpparibas' },
  { name: 'Societe Generale',       slug: 'societegenerale' },
  { name: 'Ares Management',        slug: 'ares' },
  { name: 'KKR',                    slug: 'kkr' },
  { name: 'Davidson Kempner',       slug: 'davidsonkempner' },
  { name: 'Millennium Management',  slug: 'millennium' },
  { name: 'ExodusPoint Capital',    slug: 'exoduspoint' },
  { name: 'Hudson Bay Capital',     slug: 'hudsonbay' },
  { name: 'Magnetar Capital',       slug: 'magnetar' },
  { name: 'Anchorage Capital',      slug: 'anchorage' },
];

// ─────────────────────────────────────────────────────────────
// WORKDAY — big 50 banks + insurers
// Multiple board variants tried per tenant
// ─────────────────────────────────────────────────────────────
const WD_FIRMS = [
  { t:'gs',              boards:['campus_career_site','experienced_professional','GS_Careers'],      n:'Goldman Sachs' },
  { t:'morganstanley',   boards:['Experienced_Professionals','campus','MS_Careers'],                 n:'Morgan Stanley' },
  { t:'citi',            boards:['2','Citi_Careers','External'],                                     n:'Citi' },
  { t:'barclays',        boards:['campus','experienced','Barclays_Careers'],                         n:'Barclays' },
  { t:'ubs',             boards:['UBS_Experienced_Professionals','UBS_Campus','External'],            n:'UBS' },
  { t:'db',              boards:['DBWebsite','External','DB_Careers'],                               n:'Deutsche Bank' },
  { t:'wellsfargo',      boards:['WellsFargoJobs','External'],                                       n:'Wells Fargo' },
  { t:'hsbc',            boards:['ExternalCareerSite','Experienced_Professionals','External'],        n:'HSBC' },
  { t:'bnymellon',       boards:['BNY_Mellon_Careers','External'],                                   n:'BNY Mellon' },
  { t:'statestreet',     boards:['Global','External','StateStreet'],                                 n:'State Street' },
  { t:'northerntrust',   boards:['ntcareers','External'],                                            n:'Northern Trust' },
  { t:'creditsuisse',    boards:['CS_Careers','External'],                                           n:'Credit Suisse' },
  { t:'bnpparibas',      boards:['BNP_Careers','External'],                                         n:'BNP Paribas' },
  { t:'societegenerale', boards:['SG_Careers','External'],                                          n:'Societe Generale' },
  { t:'macquarie',       boards:['External','Macquarie'],                                           n:'Macquarie' },
  { t:'tdbankgroup',     boards:['TD_External','External'],                                         n:'TD Bank' },
  { t:'scotiabank',      boards:['External','Scotiabank'],                                          n:'Scotiabank' },
  { t:'bmo',             boards:['External','BMO'],                                                 n:'BMO Capital Markets' },
  { t:'natixis',         boards:['External'],                                                       n:'Natixis' },
  { t:'ing',             boards:['ING','External'],                                                 n:'ING' },
  { t:'mizuho',          boards:['External','Mizuho'],                                              n:'Mizuho' },
  { t:'smbc',            boards:['External','SMBC'],                                                n:'SMBC' },
  { t:'rbc',             boards:['RBC_Careers','External'],                                         n:'RBC Capital Markets' },
  { t:'jefferies',       boards:['jefferiesllc','External'],                                        n:'Jefferies' },
  { t:'stifel',          boards:['stifelcareers','External'],                                       n:'Stifel' },
  { t:'schwab',          boards:['External','Schwab'],                                              n:'Charles Schwab' },
  { t:'ameriprise',      boards:['AECareers','External'],                                           n:'Ameriprise' },
  { t:'pnc',             boards:['PNCExternalCareers','External'],                                  n:'PNC Financial' },
  { t:'truist',          boards:['TruistCareers','External'],                                       n:'Truist' },
  { t:'raymondjames',    boards:['External'],                                                       n:'Raymond James' },
  { t:'lpl',             boards:['External'],                                                       n:'LPL Financial' },
  // Asset Managers on Workday
  { t:'wellington',      boards:['Wellington','External'],                                          n:'Wellington Management' },
  { t:'blackrock',       boards:['Global','Experienced_Professionals'],                             n:'BlackRock' },
  { t:'pimco',           boards:['PIMCO','External'],                                              n:'PIMCO' },
  { t:'invesco',         boards:['External'],                                                       n:'Invesco' },
  { t:'franklintempleton', boards:['FTEMEA','FTI_External'],                                       n:'Franklin Templeton' },
  { t:'troweprice',      boards:['External'],                                                       n:'T. Rowe Price' },
  { t:'vanguard',        boards:['External','Vanguard'],                                           n:'Vanguard' },
  { t:'alliancebernstein', boards:['External','AB'],                                               n:'AllianceBernstein' },
  { t:'nuveen',          boards:['NuveenExternal','External'],                                      n:'Nuveen' },
  { t:'tcw',             boards:['External'],                                                       n:'TCW Group' },
  { t:'ares',            boards:['AriesManagement','External'],                                     n:'Ares Management' },
  { t:'kkr',             boards:['KKR','External'],                                                n:'KKR' },
  { t:'apollo',          boards:['apolloglobal','External'],                                        n:'Apollo Global' },
  { t:'carlyle',         boards:['External_Careers','External'],                                    n:'Carlyle' },
  { t:'blackstone',      boards:['Blackstone','External'],                                          n:'Blackstone' },
  { t:'brookfield',      boards:['External'],                                                       n:'Brookfield' },
  { t:'hamiltonlane',    boards:['External'],                                                       n:'Hamilton Lane' },
  // Insurance/Pension
  { t:'tiaa',            boards:['TIAA','External'],                                               n:'TIAA' },
  { t:'principal',       boards:['PFG','External'],                                                n:'Principal Financial' },
  { t:'metlife',         boards:['EXTERNAL','External'],                                           n:'MetLife' },
  { t:'prudential',      boards:['Prudential','External'],                                         n:'Prudential' },
  { t:'allstate',        boards:['allstate','External'],                                           n:'Allstate' },
  { t:'nationwide',      boards:['nationwide','External'],                                         n:'Nationwide' },
  { t:'massmutual',      boards:['MassMutual','External'],                                        n:'MassMutual' },
  { t:'lincolnfinancial', boards:['Lincoln','External'],                                           n:'Lincoln Financial' },
  { t:'hartford',        boards:['thehartford','External'],                                        n:'Hartford' },
  { t:'sunlife',         boards:['SunLifeFinancial','External'],                                   n:'Sun Life' },
  { t:'manulife',        boards:['manulife','External'],                                           n:'Manulife' },
  { t:'voya',            boards:['External'],                                                      n:'Voya Financial' },
  { t:'aig',             boards:['AIG','External'],                                               n:'AIG' },
];

const WD_TERMS = [
  'investment banking','sales trading','equity research','portfolio manager',
  'quantitative','fixed income','private equity','capital markets',
  'wealth management','credit analyst','derivatives','asset management',
  'fund manager','macro','structured finance',
];

// ─────────────────────────────────────────────────────────────
// CUSTOM SCRAPERS — firms with own portals
// ─────────────────────────────────────────────────────────────

// Millennium Management — mlp.com career portal
async function scrapMillennium() {
  const terms = ['analyst','trader','portfolio manager','researcher','quantitative','investment'];
  let added = 0;
  // Try multiple URL patterns for MLP careers
  const urlPatterns = [
    term => `https://mlp.com/api/v1/jobs?search=${encodeURIComponent(term)}`,
    term => `https://mlp.com/careers/search?q=${encodeURIComponent(term)}&format=json`,
    term => `https://www.mlp.com/api/jobs?keyword=${encodeURIComponent(term)}`,
  ];
  for (const term of terms) {
    for (const urlFn of urlPatterns) {
      try {
        const res = await fetchJ(urlFn(term), { headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'} }, 8000);
        if (!res.ok) continue;
        const text = await res.text();
        if (!text.includes('{')) continue;
        const d = JSON.parse(text);
        const jobs = d.jobs||d.results||d.data||d.items||[];
        if (!jobs.length) continue;
        console.log(`    MLP "${term}": ${jobs.length}`);
        added += await insertBatch(jobs.map(j=>({
          source_id:`mlp-${j.id||j.jobId||j.requisitionId||Math.random().toString(36).slice(2)}`,
          title:j.title||j.jobTitle||j.name||'', firm:'Millennium Management',
          location:j.location||j.city||j.locationName||null,
          description:(j.description||j.jobDescription||'').replace(/<[^>]+>/g,' ').slice(0,1500),
          apply_url:j.url||j.applyUrl||j.applicationUrl||'https://www.mlp.com/careers',
          source:'Millennium Careers', is_front_office:true, is_approved:true,
          posted_at:new Date().toISOString(),
        })));
        break;
      } catch {}
    }
    await sleep(300);
  }
  return added;
}

// JPMorgan — Oracle Fusion HCM
async function scrapJPMorgan() {
  const terms = ['investment banking','sales trading','equity research','portfolio','quantitative','capital markets','fixed income','derivatives','wealth management','private equity','macro','credit'];
  let added = 0;
  for (const term of terms) {
    try {
      const url = `https://jpmc.fa.oracle.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions?keyword=${encodeURIComponent(term)}&mode=location`;
      const res = await fetchJ(url, { headers:{'Accept':'application/json','User-Agent':'Mozilla/5.0'} }, 10000);
      if (!res.ok) continue;
      const d = await res.json();
      const reqs = d.requisitionList||d.items||d.data||[];
      if (!reqs.length) continue;
      console.log(`    JPM "${term}": ${reqs.length}`);
      added += await insertBatch(reqs.map(r=>({
        source_id:`jpmc-${r.Id||r.id||r.requisitionId||Math.random().toString(36).slice(2)}`,
        title:r.Title||r.PostedJobTitle||r.title||'', firm:'JPMorgan Chase',
        location:r.PrimaryLocation||r.primaryLocation||null,
        description:(r.ExternalDescriptionStr||r.jobDescription||'').replace(/<[^>]+>/g,' ').slice(0,1500),
        apply_url:`https://jpmc.fa.oracle.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions/${r.Id||r.id}`,
        source:'JPMorgan Careers', is_front_office:true, is_approved:true,
        posted_at:new Date().toISOString(),
      })));
      await sleep(400);
    } catch(e){ console.log(`    JPM "${term}" err: ${e.message.slice(0,50)}`); }
  }
  return added;
}

// Bank of America — custom portal
async function scrapBofA() {
  const terms = ['investment banking','sales trading','equity research','portfolio manager','capital markets','fixed income','quantitative','wealth management'];
  let added = 0;
  for (const term of terms) {
    for (const url of [
      `https://careers.bankofamerica.com/en-us/job-search-results?q=${encodeURIComponent(term)}&format=json`,
      `https://careers.bankofamerica.com/api/jobs?search=${encodeURIComponent(term)}`,
    ]) {
      try {
        const res = await fetchJ(url, { headers:{'Accept':'application/json','User-Agent':'Mozilla/5.0'} }, 10000);
        if (!res.ok) continue;
        const text = await res.text();
        if (!text.startsWith('{')&&!text.startsWith('[')) continue;
        const d = JSON.parse(text);
        const jobs = d.jobs||d.results||d.data||[];
        if (!jobs.length) continue;
        console.log(`    BofA "${term}": ${jobs.length}`);
        added += await insertBatch(jobs.map(j=>({
          source_id:`bofa-${j.id||j.jobId||Math.random().toString(36).slice(2)}`,
          title:j.title||j.jobTitle||'', firm:'Bank of America',
          location:j.location||null,
          description:(j.description||'').replace(/<[^>]+>/g,' ').slice(0,1500),
          apply_url:j.url||j.applyUrl||'https://careers.bankofamerica.com',
          source:'BofA Careers', is_front_office:true, is_approved:true,
          posted_at:new Date().toISOString(),
        })));
        break;
      } catch {}
    }
    await sleep(400);
  }
  return added;
}

// ─────────────────────────────────────────────────────────────
// EFC QUERIES — 72 queries × 5 pages
// ─────────────────────────────────────────────────────────────
const EFC_QUERIES = [
  'sales+trading+analyst','sales+trading+associate','sales+trading+VP','sales+trading+director',
  'investment+banking+analyst','investment+banking+associate','investment+banking+VP',
  'M%26A+analyst','M%26A+associate','leveraged+finance','DCM+analyst','ECM+analyst',
  'equity+research+analyst','equity+research+associate','credit+research+analyst',
  'fixed+income+research','macro+strategist','global+macro',
  'portfolio+manager','investment+analyst','fund+manager','CIO',
  'hedge+fund+analyst','hedge+fund+associate','hedge+fund+trader',
  'quantitative+researcher','quantitative+analyst','quantitative+trader',
  'prop+trader','proprietary+trader',
  'FX+trader','rates+trader','credit+trader','equity+trader',
  'commodities+trader','options+trader','derivatives+trader','vol+trader',
  'structured+finance','structured+products','securitization',
  'CLO+analyst','ABS+analyst','MBS+analyst','RMBS','CMBS',
  'high+yield+analyst','distressed+debt','special+situations','credit+opportunities',
  'private+equity+associate','private+equity+analyst','PE+associate',
  'growth+equity+associate','venture+capital+associate',
  'wealth+manager','private+banker','private+wealth+advisor','family+office',
  'prime+brokerage','securities+lending',
  'capital+markets+analyst','loan+syndication','project+finance',
  'real+estate+investment','infrastructure+investment','real+assets',
  'insurance+investment+analyst','insurance+portfolio+manager',
  'pension+fund+manager','pension+investment+officer',
  'endowment+investment','sovereign+wealth+fund',
  'merger+arbitrage','convertible+bonds+analyst',
  'trading+risk','market+risk+analyst','credit+risk+analyst',
];

const ADZUNA_QUERIES = [
  'investment banker','sales trader bank','equity researcher',
  'portfolio manager finance','quantitative analyst finance',
  'fixed income trader','FX trader bank','private equity associate',
  'hedge fund analyst','wealth manager bank','credit analyst bank',
  'derivatives trader','capital markets analyst','M&A analyst',
  'leveraged finance analyst','structured finance analyst',
  'insurance investment analyst','pension fund manager',
  'prime brokerage analyst','macro trader hedge fund',
];

// ─────────────────────────────────────────────────────────────
// SYNC FUNCTIONS
// ─────────────────────────────────────────────────────────────
async function syncGreenhouse() {
  console.log('\n[1/7] Greenhouse ATS...');
  let raw=0, added=0;
  const firmsSeen = new Set();
  for (const f of GH_FIRMS) {
    if (firmsSeen.has(f.name)) continue;
    for (const slug of f.slugs) {
      try {
        const res = await fetchJ(`https://boards-api.greenhouse.io/v1/boards/${slug}/jobs`,
          { headers:{'User-Agent':'Mozilla/5.0'} }, 6000);
        if (!res.ok) continue;
        const { jobs=[] } = await res.json();
        if (!jobs.length) continue;
        firmsSeen.add(f.name);
        console.log(`  ✓ ${f.name} (${slug}): ${jobs.length} jobs`);
        raw+=jobs.length;
        added+=await insertBatch(jobs.map(r=>({
          source_id:`gh-${r.id}`, title:r.title, firm:f.name,
          location:r.location?.name||null,
          description:(r.content||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
          apply_url:r.absolute_url, source:'Greenhouse',
          is_front_office:true, is_approved:true,
          posted_at:r.updated_at||new Date().toISOString(),
        })));
        await sleep(100);
        break; // found a working slug, move on
      } catch { await sleep(50); }
    }
  }
  console.log(`  → ${raw} raw, ${added} added`);
  return added;
}

async function syncLever() {
  console.log('\n[2/7] Lever ATS...');
  let raw=0, added=0;
  const seen=new Set();
  for (const f of LEVER_FIRMS) {
    if (seen.has(f.name)) continue;
    try {
      const res = await fetchJ(`https://api.lever.co/v0/postings/${f.slug}?mode=json&limit=100`,
        { headers:{'User-Agent':'Mozilla/5.0'} }, 6000);
      if (!res.ok) continue;
      const jobs = await res.json();
      if (!Array.isArray(jobs)||!jobs.length) continue;
      seen.add(f.name);
      console.log(`  ✓ ${f.name}: ${jobs.length} jobs`);
      raw+=jobs.length;
      added+=await insertBatch(jobs.map(r=>({
        source_id:`lever-${r.id}`, title:r.text, firm:f.name,
        location:r.categories?.location||null,
        description:(r.descriptionPlain||'').slice(0,1500),
        apply_url:r.hostedUrl||r.applyUrl, source:'Lever',
        is_front_office:true, is_approved:true,
        posted_at:r.createdAt?new Date(r.createdAt).toISOString():new Date().toISOString(),
      })));
      await sleep(100);
    } catch {}
  }
  console.log(`  → ${raw} raw, ${added} added`);
  return added;
}

async function syncWorkday() {
  console.log('\n[3/7] Workday ATS...');
  let raw=0, added=0;
  for (const f of WD_FIRMS) {
    let firmGotJobs = false;
    for (const board of f.boards) {
      if (firmGotJobs) break;
      for (const term of WD_TERMS) {
        for (const ver of ['wd5','wd3']) {
          try {
            const url=`https://${f.t}.${ver}.myworkdayjobs.com/wday/cxs/${f.t}/${board}/jobs`;
            const res=await fetchJ(url,{
              method:'POST',
              headers:{'Content-Type':'application/json','Accept':'application/json','User-Agent':'Mozilla/5.0'},
              body:JSON.stringify({appliedFacets:{},limit:20,offset:0,searchText:term}),
            },9000);
            if (!res.ok) continue;
            const d=await res.json();
            const posts=d.jobPostings||[];
            if (!posts.length) continue;
            console.log(`  ✓ ${f.n} (${board}/${ver}) "${term}": ${posts.length}`);
            raw+=posts.length; firmGotJobs=true;
            added+=await insertBatch(posts.map(r=>({
              source_id:`wd-${f.t}-${(r.externalPath||Math.random().toString(36)).replace(/\W/g,'-').slice(-25)}`,
              title:r.title, firm:f.n, location:r.locationsText||null, description:'',
              apply_url:`https://${f.t}.wd5.myworkdayjobs.com${r.externalPath||''}`,
              source:'Workday', is_front_office:true, is_approved:true,
              posted_at:r.postedOn?new Date(r.postedOn).toISOString():new Date().toISOString(),
            })));
            await sleep(250); break;
          } catch { await sleep(100); }
        }
        if (firmGotJobs) break;
        await sleep(150);
      }
    }
  }
  console.log(`  → ${raw} raw, ${added} added`);
  return added;
}

async function syncCustom() {
  console.log('\n[4/7] Custom scrapers...');
  const mlp  = await scrapMillennium();
  const jpm  = await scrapJPMorgan();
  const bofa = await scrapBofA();
  console.log(`  → MLP:${mlp} JPM:${jpm} BofA:${bofa}`);
  return mlp+jpm+bofa;
}

async function syncEFC() {
  console.log('\n[5/7] eFinancialCareers...');
  let added=0;
  for (const q of EFC_QUERIES) {
    for (let p=1;p<=5;p++) {
      try {
        const res=await fetchJ(
          `https://www.efinancialcareers.com/search?q=${q}&employment_type=permanent&format=rss&page=${p}`,
          { headers:{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'} }, 9000
        );
        if (!res.ok) break;
        const xml=await res.text();
        const items=(xml.match(/<item>([\s\S]*?)<\/item>/g)||[]);
        if (!items.length) break;
        if (p===1) console.log(`  "${q}": ${items.length}+`);
        const mapped=items.flatMap(item=>{
          const title=(item.match(/<title><!\[CDATA\[(.*?)\]\]>/)||[])[1]?.trim()||'';
          const link=(item.match(/<link>(.*?)<\/link>/)||[])[1]?.trim()||'';
          const desc=(item.match(/<description><!\[CDATA\[(.*?)(?:\]\]>|$)/)||[])[1]||'';
          const firm=(item.match(/<source[^>]*>(.*?)<\/source>/)||[])[1]?.trim()||'Unknown';
          const pubDate=(item.match(/<pubDate>(.*?)<\/pubDate>/)||[])[1]||'';
          const loc=(item.match(/<category>(.*?)<\/category>/)||[])[1]?.trim()||null;
          if (!title||!link) return [];
          return [{
            source_id:`efc-${Buffer.from(link).toString('base64').slice(0,40)}`,
            title, firm, location:loc,
            description:desc.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,1500),
            apply_url:link, source:'eFinancialCareers',
            is_front_office:true, is_approved:true,
            posted_at:pubDate?new Date(pubDate).toISOString():new Date().toISOString(),
          }];
        });
        if (mapped.length) added+=await insertBatch(mapped);
        await sleep(400);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added`);
  return added;
}

async function syncAdzuna() {
  if (!ADZUNA_APP_ID||!ADZUNA_API_KEY) {
    console.log('\n[6/7] Adzuna: SKIPPED — add ADZUNA_APP_ID + ADZUNA_API_KEY secrets');
    console.log('  Free at developer.adzuna.com — adds ~2000 more FO roles');
    return 0;
  }
  console.log('\n[6/7] Adzuna...');
  let added=0;
  for (const q of ADZUNA_QUERIES) {
    for (let p=1;p<=5;p++) {
      try {
        const res=await fetchJ(`https://api.adzuna.com/v1/api/jobs/us/search/${p}?app_id=${ADZUNA_APP_ID}&app_key=${ADZUNA_API_KEY}&what=${encodeURIComponent(q)}&results_per_page=50&category=finance-jobs&sort_by=date&max_days_old=60`,{},9000);
        if (!res.ok) break;
        const d=await res.json();
        const jobs=d.results||[];
        if (!jobs.length) break;
        if (p===1) console.log(`  "${q}": ${d.count} total`);
        added+=await insertBatch(jobs.map(r=>({
          source_id:`adzuna-${r.id}`, title:r.title,
          firm:r.company?.display_name||'Unknown',
          location:r.location?.display_name||null,
          description:(r.description||'').slice(0,1500),
          apply_url:r.redirect_url, source:'Adzuna',
          is_front_office:true, is_approved:true,
          posted_at:r.created||new Date().toISOString(),
        })));
        await sleep(300);
      } catch { break; }
    }
  }
  console.log(`  → ${added} added`);
  return added;
}

async function cleanupExpired() {
  const cutoff=new Date(Date.now()-60*24*60*60*1000).toISOString();
  const {count}=await supabase.from('jobs').delete({count:'exact'}).lt('posted_at',cutoff).eq('is_featured',false);
  console.log(`\n[7/7] Cleanup: removed ${count||0} expired listings`);
}

async function main() {
  console.log(`\n[${new Date().toISOString()}] Front Office Jobs sync\n`);
  const gh   = await syncGreenhouse();
  const lv   = await syncLever();
  const wd   = await syncWorkday();
  const cust = await syncCustom();
  const efc  = await syncEFC();
  const adz  = await syncAdzuna();
  await cleanupExpired();
  const total=gh+lv+wd+cust+efc+adz;
  console.log(`\n✓ DONE — ${total} new roles`);
  console.log(`  GH:${gh} | Lever:${lv} | Workday:${wd} | Custom:${cust} | EFC:${efc} | Adzuna:${adz}`);
}

main().catch(e=>{ console.error('Fatal:',e); process.exit(1); });
