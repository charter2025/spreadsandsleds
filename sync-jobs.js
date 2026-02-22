// sync-jobs.js
// Runs daily via GitHub Actions cron
// Sources: Greenhouse ATS + Workday (banks/funds)
// Classifies with Claude Haiku → stores in Supabase

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const sleep    = ms => new Promise(r => setTimeout(r, ms));

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE FIRMS — verified + expanded
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  // Confirmed working
  { slug: 'point72',              name: 'Point72' },
  { slug: 'janestreet',           name: 'Jane Street' },
  { slug: 'mangroup',             name: 'Man Group' },
  // Boutique banks
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
  { slug: 'baird',                name: 'Baird' },
  { slug: 'rwbaird',              name: 'Baird' },
  { slug: 'pipersandler',         name: 'Piper Sandler' },
  { slug: 'stifel',               name: 'Stifel' },
  { slug: 'cowen',                name: 'Cowen' },
  { slug: 'tdcowen',              name: 'TD Cowen' },
  // Hedge funds / quant
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
  // Asset managers
  { slug: 'fidelity',             name: 'Fidelity Investments' },
  { slug: 'fidelityinvestments',  name: 'Fidelity Investments' },
  { slug: 'troweprice',           name: 'T. Rowe Price' },
  { slug: 'invesco',              name: 'Invesco' },
  { slug: 'franklintempleton',    name: 'Franklin Templeton' },
  { slug: 'nuveen',               name: 'Nuveen' },
  { slug: 'pgim',                 name: 'PGIM' },
  { slug: 'westernasset',         name: 'Western Asset Management' },
  // PE / Alternatives
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
  { slug: 'blueowl',              name: 'Blue Owl Capital' },
  { slug: 'bluowl',               name: 'Blue Owl Capital' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// WORKDAY FIRMS — major banks/funds that use Workday ATS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const WORKDAY_FIRMS = [
  { name: 'Goldman Sachs',   tenant: 'goldmansachs',  board: 'GoldmanSachs' },
  { name: 'JPMorgan',        tenant: 'jpmc',          board: 'JPMorgan' },
  { name: 'Morgan Stanley',  tenant: 'morganstanley', board: 'MorganStanley' },
  { name: 'BlackRock',       tenant: 'blackrock',     board: 'BlackRock' },
  { name: 'Citadel',         tenant: 'citadel',       board: 'Citadel' },
  { name: 'Blackstone',      tenant: 'blackstone',    board: 'Blackstone' },
  { name: 'Bank of America', tenant: 'bankofamerica', board: 'BankofAmerica' },
  { name: 'Citigroup',       tenant: 'citi',          board: 'Citi' },
  { name: 'Barclays',        tenant: 'barclays',      board: 'Barclays' },
  { name: 'UBS',             tenant: 'ubs',           board: 'UBS' },
  { name: 'Wells Fargo',     tenant: 'wellsfargo',    board: 'WellsFargo' },
  { name: 'HSBC',            tenant: 'hsbc',          board: 'HSBC' },
];

const WORKDAY_SEARCH_TERMS = [
  'trader', 'portfolio manager', 'investment banking', 'sales trading',
  'equity research', 'fixed income', 'private equity', 'quantitative researcher',
  'derivatives', 'credit analyst', 'wealth management', 'capital markets',
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLAUDE CLASSIFICATION — loosened filter
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyRole(title, description) {
  const prompt = `You are classifying finance job postings for a front office jobs board.

INCLUDE (is_front_office: true):
- Sales & Trading (equities, FI, FX, rates, commodities, derivatives, structured products)
- Investment Banking (M&A, ECM, DCM, leveraged finance, restructuring, coverage, origination)
- Asset/Fund Management (portfolio managers, analysts, strategists, allocators)
- Private Equity, Venture Capital, Credit, Infrastructure, Real Assets investing
- Equity Research, Credit Research, Macro Strategy, Market Intelligence
- Private Banking, Wealth Management (client-facing)
- Quantitative Research (investment/alpha focused, systematic strategies)
- Trading Risk Management (market risk, counterparty risk on trading desks)
- Structured Finance / Securitization (deal execution)
- Capital Markets (origination, syndication, underwriting)
- Corporate Access, Investor Relations

EXCLUDE (is_front_office: false):
- Pure software/tech engineering (unless explicitly on trading desk)
- Operations, settlements, clearing, reconciliation, back office
- Enterprise risk, compliance, legal
- HR, Finance/Accounting, Admin
- General data engineering

When in doubt for roles at investment banks or hedge funds — lean INCLUDE.

Respond ONLY with JSON, no other text:
{"is_front_office": true/false, "function": "S&T"|"IBD"|"AM"|"PE"|"RM"|"PB"|null, "level": "Analyst"|"Associate"|"VP"|"Director"|"MD"|"Partner"|null}

Title: ${title}
Description: ${(description || '').replace(/<[^>]+>/g, '').slice(0, 500)}`;

  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 120,
      messages: [{ role: 'user', content: prompt }]
    });
    const raw = res.content[0].text.trim().replace(/```json|```/g, '').trim();
    return JSON.parse(raw);
  } catch (e) {
    console.warn('  Claude classification failed:', e.message);
    return { is_front_office: false, function: null, level: null };
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncGreenhouse() {
  console.log('\n── Syncing Greenhouse ATS ──');
  let fetched = 0, added = 0, skipped = 0;
  const seenNames = new Set();

  for (const firm of GREENHOUSE_FIRMS) {
    try {
      const res = await fetch(
        `https://boards-api.greenhouse.io/v1/boards/${firm.slug}/jobs?content=true`,
        { headers: { 'User-Agent': 'Mozilla/5.0' } }
      );
      if (!res.ok) {
        if (!seenNames.has(firm.name)) {
          console.log(`  ✗ ${firm.name} (${firm.slug}): HTTP ${res.status}`);
        }
        continue;
      }

      const data  = await res.json();
      const roles = data.jobs || [];
      seenNames.add(firm.name);
      console.log(`  ✓ ${firm.name}: ${roles.length} total postings`);
      fetched += roles.length;

      for (const role of roles) {
        const sourceId = `greenhouse-${role.id}`;
        const { data: existing } = await supabase
          .from('jobs').select('id').eq('source_id', sourceId).maybeSingle();
        if (existing) { skipped++; continue; }

        const cl = await classifyRole(role.title, role.content);
        if (!cl.is_front_office) { skipped++; continue; }

        await supabase.from('jobs').insert({
          title:           role.title,
          firm:            firm.name,
          location:        role.location?.name || null,
          function:        cl.function,
          level:           cl.level,
          description:     (role.content || '').replace(/<[^>]+>/g, '').slice(0, 1500),
          apply_url:       role.absolute_url,
          source:          'Greenhouse',
          source_id:       sourceId,
          is_front_office: true,
          is_approved:     true,
          posted_at:       role.updated_at || new Date().toISOString(),
        });
        added++;
        console.log(`    + Added: ${role.title}`);
        await sleep(200);
      }
      await sleep(300);
    } catch (e) {
      console.error(`  ✗ ${firm.name} failed:`, e.message);
    }
  }

  console.log(`  Greenhouse: ${fetched} fetched, ${added} added, ${skipped} skipped\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// WORKDAY SYNC
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncWorkday() {
  console.log('── Syncing Workday (Major Banks & Funds) ──');
  let totalAdded = 0;

  for (const firm of WORKDAY_FIRMS) {
    let firmAdded = 0;
    const apiUrl = `https://${firm.tenant}.wd5.myworkdayjobs.com/wday/cxs/${firm.tenant}/${firm.board}/jobs`;

    for (const term of WORKDAY_SEARCH_TERMS.slice(0, 6)) {
      try {
        const res = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
          },
          body: JSON.stringify({ appliedFacets: {}, limit: 20, offset: 0, searchText: term }),
        });

        if (!res.ok) continue;
        const data  = await res.json();
        const roles = data.jobPostings || [];

        for (const role of roles) {
          const extId    = role.externalPath?.split('/').pop() || role.bulletFields?.join('-') || role.title;
          const sourceId = `workday-${firm.tenant}-${extId}`.slice(0, 200);

          const { data: existing } = await supabase
            .from('jobs').select('id').eq('source_id', sourceId).maybeSingle();
          if (existing) continue;

          const cl = await classifyRole(role.title, role.jobDescription || role.briefDescription || '');
          if (!cl.is_front_office) continue;

          const applyUrl = `https://${firm.tenant}.wd5.myworkdayjobs.com/${firm.board}/job${role.externalPath || ''}`;

          await supabase.from('jobs').insert({
            title:           role.title,
            firm:            firm.name,
            location:        role.locationsText || null,
            function:        cl.function,
            level:           cl.level,
            description:     (role.briefDescription || '').slice(0, 1500),
            apply_url:       applyUrl,
            source:          'Workday',
            source_id:       sourceId,
            is_front_office: true,
            is_approved:     true,
            posted_at:       role.postedOn ? new Date(role.postedOn).toISOString() : new Date().toISOString(),
          });
          firmAdded++;
          totalAdded++;
          console.log(`    + Added: ${role.title} @ ${firm.name}`);
          await sleep(200);
        }
        await sleep(500);
      } catch (e) {
        // skip failed search terms silently
      }
    }

    if (firmAdded > 0) {
      console.log(`  ✓ ${firm.name}: ${firmAdded} roles added`);
    } else {
      console.log(`  ~ ${firm.name}: no new roles`);
    }
    await sleep(1000);
  }

  console.log(`  Workday: ${totalAdded} new roles added\n`);
  return totalAdded;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function cleanupExpired() {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const { count } = await supabase
    .from('jobs').delete({ count: 'exact' })
    .lt('posted_at', cutoff).eq('is_featured', false);
  console.log(`── Cleanup: removed ${count || 0} expired listings\n`);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function main() {
  console.log(`\n[${new Date().toISOString()}] Starting Front Office Jobs sync...\n`);
  const ghAdded = await syncGreenhouse();
  const wdAdded = await syncWorkday();
  await cleanupExpired();
  console.log(`\n✓ Sync complete. Added ${ghAdded + wdAdded} new front office roles.`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
