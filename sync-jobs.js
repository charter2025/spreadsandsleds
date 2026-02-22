// sync-jobs.js
// Runs daily via GitHub Actions cron
// Fetches from Greenhouse ATS + Indeed RSS → classifies with Claude → stores in Supabase

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

// ── CONFIG (set these as GitHub Actions secrets) ──
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const claude   = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GREENHOUSE FIRMS
// Verify slugs at: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const GREENHOUSE_FIRMS = [
  { slug: 'goldmansachs',        name: 'Goldman Sachs' },
  { slug: 'citadel',             name: 'Citadel' },
  { slug: 'citadelsecurities',   name: 'Citadel Securities' },
  { slug: 'blackstone',          name: 'Blackstone' },
  { slug: 'kkr',                 name: 'KKR' },
  { slug: 'apolloglobal',        name: 'Apollo Global Management' },
  { slug: 'bridgewater',         name: 'Bridgewater Associates' },
  { slug: 'point72',             name: 'Point72' },
  { slug: 'deshawgroup',         name: 'D.E. Shaw' },
  { slug: 'twosigma',            name: 'Two Sigma' },
  { slug: 'janestreet',          name: 'Jane Street' },
  { slug: 'hudsonrivertrading',  name: 'Hudson River Trading' },
  { slug: 'pimco',               name: 'PIMCO' },
  { slug: 'mangroup',            name: 'Man Group' },
  { slug: 'lazard',              name: 'Lazard' },
  { slug: 'evercoregroup',       name: 'Evercore' },
  { slug: 'moelis',              name: 'Moelis & Company' },
  { slug: 'pwp',                 name: 'Perella Weinberg Partners' },
  { slug: 'houlihanlokeyinc',    name: 'Houlihan Lokey' },
  { slug: 'bcgpartners',         name: 'Cowen' },
  // Add more as you find valid slugs
];

// Indeed RSS — finance-specific queries (no auth required)
const INDEED_QUERIES = [
  { q: 'equity+sales+managing+director', loc: 'New+York' },
  { q: 'investment+banking+VP+director', loc: 'New+York' },
  { q: 'portfolio+manager+hedge+fund',   loc: 'New+York' },
  { q: 'fixed+income+trader+director',   loc: 'New+York' },
  { q: 'private+equity+associate',       loc: 'New+York' },
  { q: 'equity+research+analyst+VP',     loc: 'New+York' },
  { q: 'leveraged+finance+origination',  loc: 'New+York' },
  { q: 'FX+sales+corporate+director',    loc: 'London' },
  { q: 'credit+trading+director',        loc: 'New+York' },
  { q: 'asset+management+portfolio',     loc: 'New+York' },
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLAUDE CLASSIFICATION
// Uses Haiku — cheapest, fastest, ~$0.002 per 1000 roles
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function classifyRole(title, description) {
  const prompt = `You are classifying finance job postings for a front office jobs board.

FRONT OFFICE (revenue-generating) roles include:
- Sales & Trading (equities, fixed income, FX, commodities, derivatives, structured products)
- Investment Banking (M&A advisory, ECM, DCM, leveraged finance, coverage)
- Asset Management (portfolio management, fund management, hedge fund strategies)
- Private Equity / Venture Capital / Infrastructure investing
- Equity Research / Credit Research / Market Strategy
- Private Banking / Wealth Management (client-facing coverage)

NOT front office (reject these):
- Operations, middle office, back office
- Technology / Software Engineering / Quant Dev
- Compliance, Legal, Risk Management (non-trading)
- HR, Finance, Accounting, Admin
- Data Science (unless clearly investment-focused)

Respond with ONLY a JSON object, nothing else:
{
  "is_front_office": true/false,
  "function": "S&T" | "IBD" | "AM" | "PE" | "RM" | "PB" | null,
  "level": "Analyst" | "Associate" | "VP" | "Director" | "MD" | "Partner" | null
}

Title: ${title}
Description: ${(description || '').replace(/<[^>]+>/g, '').slice(0, 500)}`;

  try {
    const res = await claude.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 100,
      messages: [{ role: 'user', content: prompt }]
    });
    return JSON.parse(res.content[0].text.trim());
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

  for (const firm of GREENHOUSE_FIRMS) {
    try {
      const res  = await fetch(`https://boards-api.greenhouse.io/v1/boards/${firm.slug}/jobs?content=true`);
      if (!res.ok) {
        console.log(`  ✗ ${firm.name} (${firm.slug}): HTTP ${res.status} — slug may be wrong`);
        continue;
      }
      const data = await res.json();
      const roles = data.jobs || [];
      console.log(`  ✓ ${firm.name}: ${roles.length} total postings`);
      fetched += roles.length;

      for (const role of roles) {
        const sourceId = `greenhouse-${role.id}`;

        // Skip if already in DB
        const { data: existing } = await supabase
          .from('jobs')
          .select('id')
          .eq('source_id', sourceId)
          .maybeSingle();
        if (existing) { skipped++; continue; }

        // Classify with Claude
        const classification = await classifyRole(role.title, role.content);
        if (!classification.is_front_office) { skipped++; continue; }

        // Insert into Supabase
        await supabase.from('jobs').insert({
          title:           role.title,
          firm:            firm.name,
          location:        role.location?.name || null,
          function:        classification.function,
          level:           classification.level,
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

        // Rate limit: ~5 requests/sec to Claude
        await sleep(200);
      }

      await sleep(300); // Pause between firms
    } catch (e) {
      console.error(`  ✗ ${firm.name} failed:`, e.message);
    }
  }

  console.log(`  Greenhouse: ${fetched} fetched, ${added} added, ${skipped} skipped\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INDEED RSS SYNC
// No API key needed — uses public RSS feed
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function syncIndeed() {
  console.log('── Syncing Indeed RSS ──');
  let added = 0;

  for (const query of INDEED_QUERIES) {
    try {
      const url = `https://rss.indeed.com/rss?q=${query.q}&l=${query.loc}&sort=date&limit=20`;
      const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!res.ok) { console.log(`  ✗ Indeed query failed: ${res.status}`); continue; }

      const xml = await res.text();

      // Simple XML parsing without external library
      const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
      console.log(`  Query "${query.q}": ${items.length} results`);

      for (const item of items) {
        const title    = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/)  || [])[1] || '';
        const link     = (item.match(/<link>(.*?)<\/link>/)                    || [])[1] || '';
        const desc     = (item.match(/<description><!\[CDATA\[(.*?)\]\]>/)     || [])[1] || '';
        const firm     = (item.match(/<source[^>]*>(.*?)<\/source>/)           || [])[1] || 'Unknown';
        const pubDate  = (item.match(/<pubDate>(.*?)<\/pubDate>/)              || [])[1] || '';

        if (!title || !link) continue;
        const sourceId = `indeed-${Buffer.from(link).toString('base64').slice(0,32)}`;

        // Skip if already in DB
        const { data: existing } = await supabase
          .from('jobs')
          .select('id')
          .eq('source_id', sourceId)
          .maybeSingle();
        if (existing) continue;

        // Classify
        const classification = await classifyRole(title, desc);
        if (!classification.is_front_office) continue;

        // Extract location from title (Indeed often includes it)
        const locMatch = title.match(/[-–]\s*([A-Za-z\s]+,\s*[A-Z]{2})$/);
        const location = locMatch ? locMatch[1] : query.loc.replace('+', ' ');

        await supabase.from('jobs').insert({
          title:           title.replace(/\s*-\s*[A-Za-z\s]+,\s*[A-Z]{2}$/, '').trim(),
          firm:            firm,
          location:        location,
          function:        classification.function,
          level:           classification.level,
          description:     desc.replace(/<[^>]+>/g, '').slice(0, 1500),
          apply_url:       link,
          source:          'Indeed',
          source_id:       sourceId,
          is_front_office: true,
          is_approved:     true,
          posted_at:       pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
        });
        added++;
        await sleep(200);
      }

      await sleep(500); // Between Indeed queries
    } catch (e) {
      console.error(`  ✗ Indeed query failed:`, e.message);
    }
  }

  console.log(`  Indeed: ${added} new roles added\n`);
  return added;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLEANUP — Remove expired listings (60+ days old)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function cleanupExpired() {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const { count } = await supabase
    .from('jobs')
    .delete({ count: 'exact' })
    .lt('posted_at', cutoff)
    .eq('is_featured', false);
  console.log(`── Cleanup: removed ${count || 0} expired listings\n`);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function main() {
  console.log(`\n[${new Date().toISOString()}] Starting Front Office Jobs sync...\n`);

  const ghAdded     = await syncGreenhouse();
  const indeedAdded = await syncIndeed();
  await cleanupExpired();

  console.log(`\n✓ Sync complete. Added ${ghAdded + indeedAdded} new front office roles.`);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
