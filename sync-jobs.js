# .github/workflows/sync-jobs.yml
# THREE PARALLEL JOBS — total wall time ~45 min instead of 3+ hours

name: Sync Jobs

on:
  schedule:
    - cron: '0 6 * * *'   # 6am UTC daily
  workflow_dispatch:       # manual trigger anytime

jobs:

  # Greenhouse + Lever: fast (~10 min), finance-company ATS boards
  sync-ats:
    name: "ATS (Greenhouse + Lever)"
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm install
      - run: node sync-jobs.js
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          SYNC_SOURCES: "gh,lever"

  # The Muse: free, no key, covers ALL big banks (~20 min)
  sync-muse:
    name: "The Muse (big banks)"
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm install
      - run: node sync-jobs.js
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          SYNC_SOURCES: "muse"

  # EFC + Adzuna: aggregators, need slower pacing (~45 min)
  sync-aggregators:
    name: "EFC + Adzuna"
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm install
      - run: node sync-jobs.js
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          ADZUNA_APP_ID: ${{ secrets.ADZUNA_APP_ID }}
          ADZUNA_API_KEY: ${{ secrets.ADZUNA_API_KEY }}
          SYNC_SOURCES: "efc,adzuna"
