name: Ingest & Score

# Two ways this runs:
# 1. Automatically, every night at 2am UTC (2-3am UK time depending on DST)
# 2. Manually, from the GitHub Actions tab — click "Run workflow" and
#    optionally override the season / max-fixtures for that run.
#
# Re-enabled the nightly schedule now that:
#   - ingest.py pulls per-club current squads (fast, minutes not hours)
#   - Pro tier (7,500 req/day) comfortably covers a full nightly refresh
# If quota ever becomes tight again, remove the `schedule:` block below
# and rely on manual triggers instead.

on:
  workflow_dispatch:
    inputs:
      season:
        description: "Season year to ingest"
        required: false
        default: "2025"
      max_fixtures:
        description: "Recent matches per league to pull (costs 1 API request each)"
        required: false
        default: "600"
  schedule:
    - cron: "0 2 * * *"

jobs:
  ingest-and-score:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Ingest players/clubs/leagues
        env:
          FOOTBALL_API_KEY: ${{ secrets.FOOTBALL_API_KEY }}
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          python ingest.py --all-leagues --season ${{ github.event.inputs.season || '2025' }}

      - name: Ingest recent match stats
        env:
          FOOTBALL_API_KEY: ${{ secrets.FOOTBALL_API_KEY }}
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          python fixtures_ingest.py --all-leagues --season ${{ github.event.inputs.season || '2025' }} --max-fixtures ${{ github.event.inputs.max_fixtures || '600' }}

      - name: Recompute potential scores
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          python scoring_model.py --season ${{ github.event.inputs.season || '2025' }}