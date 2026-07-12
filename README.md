# Cross-League Football Scouting Platform

A cross-league scouting and analytics platform covering 17 leagues — the
top 5 European leagues plus talent-pipeline leagues most scouting tools
ignore (Eredivisie, Süper Lig, Belgian Pro League, MLS, Argentine Liga
Profesional, Brazilian Serie A, J1 League, and more). Combines real
ingested match data with a transparent scoring model, structured human
scouting input, and a genuine layer of cross-feature analytical
intelligence — not just a stats browser.

**Live:**
- Dashboard: https://ryanwzrd.github.io/football-scouting-platform/
- API: https://football-scouting-api-so8h.onrender.com

## Architecture

```
API-Football (Pro tier, 7,500 req/day)
        │
        ▼
  ingest.py               → players, clubs, leagues, nationalities
  fixtures_ingest.py       → matches, player_match_stats, upcoming fixtures
  fix_club_assignments.py  → corrects club assignments using real match evidence
        │
        ▼
  scoring_model.py         → player_potential_scores + trend history
        │
        ▼
  api.py (FastAPI, deployed on Render)
        │
        ▼
  index.html (GitHub Pages) — public dashboard

Automated via GitHub Actions:
  ingest.yml                 — nightly full pipeline (2am UTC)
  refresh-transfer-news.yml  — transfer news, every 5 minutes, independent of the main pipeline
```

## Data sources

Beyond API-Football, the platform pulls in several free external sources
for genuine context beyond raw stats:
- **Wikipedia** — real biography summaries, on-demand, cached permanently
- **Google News RSS** — live news per player, and a background-refreshed general transfer feed
- **YouTube Data API** — real embedded highlight videos (needs a `YOUTUBE_API_KEY`, free tier ~100 searches/day, cached)
- **REST Countries** — real flag icons for international caps, cached

## Files

| File | Purpose |
|---|---|
| `schema.sql` | Full Postgres schema — run once against a fresh database |
| `ingest.py` | Players/clubs/leagues, per-club (not per-league — avoids historical bloat), with nationality resolution |
| `fixtures_ingest.py` | Match results, per-player match stats, and upcoming fixtures |
| `fix_club_assignments.py` | Corrects `current_club_id` using real match-minutes evidence, not roster-pull ordering |
| `scoring_model.py` | Computes the potential index; prefers structured scout ratings over free-text notes when available |
| `backfill_ages.py` / `backfill_positions.py` | One-off backfills for players missing birth date / position from initial ingestion |
| `injuries_ingest.py` | Injury reports per league (whole-season history; recency filtering happens at query time) |
| `international_ingest.py` | Real national team caps — costs 1 request/player, run in offset/limit batches |
| `transfer_news_refresh.py` | Fetches and caches general transfer news, run every 5 minutes by its own workflow |
| `api.py` | FastAPI layer — the whole application's logic lives here |
| `index.html` | Standalone dashboard (no build step), deployed as a static site |
| `requirements.txt` | Python dependencies |

## Setup (from scratch)

1. Create a Supabase (or any Postgres) project, run `schema.sql` against it
2. Get an API-Football key (Pro tier recommended — 7,500 req/day; the free
   tier's low daily cap and page-3 pagination limit will not sustain real
   17-league coverage)
3. Set environment variables:
   ```
   FOOTBALL_API_KEY=...
   DATABASE_URL=postgresql://...
   ```
4. `pip install -r requirements.txt`
5. `python ingest.py --all-leagues --season 2025`
6. `python fixtures_ingest.py --all-leagues --season 2025 --max-fixtures 600`
7. `python fix_club_assignments.py --season 2025`
8. `python scoring_model.py --season 2025`
9. Deploy `api.py` (Render, or any Python host) with `DATABASE_URL`,
   `API_ACCESS_KEY` (shared-secret header), `FOOTBALL_API_KEY` (for
   on-demand match events and live scores), and `YOUTUBE_API_KEY`
   (optional — highlights won't work without it, but everything else will)
10. Set `API_BASE_URL` (and `API_KEY` if set) at the top of `index.html`,
    deploy as a static site (GitHub Pages works well)
11. Set up the two GitHub Actions workflows (`ingest.yml` nightly,
    `refresh-transfer-news.yml` every 5 minutes) with `DATABASE_URL` and
    `FOOTBALL_API_KEY` as repo secrets

## Known limitations (honest, current)

- **No xG/xA, no progressive passes/carries**: not available in
  API-Football's basic stats tier at the current subscription level.
- **No sub-positions**: API-Football's position field only ever returns
  the 4 broad categories (Goalkeeper/Defender/Midfielder/Attacker) —
  confirmed directly against real data (even Neymar's own statistics never
  return anything finer than "Attacker"). Tactical Archetype Classification
  exists specifically to approximate this gap using real stat patterns.
- **International caps are a one-off batch job**, not nightly — costs 1
  API request per player, so it's run manually in offset/limit batches,
  not part of the automated pipeline.
- **Favorited clubs live only in the browser** (localStorage), not the
  server — this means the Transfer Centre's personalized "Your Clubs"
  feed and similar features can't be pre-cached in the background the
  way the general feed can.
- **League-Adjusted Rating is deliberately conservative**: only ever
  deflates a score for a weaker league, never inflates one for a
  stronger league — a defensible simplification, not a rigorously
  validated model.
- **Tactical Archetype Classification is rule-based, not ML** — transparent
  and explainable by design, but the exact percentile thresholds are a
  reasonable first pass, not something empirically tuned against outcomes.
- **"Ask the Index" (natural-language SQL queries via Claude) exists in
  `api.py` but is dormant** — no `ANTHROPIC_API_KEY` configured, pending
  a decision on whether the (genuinely small) per-query cost is worth it.
- **Render free tier spins down after 15 min of inactivity** — first
  request after a quiet period takes 20-30 seconds to wake up.

## Rate limit notes (API-Football Pro tier)

- 7,500 requests/day, resets 00:00 UTC
- ~300 requests/minute (scripts self-pace at ~4 req/sec)
- Nightly automation uses roughly 800-900 requests/night
- Errors sometimes come back as HTTP 200 with an `errors` field in the
  body, not HTTP 429 — every ingestion script checks for this explicitly
