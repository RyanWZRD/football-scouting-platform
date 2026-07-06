# Cross-League Football Scouting Platform

A scouting/analytics system covering leagues outside the traditional top 5
(Eredivisie, Liga Portugal, Süper Lig, Brasileirão, J1 League, MLS, Scottish
Premiership, Ligue 2), combining statistical percentile scoring with an
age curve and a slot for human scout judgement.

**Live:**
- Dashboard: https://scoutindex.netlify.app/
- API: https://football-scouting-api-so8h.onrender.com

## Architecture

```
API-Football (data source, free tier)
        │
        ▼
  ingest.py            → players, clubs, leagues        (Postgres / Supabase)
  fixtures_ingest.py    → matches, player_match_stats     (Postgres / Supabase)
        │
        ▼
  scoring_model.py      → player_potential_scores
        │
        ▼
  api.py (FastAPI, deployed on Render)
        │
        ▼
  index.html (deployed on Netlify) — public dashboard
```

## Files

| File | Purpose |
|---|---|
| `schema.sql` | Full Postgres schema — run once against a fresh database |
| `ingest.py` | Pulls player/club/league data from API-Football |
| `fixtures_ingest.py` | Pulls recent match results + per-player match stats |
| `scoring_model.py` | Computes the potential index from ingested stats |
| `api.py` | FastAPI layer serving the database as JSON |
| `index.html` | Standalone dashboard (no build step) — deploy as a static site |
| `requirements.txt` | Python dependencies for all the above |

## Setup (from scratch)

1. Create a Supabase (or any Postgres) project, run `schema.sql` against it
2. Get an API-Football key (free tier: 100 requests/day, 10/minute, pagination capped at page 3)
3. Set environment variables:
   ```
   FOOTBALL_API_KEY=...
   DATABASE_URL=postgresql://...
   ```
4. `pip install -r requirements.txt`
5. `python ingest.py --all-leagues --season 2023`
6. `python fixtures_ingest.py --all-leagues --season 2023 --max-fixtures 5`
7. `python scoring_model.py --season 2023`
8. Deploy `api.py` (Render, or any host that runs Python) with `DATABASE_URL`
   and optionally `API_ACCESS_KEY` set as environment variables
9. Set `API_BASE_URL` (and `API_KEY` if you set one) at the top of `index.html`,
   deploy it as a static site (Netlify Drop is the fastest option)

## Known limitations (current state)

- **Season**: using 2023 data — the free API tier doesn't reliably cover the
  current season for most non-top5 leagues. Upgrading the API plan and
  re-running the same scripts with `--season 2025` (or current year) is a
  drop-in change, nothing else needs to change.
- **Match coverage**: only a few recent matches per league are ingested
  (`--max-fixtures`), not a full season — each match costs one API request
  via `/fixtures/players`, so a full season would exceed the free daily quota.
  `scoring_model.py`'s minimum-minutes threshold is set low (180 min) to
  match this; raise it back up as more matches get ingested over time.
- **Player age for "stub" players**: players discovered only through match
  lineups (not the original `/players` pull) don't have a birthdate, so their
  age_adjustment defaults to neutral (50) rather than a real value.
- **Qualitative scoring**: `scout_notes` table is currently empty — every
  player's qualitative_component defaults to neutral until real notes are added.
- **API security**: protected with a simple shared-secret header
  (`X-API-Key`), not full authentication. Fine for a personal/demo project,
  not sufficient if this became a multi-user product.
- **Render free tier**: spins down after 15 min of inactivity; first request
  after a quiet period takes 20-30 seconds to wake up.

## Rate limit notes (free API-Football tier)

- 100 requests/day, resets 00:00 UTC
- 10 requests/minute (scripts pace themselves at 1 request per 7 seconds)
- Pagination capped at page 3 for endpoints like `/players`
- Errors sometimes come back as HTTP 200 with an `errors` field in the body,
  not HTTP 429 — both `ingest.py` and `fixtures_ingest.py` check for this
