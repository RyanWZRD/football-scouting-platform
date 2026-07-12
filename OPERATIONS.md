# Operations Runbook

A single reference for everything that needs running, how often, and why.
Quick-reference table first, full detail below.

## Quick Reference

| Task | Command | Frequency |
|---|---|---|
| Nightly pipeline | *(automatic)* | Every night, 2am UTC |
| Transfer news | *(automatic)* | Every 5 minutes |
| Manager tracking | `python managers_ingest.py` | Monthly |
| International caps | `python international_ingest.py --season 2025 --offset 0 --limit 3600` | After each international break (~every 6-8 weeks) |
| Historical seasons | `python historical_seasons_ingest.py --season 2024 --offset 0 --limit 3600` | Once (per season, ever) |
| Age/position backfills | `python backfill_ages.py --season 2025` / `python backfill_positions.py --season 2025` | Quarterly maintenance sweep |
| Quota check | `curl.exe -H "x-apisports-key: KEY" https://v3.football.api-sports.io/status` | Before any manual run |
| Season coverage check | *(manual, see below)* | Once, ~August 2026 |

---

## Fully Automatic — Nothing To Do

These run on their own via GitHub Actions. Check the **Actions** tab on GitHub occasionally to confirm they're green, but no command to run.

- **Nightly pipeline** (`ingest.yml`) — 2am UTC daily: ingest players → fixtures → fix club assignments → scoring. Uses ~800-900 requests/night.
- **Transfer news** (`refresh-transfer-news.yml`) — every 5 minutes, all day, regardless of whether the app is open.

---

## Regular Cadence — Manual, But Predictable

### Manager/Coach Tracking
```powershell
python managers_ingest.py
```
**Frequency: monthly.** Managers don't change often — a sacking/appointment is irregular, not scheduled. Cheap (~365 requests), so no harm running it more often if you want extra confidence after a big managerial news week.

### International Caps
```powershell
python international_ingest.py --season 2025 --offset 0 --limit 3600
python international_ingest.py --season 2025 --offset 3600 --limit 3600
```
**Frequency: after each international break**, not on a fixed monthly schedule — caps only accumulate when players actually play internationally, which happens in windows roughly every 6-8 weeks in the football calendar. Running it between windows won't find much new. ~7,169 requests total for full coverage, so expect to split across the two commands above (or across days if quota is tight).

### Age/Position Backfills
```powershell
python backfill_ages.py --season 2025
python backfill_positions.py --season 2025
```
**Frequency: quarterly**, as a maintenance sweep. These mainly fixed historical gaps from early ingestion — new players entering the system now already get this data from the regular nightly pipeline, so this is genuinely low-priority, just a periodic safety net.

---

## One-Time (Per Season)

### Historical Season Data
```powershell
python historical_seasons_ingest.py --season 2024 --offset 0 --limit 3600
python historical_seasons_ingest.py --season 2024 --offset 3600 --limit 3600
```
Run once for full 2024 coverage — a completed season's data doesn't change afterward. Only revisit if you want to go further back (2023, 2022...), which would be its own one-time run per season, not a repeat.

---

## Calendar Reminder (Not a Script)

**~August 2026, before the new season starts:** verify API-Football season 2026 (2026-27) has `coverage.players=true` and real fixture data populated, so summer transfers (e.g. Elliot Anderson to Man City) are captured correctly and squad data refreshes for the new season. As of the last check (July 7, 2026), season 2026 existed but coverage wasn't yet live — this needs a fresh check, not an assumption it's ready. Also renew the API-Football Pro key around the same time (expires 2026-08-07).

---

## Before Any Manual Run — Check Quota

```powershell
curl.exe -H "x-apisports-key: 2c562e175b9355cc1355b57238ed0612" https://v3.football.api-sports.io/status
```
Look at `requests.current` vs `requests.limit_day` (7,500/day). Nightly automation already uses ~800-900 of that on its own, so budget manual runs around that baseline, not against the full 7,500.

If a script hits the rate limit mid-run, it will print exactly where to resume from (an `--offset` value) — nothing is lost by stopping partway through.
