"""
Ingests official, API-Football-computed club season stats: form string,
clean sheets, matches failed to score in, and the season's biggest win /
heaviest defeat. Confirmed from API-Football's own documentation
(teams/statistics endpoint) — league, season, and team are all required
parameters. Written defensively with .get() fallbacks throughout, since
the exact "biggest"/"clean_sheet" field shape wasn't fully visible in
available documentation snippets — a wrong assumption here should
produce a null field, not a crash.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python club_season_stats_ingest.py --season 2025 --limit 500
"""

import os
import time
import argparse
import requests
import psycopg2

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.environ.get("FOOTBALL_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
HEADERS = {"x-apisports-key": API_KEY}
REQUEST_DELAY_SECONDS = 0.25

# Same calendar-year league adjustment established in fixtures_ingest.py —
# these leagues' "current" season is season+1 in this project's global
# --season convention.
CALENDAR_YEAR_LEAGUE_IDS = {253, 262, 71}


class RateLimitError(Exception):
    pass


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {})
    if resp.status_code == 429:
        raise RateLimitError("Rate limit hit (HTTP 429).")
    resp.raise_for_status()
    resp.encoding = "utf-8"
    body = resp.json()
    errors = body.get("errors")
    if errors:
        if isinstance(errors, dict) and any("limit" in str(v).lower() for v in errors.values()):
            raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response")


def format_biggest(entry):
    """Biggest win/loss entries are typically {home, away} score strings
    like '4-0' — format defensively since the exact shape is unconfirmed."""
    if not entry:
        return None
    home = entry.get("home") if isinstance(entry, dict) else None
    away = entry.get("away") if isinstance(entry, dict) else None
    if home and away:
        return f"Home {home} / Away {away}"
    return str(entry) if entry else None


def run(season, limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.id, cl.external_id, cl.name, l.external_id AS league_external_id
            FROM clubs cl
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN club_season_stats css ON css.club_id = cl.id AND css.season = %s
            WHERE css.id IS NULL AND cl.external_id IS NOT NULL AND l.external_id IS NOT NULL
            LIMIT %s
        """, (str(season), limit))
        clubs = cur.fetchall()

    print(f"Processing {len(clubs)} clubs missing season {season} stats.")
    updated = 0
    no_data = 0

    for i, (club_id, club_external_id, club_name, league_external_id) in enumerate(clubs, 1):
        effective_season = season + 1 if int(league_external_id) in CALENDAR_YEAR_LEAGUE_IDS else season
        try:
            data = api_get("teams/statistics", {
                "team": club_external_id, "league": league_external_id, "season": effective_season,
            })
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} clubs updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not data or not data.get("fixtures", {}).get("played", {}).get("total"):
            no_data += 1
            continue

        goals = data.get("goals", {})
        clean_sheet = data.get("clean_sheet", {}).get("total") if isinstance(data.get("clean_sheet"), dict) else None
        failed_to_score = data.get("failed_to_score", {}).get("total") if isinstance(data.get("failed_to_score"), dict) else None
        biggest = data.get("biggest", {})

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO club_season_stats (club_id, season, form, clean_sheets, failed_to_score, biggest_win, biggest_loss)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (club_id, season) DO UPDATE SET
                    form = EXCLUDED.form, clean_sheets = EXCLUDED.clean_sheets,
                    failed_to_score = EXCLUDED.failed_to_score,
                    biggest_win = EXCLUDED.biggest_win, biggest_loss = EXCLUDED.biggest_loss,
                    ingested_at = now()
            """, (
                club_id, str(season), data.get("form"), clean_sheet, failed_to_score,
                format_biggest(biggest.get("wins")), format_biggest(biggest.get("loses")),
            ))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(clubs)} processed ({updated} with real stats found)")

    conn.close()
    print(f"Done. {updated} clubs had real season {season} stats recorded. {no_data} had none available.")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.season, args.limit)