"""
Ingestion pipeline: API-Football -> Postgres (schema.sql)

Swap-friendly by design: everything hits API_BASE / API_KEY and a couple
of field-mapping functions. Pointing this at Wyscout or StatsBomb later
means rewriting `map_player`, `map_match`, `map_player_match_stats` —
the DB schema and everything downstream (scoring model, dashboard) stays.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=postgresql://user:pass@host/dbname
    python ingest.py --league 88 --season 2025          # Eredivisie example
    python ingest.py --all-leagues --season 2025         # every tracked league

Requires: pip install requests psycopg2-binary python-dotenv
"""

import os
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.environ.get("FOOTBALL_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

HEADERS = {"x-apisports-key": API_KEY}

# Pro plan: 7,500/day, 300 requests/minute. 0.25s between calls keeps us
# comfortably under that (~4 req/sec = 240/min) with headroom to spare.
REQUEST_DELAY_SECONDS = 0.25


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {})
    if resp.status_code == 429:
        raise RateLimitError("Daily or per-minute request limit hit (HTTP 429).")
    resp.raise_for_status()
    body = resp.json()
    errors = body.get("errors")
    if errors:
        error_text = str(errors).lower()
        if "page parameter" in error_text:
            # Free tier caps pagination at page 3 — not a rate limit, just stop paging.
            raise PageLimitReached()
        if isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
            raise RateLimitError(f"Rate limit reported in response body: {errors}")
        print(f"    API returned errors (non-fatal, treating as no data): {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


class RateLimitError(Exception):
    pass


class PageLimitReached(Exception):
    pass


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def upsert_league(conn, league_id, season):
    data = api_get("leagues", {"id": league_id, "season": season})
    if not data:
        return None
    entry = data[0]
    league = entry["league"]
    country = entry["country"]
    is_top5 = league["name"] in {
        "Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO countries (name, fifa_code) VALUES (%s, %s)
            ON CONFLICT DO NOTHING RETURNING id
            """,
            (country["name"], country.get("code")),
        )
        cur.execute("SELECT id FROM countries WHERE name = %s", (country["name"],))
        country_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO leagues (external_id, name, country_id, tier, is_top5, season)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE SET season = EXCLUDED.season
            RETURNING id
            """,
            (str(league["id"]), league["name"], country_id, 1, is_top5, str(season)),
        )
        conn.commit()
        return cur.fetchone()[0]


def upsert_club(conn, team, db_league_id):
    if not team or not team.get("id"):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO clubs (external_id, name, league_id, logo_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE SET
                name = EXCLUDED.name,
                league_id = EXCLUDED.league_id
            RETURNING id
            """,
            (str(team["id"]), team["name"], db_league_id, team.get("logo")),
        )
        conn.commit()
        return cur.fetchone()[0]


def upsert_players_for_league(conn, league_external_id, season, db_league_id):
    page = 1
    while True:
        try:
            data = api_get("players", {"league": league_external_id, "season": season, "page": page})
        except PageLimitReached:
            print(f"    reached free-tier page limit (page {page}) — stopping pagination for this league")
            break
        if not data:
            break
        for entry in data:
            p = entry["player"]
            stats = entry["statistics"][0] if entry["statistics"] else {}
            team = stats.get("team", {})
            db_club_id = upsert_club(conn, team, db_league_id)
            if db_club_id is None:
                print(f"    warning: no club id for player {p.get('name')} — team data: {team}")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO players
                        (external_id, full_name, date_of_birth, primary_position, current_club_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (external_id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        current_club_id = EXCLUDED.current_club_id,
                        updated_at = now()
                    """,
                    (str(p["id"]), p["name"], p.get("birth", {}).get("date"),
                     stats.get("games", {}).get("position"), db_club_id),
                )
            conn.commit()
        if page >= data.__len__() and len(data) < 20:
            break
        page += 1
        if page > 50:  # safety cap for free-tier pagination
            break


def run(league_ids, season):
    conn = get_conn()
    completed = []
    for league_id in league_ids:
        print(f"Syncing league {league_id} / season {season} ...")
        try:
            db_league_id = upsert_league(conn, league_id, season)
            if db_league_id is None:
                print(f"  no data for league {league_id}, skipping")
                continue
            upsert_players_for_league(conn, league_id, season, db_league_id)
            completed.append(league_id)
            print(f"  done: league {league_id}")
        except RateLimitError as e:
            print(f"\nHit the API rate limit while on league {league_id}.")
            print(f"Actual error detail: {e}")
            print(f"Leagues completed this run: {completed}")
            print(f"Remaining, not yet done: {[l for l in league_ids if l not in completed]}")
            print("Wait for your daily quota to reset, then re-run with --all-leagues —")
            print("already-synced players will just be updated, not duplicated.")
            break
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=int, action="append", help="API-Football league id, repeatable")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--all-leagues", action="store_true",
                         help="Use the curated league list in LEAGUE_IDS below (top 5 + talent-pipeline leagues)")
    args = parser.parse_args()

    # Now on Pro tier (7,500 req/day) — expanded to include the top 5 plus
    # a wider set of leagues known for developing talent that moves up to
    # bigger leagues. Add/remove IDs freely as priorities change.
    LEAGUE_IDS = [
        # Top 5
        39, 140, 78, 135, 61,        # Premier League, La Liga, Bundesliga, Serie A, Ligue 1
        # Original non-top5 set
        88, 94, 203, 71, 98, 253, 179, 62,
        # Eredivisie, Liga Portugal, Süper Lig, Brasileirão, J1 League, MLS, Scottish Prem, Ligue 2
        # Additional talent-pipeline leagues
        40, 144, 262, 128,           # Championship (ENG 2nd tier), Belgian Pro League, Liga MX, Argentine Liga Profesional
    ]

    ids = args.league if args.league else (LEAGUE_IDS if args.all_leagues else [])
    if not ids:
        parser.error("Provide --league <id> (repeatable) or --all-leagues")

    run(ids, args.season)