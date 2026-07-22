"""
Injury ingestion: pulls current injury reports per league from
API-Football's /injuries endpoint.

NOTE ON DATA QUALITY: this endpoint returns reported injuries but doesn't
always cleanly distinguish "still out" from "since recovered" — treat
results as "most recent known injury report" for a player, not a
guaranteed real-time fitness status. Good context for scouting, not a
substitute for checking current news on a specific player.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python injuries_ingest.py --all-leagues --season 2025
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
REQUEST_DELAY_SECONDS = 0.25


class RateLimitError(Exception):
    pass


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {}, timeout=15)
    if resp.status_code == 429:
        raise RateLimitError("Rate limit hit (HTTP 429).")
    resp.raise_for_status()
    # Force UTF-8 explicitly rather than trusting requests' auto-detected
    # encoding — see ingest.py for the full explanation of this bug.
    resp.encoding = "utf-8"
    body = resp.json()
    errors = body.get("errors")
    if errors and isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
        raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def ingest_injuries_for_league(conn, league_external_id, season):
    data = api_get("injuries", {"league": league_external_id, "season": season})
    if not data:
        return 0

    ext_ids = list({str(entry["player"]["id"]) for entry in data})
    with conn.cursor() as cur:
        cur.execute("SELECT external_id, id FROM players WHERE external_id = ANY(%s)", (ext_ids,))
        id_map = {row[0]: row[1] for row in cur.fetchall()}

    rows = []
    for entry in data:
        ext_id = str(entry["player"]["id"])
        db_id = id_map.get(ext_id)
        if db_id is None:
            continue
        player = entry.get("player", {})
        fixture = entry.get("fixture", {})
        rows.append((
            db_id,
            player.get("type"),
            player.get("reason"),
            fixture.get("date", "")[:10] if fixture.get("date") else None,
        ))

    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO player_injuries (player_id, injury_type, reason, reported_date)
                VALUES %s
            """, rows)
        conn.commit()
    return len(rows)


def run(league_ids, season):
    conn = get_conn()
    completed = []
    for league_id in league_ids:
        print(f"Fetching injuries for league {league_id} / season {season} ...")
        try:
            n = ingest_injuries_for_league(conn, league_id, season)
            completed.append(league_id)
            print(f"  {n} injury reports recorded")
        except RateLimitError as e:
            print(f"\nHit the rate limit while on league {league_id}: {e}")
            print(f"Leagues completed this run: {completed}")
            print(f"Remaining: {[l for l in league_ids if l not in completed]}")
            break
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=int, action="append")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--all-leagues", action="store_true")
    args = parser.parse_args()

    LEAGUE_IDS = [
        39, 140, 78, 135, 61,
        88, 94, 203, 71, 98, 253, 179, 62,
        40, 144, 262, 128,
    ]

    ids = args.league if args.league else (LEAGUE_IDS if args.all_leagues else [])
    if not ids:
        parser.error("Provide --league <id> (repeatable) or --all-leagues")

    run(ids, args.season)