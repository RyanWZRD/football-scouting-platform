"""
Backfills missing date_of_birth for "stub" players — anyone discovered
only through match-lineup data (not the original squad pull), who never
got a full bio lookup. Costs 1 API request per missing player.

Only targets players with real match minutes (i.e. genuinely relevant,
not 0-minute roster noise) — run this AFTER a fixtures_ingest.py pass
so minutes data is populated and this filter is meaningful.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python backfill_ages.py --season 2025
    python backfill_ages.py --season 2025 --limit 500   # do it in batches
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


class RateLimitError(Exception):
    pass


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {})
    if resp.status_code == 429:
        raise RateLimitError("Rate limit hit (HTTP 429).")
    resp.raise_for_status()
    body = resp.json()
    errors = body.get("errors")
    if errors and isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
        raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def find_missing_age_players(conn, limit):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.external_id, p.full_name
            FROM players p
            JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE p.date_of_birth IS NULL
            GROUP BY p.id, p.external_id, p.full_name
            HAVING SUM(pms.minutes_played) > 0
            ORDER BY p.id
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


def run(season, limit):
    conn = get_conn()
    players = find_missing_age_players(conn, limit)
    print(f"Found {len(players)} players missing a birth date (with real minutes).")

    updated = 0
    not_found = 0
    for i, (db_id, external_id, name) in enumerate(players, 1):
        try:
            data = api_get("players", {"id": external_id, "season": season})
        except RateLimitError as e:
            print(f"\nHit the rate limit after {updated} updates.")
            print(f"Actual error detail: {e}")
            print(f"Re-run the same command to continue — already-updated players are skipped naturally.")
            break

        if not data:
            not_found += 1
            continue

        dob = data[0].get("player", {}).get("birth", {}).get("date")
        if dob:
            with conn.cursor() as cur:
                cur.execute("UPDATE players SET date_of_birth = %s WHERE id = %s", (dob, db_id))
            conn.commit()
            updated += 1
        else:
            not_found += 1

        if i % 100 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} updated, {not_found} had no birth date on file)")

    print(f"\nDone. Updated {updated} players. {not_found} had no birth date available from the API.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--limit", type=int, default=3000,
                         help="Max players to process this run (default 3000, covers all of them in one go)")
    args = parser.parse_args()
    run(args.season, args.limit)