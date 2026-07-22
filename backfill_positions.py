"""
Backfills missing primary_position for players whose ingestion picked up
an empty statistics entry (e.g. zero appearances for their current club
this season) — even though a DIFFERENT competition entry in the same
API response might have real position data we never checked, since the
original ingestion only ever looked at statistics[0].

Costs 1 API request per missing player.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python backfill_positions.py --season 2025
"""

import os
import time
import argparse
import json
import subprocess
import psycopg2

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.environ.get("FOOTBALL_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
REQUEST_DELAY_SECONDS = 0.25


class RateLimitError(Exception):
    pass


def api_get(path, params=None):
    """Uses curl.exe directly via subprocess rather than Python's requests
    library — confirmed, validated fix for a real, repeated SSL certificate
    validation hang in Python's own networking stack on this machine.
    curl.exe works reliably every time; Python's requests library does not."""
    url = f"{API_BASE}/{path}"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"

    result = subprocess.run(
        ["curl.exe", "-s", "-H", f"x-apisports-key: {API_KEY}", url],
        capture_output=True, text=True, timeout=20,
    )
    if result.returncode != 0:
        raise RateLimitError(f"curl failed (exit code {result.returncode}): {result.stderr}")

    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise RateLimitError(f"Got a non-JSON response, likely a real network issue: {result.stdout[:200]}")

    errors = body.get("errors")
    if errors and isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
        raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def find_missing_position_players(conn, limit):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, external_id, full_name
            FROM players
            WHERE primary_position IS NULL
            ORDER BY id
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


def run(season, limit):
    conn = get_conn()
    players = find_missing_position_players(conn, limit)
    print(f"Found {len(players)} players missing a position.")

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

        # Check EVERY statistics entry (not just [0]) for a real position —
        # a player's current club might show zero appearances while a
        # different competition entry in the same response still has one.
        position = None
        for stat_entry in data[0].get("statistics", []):
            pos = stat_entry.get("games", {}).get("position")
            if pos:
                position = pos
                break

        if position:
            with conn.cursor() as cur:
                cur.execute("UPDATE players SET primary_position = %s WHERE id = %s", (position, db_id))
            conn.commit()
            updated += 1
        else:
            not_found += 1

        if i % 100 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} updated, {not_found} had no position on file)")

    print(f"\nDone. Updated {updated} players. {not_found} had no position available from the API.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--limit", type=int, default=3000)
    args = parser.parse_args()
    run(args.season, args.limit)