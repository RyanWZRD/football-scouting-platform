"""
Backfills missing nationality for players — mostly "stub" players
discovered through match lineups (fixtures_ingest.py), which never
resolves nationality the way the original roster pull in ingest.py does.
Confirmed via the Data Quality Dashboard: ~7,523 players (36% of the
total pool) were missing this.

Costs 1 API request per missing player, same pattern as international
caps and historical seasons — meant to be run in batches across 1-2
days, not all at once.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python nationality_backfill.py --season 2025 --offset 0 --limit 3800
    python nationality_backfill.py --season 2025 --offset 3800 --limit 3800
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
    resp.encoding = "utf-8"
    body = resp.json()
    errors = body.get("errors")
    if errors and isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
        raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fix_mojibake(s):
    if not s:
        return s
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return s


def get_or_create_country(conn, name):
    if not name:
        return None
    clean_name = fix_mojibake(name)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM countries WHERE name = %s", (clean_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO countries (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", (clean_name,))
        row = cur.fetchone()
        if row:
            conn.commit()
            return row[0]
        cur.execute("SELECT id FROM countries WHERE name = %s", (clean_name,))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None


def run(season, offset, limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, external_id, full_name
            FROM players
            WHERE nationality_id IS NULL
            ORDER BY id
            OFFSET %s LIMIT %s
        """, (offset, limit))
        players = cur.fetchall()

    print(f"Processing {len(players)} players missing nationality (offset {offset}, limit {limit}).")

    updated = 0
    no_data = 0
    for i, (db_id, external_id, name) in enumerate(players, 1):
        try:
            data = api_get("players", {"id": external_id, "season": season})
        except RateLimitError as e:
            print(f"\nHit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            print("Just re-run with --offset 0 next time — this script's query already excludes anyone successfully updated, so the pool naturally shrinks each run. No offset tracking needed.")
            break

        if not data:
            no_data += 1
            continue

        nationality = data[0].get("player", {}).get("nationality")
        if not nationality:
            no_data += 1
            continue

        country_id = get_or_create_country(conn, nationality)
        if country_id is None:
            no_data += 1
            continue

        with conn.cursor() as cur:
            cur.execute("UPDATE players SET nationality_id = %s WHERE id = %s", (country_id, db_id))
        conn.commit()
        updated += 1

        if i % 200 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} updated)")

    print(f"\nDone. {updated} players had nationality backfilled. {no_data} had no data available.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=3800)
    args = parser.parse_args()
    run(args.season, args.offset, args.limit)