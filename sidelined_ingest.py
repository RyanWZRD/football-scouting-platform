"""
Ingests sidelined records — real data from API-Football's own dedicated
/sidelined endpoint. Genuinely broader than the existing player_injuries
table (a separate /injuries endpoint) — this one is documented to also
cover suspensions, not just injuries. Written defensively with .get()
fallbacks throughout since the exact field shape wasn't fully confirmed
from available documentation before building this.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python sidelined_ingest.py --limit 500
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
    if errors:
        if isinstance(errors, dict) and any("limit" in str(v).lower() for v in errors.values()):
            raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def run(limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.external_id
            FROM players p
            LEFT JOIN player_sidelined ps ON ps.player_id = p.id
            WHERE ps.id IS NULL AND p.external_id IS NOT NULL
            ORDER BY p.id
            LIMIT %s
        """, (limit,))
        players = cur.fetchall()

    print(f"Processing {len(players)} players missing sidelined data.")
    updated = 0
    no_data = 0

    for i, (player_id, external_id) in enumerate(players, 1):
        try:
            records = api_get("sidelined", {"player": external_id})
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not records:
            no_data += 1
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO player_sidelined (player_id, sidelined_type, start_date, end_date)
                    VALUES (%s, NULL, NULL, NULL)
                """, (player_id,))
            conn.commit()
            continue

        with conn.cursor() as cur:
            for entry in records:
                cur.execute("""
                    INSERT INTO player_sidelined (player_id, sidelined_type, start_date, end_date)
                    VALUES (%s, %s, %s, %s)
                """, (
                    player_id, entry.get("type"),
                    entry.get("start"), entry.get("end"),
                ))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} with real sidelined data found)")

    conn.close()
    print(f"Done. {updated} players had real sidelined data recorded. {no_data} had none available.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.limit)