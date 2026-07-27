"""
Ingests real transfer fee history — actual fees (e.g. "€45M"), or
"Free"/"Loan"/"N/A" where applicable. Confirmed real data from
API-Football's own dedicated /transfers endpoint. Genuinely different
from the existing player_club_transfers table, which is this project's
own match-based detection with no fee information at all.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python transfer_fees_ingest.py --limit 500
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
            LEFT JOIN player_transfer_history pth ON pth.player_id = p.id
            WHERE pth.id IS NULL AND p.external_id IS NOT NULL
            ORDER BY p.id
            LIMIT %s
        """, (limit,))
        players = cur.fetchall()

    print(f"Processing {len(players)} players missing transfer fee data.")
    updated = 0
    no_data = 0

    for i, (player_id, external_id) in enumerate(players, 1):
        try:
            transfers = api_get("transfers", {"player": external_id})
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not transfers or not transfers[0].get("transfers"):
            no_data += 1
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO player_transfer_history (player_id, transfer_date, fee_type, club_from, club_to)
                    VALUES (%s, NULL, NULL, NULL, NULL)
                """, (player_id,))
            conn.commit()
            continue

        with conn.cursor() as cur:
            for entry in transfers[0]["transfers"]:
                teams = entry.get("teams", {})
                cur.execute("""
                    INSERT INTO player_transfer_history (player_id, transfer_date, fee_type, club_from, club_to)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    player_id, entry.get("date"), entry.get("type"),
                    teams.get("out", {}).get("name"), teams.get("in", {}).get("name"),
                ))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} with real transfer data found)")

    conn.close()
    print(f"Done. {updated} players had real transfer fee data recorded. {no_data} had none available.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.limit)