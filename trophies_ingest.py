"""
Ingests career trophy history (league titles, cup wins, runner-up
finishes) for tracked players. Confirmed real data from API-Football's
own dedicated /trophies endpoint. Written defensively — the exact field
names for the "place" value weren't fully confirmed from available
documentation, so this tries a couple of reasonable field name
variants rather than assuming one and silently failing.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python trophies_ingest.py --limit 500
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


def extract_trophy_fields(entry):
    """Defensive extraction — tries a couple of reasonable field name
    variants for the 'place' value, since exact naming wasn't fully
    confirmed from available documentation. Returns None values rather
    than crashing if the shape genuinely differs from expectations."""
    league = entry.get("league")
    country = entry.get("country")
    season = entry.get("season")
    place = entry.get("place") or entry.get("result") or entry.get("status")
    return league, country, season, place


def run(limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.external_id
            FROM players p
            LEFT JOIN player_trophies pt ON pt.player_id = p.id
            WHERE pt.id IS NULL AND p.external_id IS NOT NULL
            ORDER BY p.id
            LIMIT %s
        """, (limit,))
        players = cur.fetchall()

    print(f"Processing {len(players)} players missing trophy data.")
    updated = 0
    no_data = 0

    for i, (player_id, external_id) in enumerate(players, 1):
        try:
            trophies = api_get("trophies", {"player": external_id})
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not trophies:
            no_data += 1
            # Still mark as processed so we don't re-check every run —
            # a genuine "no trophies" is a valid, real result.
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO player_trophies (player_id, league_name, country, season, place)
                    VALUES (%s, NULL, NULL, NULL, NULL)
                """, (player_id,))
            conn.commit()
            continue

        with conn.cursor() as cur:
            for entry in trophies:
                league, country, season, place = extract_trophy_fields(entry)
                cur.execute("""
                    INSERT INTO player_trophies (player_id, league_name, country, season, place)
                    VALUES (%s, %s, %s, %s, %s)
                """, (player_id, league, country, str(season) if season else None, place))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} with real trophy data found)")

    conn.close()
    print(f"Done. {updated} players had real trophy data recorded. {no_data} had none available.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.limit)