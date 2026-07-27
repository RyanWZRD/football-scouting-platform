"""
Ingests stadium/venue data for every tracked club. Confirmed directly
from API-Football's own documentation: the /teams?id=X response already
includes an embedded "venue" object (name, address, city, capacity,
surface, image) — no separate /venues call needed. One request per club.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python venues_ingest.py --limit 500
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
        # Clubs without a venue on record yet — external_id is required
        # since that's what /teams needs to look the club up.
        cur.execute("""
            SELECT cl.id, cl.external_id, cl.name
            FROM clubs cl
            LEFT JOIN club_venues cv ON cv.club_id = cl.id
            WHERE cv.id IS NULL AND cl.external_id IS NOT NULL
            LIMIT %s
        """, (limit,))
        clubs = cur.fetchall()

    print(f"Processing {len(clubs)} clubs missing venue data.")
    updated = 0
    no_data = 0

    for i, (club_id, external_id, club_name) in enumerate(clubs, 1):
        try:
            data = api_get("teams", {"id": external_id})
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} clubs updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not data or not data[0].get("venue"):
            no_data += 1
            continue

        venue = data[0]["venue"]
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO club_venues (club_id, external_id, name, address, city, capacity, surface, image_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (club_id) DO UPDATE SET
                    name = EXCLUDED.name, address = EXCLUDED.address, city = EXCLUDED.city,
                    capacity = EXCLUDED.capacity, surface = EXCLUDED.surface, image_url = EXCLUDED.image_url,
                    ingested_at = now()
            """, (
                club_id, str(venue.get("id")) if venue.get("id") else None, venue.get("name"),
                venue.get("address"), venue.get("city"), venue.get("capacity"),
                venue.get("surface"), venue.get("image"),
            ))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(clubs)} processed ({updated} with venue data found)")

    conn.close()
    print(f"Done. {updated} clubs had venue data recorded. {no_data} had none available.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.limit)