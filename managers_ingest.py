"""
Ingests real manager/coach data per club — a genuinely new data
dimension, never tracked before. Costs 1 API request PER CLUB (confirmed
via API-Football's own documentation: /coachs?team={id}), so full
coverage across all tracked clubs (~340) is a manageable one-time job,
not something needing multi-day batching the way international caps did.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python managers_ingest.py
"""

import os
import time
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


def run():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_id, name FROM clubs WHERE external_id IS NOT NULL ORDER BY id")
        clubs = cur.fetchall()

    print(f"Processing {len(clubs)} clubs.")
    updated = 0
    no_data = 0

    for i, (db_id, external_id, name) in enumerate(clubs, 1):
        try:
            data = api_get("coachs", {"team": external_id})
        except RateLimitError as e:
            print(f"\nHit the rate limit after {updated} clubs updated.")
            print(f"Actual error detail: {e}")
            print("Re-run the same command to continue — already-updated clubs are skipped naturally on conflict.")
            break

        if not data:
            no_data += 1
            continue

        coach = data[0]  # current coach — API-Football returns the active one first
        appointed_date = None
        for stint in coach.get("career", []):
            if stint.get("end") is None and stint.get("team", {}).get("id") == external_id:
                appointed_date = stint.get("start")
                break

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO club_managers (club_id, name, nationality, age, photo_url, appointed_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (club_id) DO UPDATE SET
                    name = EXCLUDED.name, nationality = EXCLUDED.nationality,
                    age = EXCLUDED.age, photo_url = EXCLUDED.photo_url,
                    appointed_date = EXCLUDED.appointed_date, ingested_at = now()
            """, (db_id, coach.get("name"), coach.get("nationality"), coach.get("age"),
                  coach.get("photo"), appointed_date))
        conn.commit()
        updated += 1

        if i % 50 == 0:
            print(f"  ...{i}/{len(clubs)} processed ({updated} managers found)")

    print(f"\nDone. {updated} clubs had a current manager recorded. {no_data} had none available.")
    conn.close()


if __name__ == "__main__":
    run()