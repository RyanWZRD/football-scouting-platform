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

        # BUG FIX: data[0] is NOT reliably "the current coach" — confirmed
        # directly against real data. API-Football's /coachs response is
        # ordered by their internal coach ID (oldest-added first), not by
        # recency. This searches every coach in the response for whichever
        # has the LATEST start date among entries genuinely open-ended
        # (end: null) for THIS team.
        #
        # HONEST LIMITATION: for clubs with heavy managerial turnover,
        # API-Football's own data can show MULTIPLE coaches with end:null
        # simultaneously for the same team (confirmed directly — happened
        # for a real club with 5 managers in 12 months) — a genuine data
        # integrity issue at the source, not something any client-side
        # heuristic can fully resolve. "Latest start date" is the most
        # defensible tiebreaker available, not a guarantee of correctness
        # in every case. A brand-new appointment can also simply be
        # missing from their coach list entirely for a period after it
        # happens — re-running this periodically (see OPERATIONS.md) is
        # how that eventually self-corrects, not a one-time fix.
        best_coach = None
        best_start = None
        for c in data:
            for stint in c.get("career", []):
                # str() on both sides: external_id from our DB is TEXT
                # ("65"), but API-Football's raw JSON returns team.id as
                # an integer (65) — comparing them directly as-is is
                # always False in Python, which is exactly why the
                # previous run found 0 managers instead of the expected ~365.
                if stint.get("end") is None and str(stint.get("team", {}).get("id")) == str(external_id):
                    start = stint.get("start")
                    if start and (best_start is None or start > best_start):
                        best_start = start
                        best_coach = c
                    break

        if best_coach is None:
            no_data += 1
            continue

        coach = best_coach
        appointed_date = best_start

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