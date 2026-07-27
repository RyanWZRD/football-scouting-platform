"""
Ingests match event timelines (goals/cards/subs with exact minutes) and
lineup formations for finished matches. Confirmed from API-Football's
own documentation as two SEPARATE endpoints (not one combined call) —
this genuinely costs 2 requests per match, not 1. Be mindful of quota
when choosing --limit.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python match_events_ingest.py --limit 250
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


def get_club_id_by_name(cur, name):
    cur.execute("SELECT id FROM clubs WHERE name = %s LIMIT 1", (name,))
    row = cur.fetchone()
    return row[0] if row else None


def run(limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.id, m.external_id
            FROM matches m
            LEFT JOIN match_events me ON me.match_id = m.id
            WHERE m.status = 'finished' AND me.id IS NULL AND m.external_id IS NOT NULL
            ORDER BY m.match_date DESC
            LIMIT %s
        """, (limit,))
        matches = cur.fetchall()

    print(f"Processing {len(matches)} finished matches missing event/lineup data.")
    updated = 0
    no_data = 0

    for i, (match_id, external_id) in enumerate(matches, 1):
        try:
            events = api_get("fixtures/events", {"fixture": external_id})
            lineups = api_get("fixtures/lineups", {"fixture": external_id})
        except RateLimitError as e:
            print(f"Hit the rate limit after {updated} matches updated.")
            print(f"Actual error detail: {e}")
            conn.close()
            return

        if not events and not lineups:
            no_data += 1
            continue

        with conn.cursor() as cur:
            for ev in events:
                cur.execute("""
                    INSERT INTO match_events (match_id, minute, extra_minute, event_type, detail, player_name, assist_name, club_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    match_id, ev.get("time", {}).get("elapsed"), ev.get("time", {}).get("extra"),
                    ev.get("type"), ev.get("detail"),
                    ev.get("player", {}).get("name"), ev.get("assist", {}).get("name"),
                    ev.get("team", {}).get("name"),
                ))
            for lu in lineups:
                club_id = get_club_id_by_name(cur, lu.get("team", {}).get("name"))
                if club_id:
                    cur.execute("""
                        INSERT INTO match_lineups (match_id, club_id, formation)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (match_id, club_id) DO UPDATE SET formation = EXCLUDED.formation
                    """, (match_id, club_id, lu.get("formation")))
        conn.commit()
        updated += 1

        if i % 25 == 0:
            print(f"  ...{i}/{len(matches)} processed ({updated} with data found)")

    conn.close()
    print(f"Done. {updated} matches had event/lineup data recorded. {no_data} had none available.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=250)
    args = parser.parse_args()
    run(args.limit)