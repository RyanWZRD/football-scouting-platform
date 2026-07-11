"""
Ingests real international caps (national team appearances) for tracked
players — genuinely new data, never captured before. Costs 1 API request
PER PLAYER (the per-player /players?id=X endpoint is the only one that
returns international statistics; the team-based bulk endpoint used for
regular ingestion does NOT include them — confirmed by direct testing).

Given the ~7,169-player pool, this is meant to be run in TWO batches
across two days to avoid colliding with nightly automation's quota:

    Day 1:
        python international_ingest.py --season 2025 --offset 0 --limit 3600
    Day 2:
        python international_ingest.py --season 2025 --offset 3600 --limit 3600

Filtering logic: a statistics entry counts as a genuine international cap
if its team name does NOT match any club we track (eliminates all club
competitions, including any former loan club), AND the competition name
doesn't contain "Clubs" (eliminates API-Football's "Friendlies Clubs" —
club-level pre-season friendlies that are confusingly also tagged
country="World", which would otherwise look international at a glance).

This is a reasonable, defensible heuristic but not mathematically
guaranteed to be perfect on 100% of edge cases — spot-check a few known
international players (e.g. search for someone you know plays for their
country) after running to sanity-check the results before fully trusting it.
"""

import os
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values

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


def run(season, offset, limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.external_id, p.full_name
            FROM players p
            JOIN player_potential_scores pps ON pps.player_id = p.id
            WHERE pps.season = %s
            ORDER BY p.id
            OFFSET %s LIMIT %s
        """, (str(season), offset, limit))
        players = cur.fetchall()

        cur.execute("SELECT name FROM clubs")
        known_club_names = {row[0] for row in cur.fetchall()}

    print(f"Processing {len(players)} players (offset {offset}, limit {limit}).")

    updated = 0
    no_caps = 0
    for i, (db_id, external_id, name) in enumerate(players, 1):
        try:
            data = api_get("players", {"id": external_id, "season": season})
        except RateLimitError as e:
            print(f"\nHit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            print(f"Re-run with --offset {offset + i - 1} to continue from here.")
            break

        if not data:
            no_caps += 1
            continue

        caps_by_key = {}
        for stat in data[0].get("statistics", []):
            team_name = stat.get("team", {}).get("name")
            league_name = stat.get("league", {}).get("name", "")
            if not team_name or team_name in known_club_names:
                continue  # a real tracked club — not international
            if "Clubs" in league_name:
                continue  # e.g. "Friendlies Clubs" — club friendlies, not international duty

            games = stat.get("games", {})
            goals = stat.get("goals", {})
            key = (team_name, league_name)
            # A player can occasionally have TWO statistics entries for the
            # same team+competition (e.g. a group-stage/playoff split that
            # API-Football tags identically) — keep whichever has more
            # appearances rather than trying to insert both, which Postgres
            # correctly rejects as a duplicate within one batch statement.
            appearances = games.get("appearences") or 0
            if key not in caps_by_key or appearances > (caps_by_key[key][3] or 0):
                caps_by_key[key] = (
                    db_id, team_name, league_name,
                    appearances, goals.get("total"), goals.get("assists"),
                    games.get("minutes"), str(season),
                )
        caps_rows = list(caps_by_key.values())

        if caps_rows:
            try:
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO player_international_caps
                            (player_id, team_name, competition_name, appearances, goals, assists, minutes_played, season)
                        VALUES %s
                        ON CONFLICT (player_id, team_name, competition_name, season) DO UPDATE SET
                            appearances = EXCLUDED.appearances,
                            goals = EXCLUDED.goals,
                            assists = EXCLUDED.assists,
                            minutes_played = EXCLUDED.minutes_played,
                            ingested_at = now()
                    """, caps_rows)
                conn.commit()
                updated += 1
            except Exception as e:
                conn.rollback()
                print(f"  skipped {name} (id {external_id}) due to a data issue: {e}")
                no_caps += 1
        else:
            no_caps += 1

        if i % 200 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} with real international caps found)")

    print(f"\nDone. {updated} players had genuine international caps recorded. {no_caps} had none (most players never play internationally — this is expected, not an error).")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=3600)
    args = parser.parse_args()
    run(args.season, args.offset, args.limit)