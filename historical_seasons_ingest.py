"""
Ingests real historical season data for players already tracked — turns
Trajectory Projection from "a few weeks of nightly tracking" into genuine
multi-year career snapshots. Costs 1 API request PER PLAYER (same
per-player endpoint used for international caps and backfills), so this
is meant to be run in batches across multiple days, same pattern as
international_ingest.py.

HONEST SCOPE NOTE: this stores real season TOTALS (goals, assists,
minutes, average rating) for each player, aggregated across their genuine
competitive matches that season (excluding friendlies). It does NOT
compute a fully re-normalized potential_index for that historical season
— doing that properly would require the ENTIRE player population for
that season to percentile-rank against, not just our currently-tracked
subset, which this data source doesn't give us. What you genuinely get:
real year-over-year output you can compare yourself — a substantial
upgrade over nothing, but not a like-for-like historical potential score.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python historical_seasons_ingest.py --season 2024 --offset 0 --limit 3600
    python historical_seasons_ingest.py --season 2024 --offset 3600 --limit 3600
    # repeat for --season 2023 once 2024 is done, if you want to go back further
"""

import os
import time
import json
import argparse
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


def run(season, offset, limit):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.external_id, p.full_name
            FROM players p
            JOIN player_potential_scores pps ON pps.player_id = p.id
            ORDER BY p.id
            OFFSET %s LIMIT %s
        """, (offset, limit))
        players = cur.fetchall()

    print(f"Processing {len(players)} players for season {season} (offset {offset}, limit {limit}).")

    updated = 0
    no_data = 0
    for i, (db_id, external_id, name) in enumerate(players, 1):
        try:
            data = api_get("players", {"id": external_id, "season": season})
        except RateLimitError as e:
            print(f"\nHit the rate limit after {updated} players updated.")
            print(f"Actual error detail: {e}")
            print(f"Re-run with --offset {offset + i - 1} to continue from here.")
            break

        if not data:
            no_data += 1
            continue

        # Sum real competitive output across every non-friendly entry —
        # a player might have appeared for more than one club/competition
        # that season (a mid-season transfer), so this genuinely combines
        # everything rather than picking just one arbitrary entry.
        total_apps = total_minutes = total_goals = total_assists = 0
        ratings = []
        best_entry = None  # whichever entry has the most minutes — used for club/league display
        best_minutes = -1

        for stat in data[0].get("statistics", []):
            league_name = stat.get("league", {}).get("name", "")
            if "Friendlies" in league_name:
                continue  # exhibition matches, not real season output
            games = stat.get("games", {})
            goals = stat.get("goals", {})
            minutes = games.get("minutes") or 0
            total_apps += games.get("appearences") or 0
            total_minutes += minutes
            total_goals += goals.get("total") or 0
            total_assists += goals.get("assists") or 0
            rating = games.get("rating")
            if rating:
                try:
                    ratings.append(float(rating))
                except (TypeError, ValueError):
                    pass
            if minutes > best_minutes:
                best_minutes = minutes
                best_entry = stat

        if total_minutes == 0 or not best_entry:
            no_data += 1
            continue

        avg_rating = sum(ratings) / len(ratings) if ratings else None
        club_name = best_entry.get("team", {}).get("name")
        league_name = best_entry.get("league", {}).get("name")

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO player_season_history
                    (player_id, season, club_name, league_name, appearances, minutes_played, goals, assists, avg_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, season) DO UPDATE SET
                    club_name = EXCLUDED.club_name, league_name = EXCLUDED.league_name,
                    appearances = EXCLUDED.appearances, minutes_played = EXCLUDED.minutes_played,
                    goals = EXCLUDED.goals, assists = EXCLUDED.assists,
                    avg_rating = EXCLUDED.avg_rating, ingested_at = now()
            """, (db_id, str(season), club_name, league_name, total_apps, total_minutes,
                  total_goals, total_assists, avg_rating))
        conn.commit()
        updated += 1

        if i % 200 == 0:
            print(f"  ...{i}/{len(players)} processed ({updated} with real season data found)")

    print(f"\nDone. {updated} players had real season {season} data recorded. {no_data} had none (didn't play that season, or only in friendlies).")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=3600)
    args = parser.parse_args()
    run(args.season, args.offset, args.limit)