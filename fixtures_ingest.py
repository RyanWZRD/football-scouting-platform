"""
Match-level ingestion: pulls recent finished fixtures per league, then
per-player stats for each of those fixtures. This is what populates
player_match_stats — the table scoring_model.py actually depends on.

COST WARNING: /fixtures/players costs one API request PER MATCH. A full
season is 200-380 matches per league. On the free tier (100 req/day),
this script deliberately caps how many recent matches it pulls per
league via --max-fixtures, rather than trying to backfill a full season.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python fixtures_ingest.py --league 88 --season 2023 --max-fixtures 5
    python fixtures_ingest.py --all-leagues --season 2023 --max-fixtures 5
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
# Pro plan: 7,500/day, 300 requests/minute — much faster pacing is safe now.
REQUEST_DELAY_SECONDS = 0.25


class RateLimitError(Exception):
    pass


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {})
    if resp.status_code == 429:
        raise RateLimitError("Rate limit hit (HTTP 429).")
    resp.raise_for_status()
    body = resp.json()
    errors = body.get("errors")
    if errors:
        if isinstance(errors, dict) and any("limit" in str(v).lower() for v in errors.values()):
            raise RateLimitError(f"Rate limit reported in response body: {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_db_league_id(conn, league_external_id):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM leagues WHERE external_id = %s", (str(league_external_id),))
        row = cur.fetchone()
        return row[0] if row else None


def get_db_club_id(conn, club_external_id):
    if not club_external_id:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM clubs WHERE external_id = %s", (str(club_external_id),))
        row = cur.fetchone()
        return row[0] if row else None


def ensure_player(conn, player_external_id, name, club_id=None):
    """Some players in a match lineup may not exist yet (e.g. squad players
    the /players endpoint page limit didn't reach). Create a minimal row
    rather than dropping their stats — and link the club we saw them play
    for, so they don't end up with no league/club context."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO players (external_id, full_name, current_club_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE SET
                current_club_id = COALESCE(players.current_club_id, EXCLUDED.current_club_id)
            """,
            (str(player_external_id), name, club_id),
        )
        cur.execute("SELECT id FROM players WHERE external_id = %s", (str(player_external_id),))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def upsert_matches_for_league(conn, league_external_id, season, db_league_id, max_fixtures):
    fixtures = api_get("fixtures", {
        "league": league_external_id, "season": season, "status": "FT"
    })
    if not fixtures:
        print(f"    no finished fixtures found for league {league_external_id}")
        return []

    # Most recent first, capped to the budget.
    fixtures = sorted(fixtures, key=lambda f: f["fixture"]["date"], reverse=True)[:max_fixtures]

    db_match_ids = []
    for f in fixtures:
        fx = f["fixture"]
        teams = f["teams"]
        goals = f["goals"]
        home_club_id = get_db_club_id(conn, teams["home"]["id"])
        away_club_id = get_db_club_id(conn, teams["away"]["id"])

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches (external_id, league_id, home_club_id, away_club_id,
                                      match_date, home_score, away_score, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'finished')
                ON CONFLICT (external_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score
                RETURNING id
                """,
                (str(fx["id"]), db_league_id, home_club_id, away_club_id,
                 fx["date"], goals["home"], goals["away"]),
            )
            conn.commit()
            db_match_ids.append((cur.fetchone()[0], fx["id"]))
    return db_match_ids


def upsert_player_stats_for_match(conn, db_match_id, fixture_external_id):
    data = api_get("fixtures/players", {"fixture": fixture_external_id})
    if not data:
        return
    for team_block in data:
        club_id = get_db_club_id(conn, team_block["team"]["id"])
        for entry in team_block["players"]:
            p = entry["player"]
            stats = entry["statistics"][0] if entry["statistics"] else {}
            games = stats.get("games", {})
            minutes = games.get("minutes") or 0
            if minutes == 0:
                continue  # didn't actually play — skip, keeps table meaningful
            db_player_id = ensure_player(conn, p["id"], p["name"], club_id)

            shots = stats.get("shots", {})
            passes = stats.get("passes", {})
            tackles = stats.get("tackles", {})
            duels = stats.get("duels", {})
            dribbles = stats.get("dribbles", {})
            goals = stats.get("goals", {})

            # API-Football gives pass "total" and an "accuracy" percentage,
            # not a raw completed count — derive completed from the two.
            passes_total = passes.get("total") or 0
            accuracy_raw = passes.get("accuracy")
            accuracy_pct = None
            if accuracy_raw is not None:
                try:
                    accuracy_pct = float(str(accuracy_raw).replace("%", ""))
                except ValueError:
                    accuracy_pct = None

            if accuracy_pct is not None and passes_total > 0:
                passes_completed = round(passes_total * accuracy_pct / 100)
                passes_attempted = passes_total
            else:
                # Unknown accuracy — store as no data rather than guessing 100%.
                passes_completed = 0
                passes_attempted = 0

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO player_match_stats
                        (player_id, match_id, club_id, minutes_played, position_played,
                         goals, assists, shots, shots_on_target, key_passes,
                         passes_completed, passes_attempted, take_ons_attempted,
                         take_ons_completed, tackles, interceptions, duels_won,
                         duels_attempted, rating)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (player_id, match_id) DO UPDATE SET
                        minutes_played = EXCLUDED.minutes_played,
                        goals = EXCLUDED.goals,
                        assists = EXCLUDED.assists,
                        shots = EXCLUDED.shots,
                        shots_on_target = EXCLUDED.shots_on_target,
                        key_passes = EXCLUDED.key_passes,
                        passes_completed = EXCLUDED.passes_completed,
                        passes_attempted = EXCLUDED.passes_attempted,
                        take_ons_attempted = EXCLUDED.take_ons_attempted,
                        take_ons_completed = EXCLUDED.take_ons_completed,
                        tackles = EXCLUDED.tackles,
                        interceptions = EXCLUDED.interceptions,
                        duels_won = EXCLUDED.duels_won,
                        duels_attempted = EXCLUDED.duels_attempted,
                        rating = EXCLUDED.rating
                    """,
                    (
                        db_player_id, db_match_id, club_id, minutes, games.get("position"),
                        goals.get("total") or 0, goals.get("assists") or 0,
                        shots.get("total") or 0, shots.get("on") or 0,
                        passes.get("key") or 0, passes_completed,
                        passes_attempted,
                        dribbles.get("attempts") or 0, dribbles.get("success") or 0,
                        tackles.get("total") or 0, tackles.get("interceptions") or 0,
                        duels.get("won") or 0, duels.get("total") or 0,
                        float(stats.get("games", {}).get("rating") or 0) or None,
                    ),
                )
            conn.commit()


def run(league_ids, season, max_fixtures):
    conn = get_conn()
    completed = []
    for league_id in league_ids:
        print(f"Fetching fixtures for league {league_id} / season {season} ...")
        try:
            db_league_id = get_db_league_id(conn, league_id)
            if db_league_id is None:
                print(f"  league {league_id} not found in DB yet — run ingest.py for it first, skipping")
                continue
            matches = upsert_matches_for_league(conn, league_id, season, db_league_id, max_fixtures)
            print(f"  {len(matches)} matches recorded, fetching player stats...")
            for db_match_id, fixture_external_id in matches:
                upsert_player_stats_for_match(conn, db_match_id, fixture_external_id)
            completed.append(league_id)
            print(f"  done: league {league_id}")
        except RateLimitError:
            print(f"\nHit the rate limit while on league {league_id}.")
            print(f"Leagues completed this run: {completed}")
            print(f"Remaining: {[l for l in league_ids if l not in completed]}")
            print("Wait for quota reset, then re-run — already-recorded matches won't duplicate.")
            break
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=int, action="append")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--all-leagues", action="store_true")
    parser.add_argument("--max-fixtures", type=int, default=15,
                         help="Recent finished matches to pull PER LEAGUE. Each costs 1 API request. Pro tier (7,500/day) can afford much more than the free-tier default of 5.")
    args = parser.parse_args()

    # Same expanded set as ingest.py — keep these two lists in sync.
    LEAGUE_IDS = [
        39, 140, 78, 135, 61,        # Top 5
        88, 94, 203, 71, 98, 253, 179, 62,   # Original non-top5 set
        40, 144, 262, 128,           # Additional talent-pipeline leagues
    ]

    ids = args.league if args.league else (LEAGUE_IDS if args.all_leagues else [])
    if not ids:
        parser.error("Provide --league <id> (repeatable) or --all-leagues")

    run(ids, args.season, args.max_fixtures)