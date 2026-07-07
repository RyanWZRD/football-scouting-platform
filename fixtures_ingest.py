"""
Match-level ingestion: pulls recent finished fixtures per league, then
per-player stats for each of those fixtures. This is what populates
player_match_stats — the table scoring_model.py actually depends on.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=...
    python fixtures_ingest.py --league 88 --season 2025 --max-fixtures 600
    python fixtures_ingest.py --all-leagues --season 2025 --max-fixtures 600
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


def get_db_club_id(conn, club_external_id, cache=None):
    if not club_external_id:
        return None
    key = str(club_external_id)
    if cache is not None and key in cache:
        return cache[key]
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM clubs WHERE external_id = %s", (key,))
        row = cur.fetchone()
    result = row[0] if row else None
    if cache is not None:
        cache[key] = result
    return result


def upsert_player_stats_for_match(conn, db_match_id, fixture_external_id, club_cache):
    """Batched version: resolves all players in one match with a handful of
    round-trips total instead of ~3 per player. Also de-duplicates — some
    fixtures contain repeated/placeholder player entries (seen as external_id
    "0" in a couple of Argentine-league matches), which would otherwise crash
    the batched INSERT with a Postgres "affect row a second time" error."""
    data = api_get("fixtures/players", {"fixture": fixture_external_id})
    if not data:
        return

    # Pass 1: collect everyone who actually played, across both teams.
    player_entries = []
    for team_block in data:
        club_id = get_db_club_id(conn, team_block["team"]["id"], club_cache)
        for entry in team_block["players"]:
            p = entry["player"]
            stats = entry["statistics"][0] if entry["statistics"] else {}
            games = stats.get("games", {})
            minutes = games.get("minutes") or 0
            if minutes == 0:
                continue  # didn't actually play — skip, keeps table meaningful
            player_entries.append({
                "external_id": str(p["id"]), "name": p["name"], "club_id": club_id,
                "stats": stats, "games": games,
            })

    if not player_entries:
        return

    ext_ids = [e["external_id"] for e in player_entries]

    # One round-trip to find which of these players already exist.
    with conn.cursor() as cur:
        cur.execute("SELECT external_id, id FROM players WHERE external_id = ANY(%s)", (ext_ids,))
        id_map = {row[0]: row[1] for row in cur.fetchall()}

    seen_players = set()
    missing = []
    for e in player_entries:
        if e["external_id"] not in id_map and e["external_id"] not in seen_players:
            missing.append(e)
            seen_players.add(e["external_id"])

    if missing:
        values = [(e["external_id"], e["name"], e["club_id"]) for e in missing]
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO players (external_id, full_name, current_club_id)
                VALUES %s
                ON CONFLICT (external_id) DO UPDATE SET
                    current_club_id = COALESCE(players.current_club_id, EXCLUDED.current_club_id)
            """, values)
        with conn.cursor() as cur:
            cur.execute("SELECT external_id, id FROM players WHERE external_id = ANY(%s)", (ext_ids,))
            id_map = {row[0]: row[1] for row in cur.fetchall()}

    # Build one batched INSERT covering every player's match stats.
    stat_rows = []
    seen_stat_keys = set()
    for e in player_entries:
        db_player_id = id_map.get(e["external_id"])
        if db_player_id is None:
            continue

        stat_key = (db_player_id, db_match_id)
        if stat_key in seen_stat_keys:
            print(f"    duplicate stats skipped for player {e['external_id']} in fixture {fixture_external_id}")
            continue
        seen_stat_keys.add(stat_key)

        stats = e["stats"]
        games = e["games"]
        minutes = games.get("minutes") or 0
        shots = stats.get("shots", {})
        passes = stats.get("passes", {})
        tackles = stats.get("tackles", {})
        duels = stats.get("duels", {})
        dribbles = stats.get("dribbles", {})
        goals = stats.get("goals", {})
        fouls = stats.get("fouls", {})
        cards = stats.get("cards", {})
        penalty = stats.get("penalty", {})

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
            passes_completed = 0
            passes_attempted = 0

        stat_rows.append((
            db_player_id, db_match_id, e["club_id"], minutes, games.get("position"),
            goals.get("total") or 0, goals.get("assists") or 0,
            shots.get("total") or 0, shots.get("on") or 0,
            passes.get("key") or 0, passes_completed, passes_attempted,
            dribbles.get("attempts") or 0, dribbles.get("success") or 0,
            tackles.get("total") or 0, tackles.get("interceptions") or 0,
            duels.get("won") or 0, duels.get("total") or 0,
            float(stats.get("games", {}).get("rating") or 0) or None,
            goals.get("saves") or 0, goals.get("conceded") or 0,
            fouls.get("committed") or 0, fouls.get("drawn") or 0,
            cards.get("yellow") or 0, cards.get("red") or 0,
            penalty.get("won") or 0, penalty.get("committed") or 0,
            penalty.get("scored") or 0, penalty.get("missed") or 0,
            stats.get("offsides") or 0,
        ))

    if stat_rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO player_match_stats
                    (player_id, match_id, club_id, minutes_played, position_played,
                     goals, assists, shots, shots_on_target, key_passes,
                     passes_completed, passes_attempted, take_ons_attempted,
                     take_ons_completed, tackles, interceptions, duels_won,
                     duels_attempted, rating,
                     saves, goals_conceded, fouls_committed, fouls_drawn,
                     yellow_cards, red_cards, penalties_won, penalties_committed,
                     penalties_scored, penalties_missed, offsides)
                VALUES %s
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
                    rating = EXCLUDED.rating,
                    saves = EXCLUDED.saves,
                    goals_conceded = EXCLUDED.goals_conceded,
                    fouls_committed = EXCLUDED.fouls_committed,
                    fouls_drawn = EXCLUDED.fouls_drawn,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards = EXCLUDED.red_cards,
                    penalties_won = EXCLUDED.penalties_won,
                    penalties_committed = EXCLUDED.penalties_committed,
                    penalties_scored = EXCLUDED.penalties_scored,
                    penalties_missed = EXCLUDED.penalties_missed,
                    offsides = EXCLUDED.offsides
            """, stat_rows)

    conn.commit()  # once per match, not once (or twice) per player


def upsert_matches_for_league(conn, league_external_id, season, db_league_id, max_fixtures, club_cache):
    fixtures = api_get("fixtures", {
        "league": league_external_id, "season": season, "status": "FT"
    })
    if not fixtures:
        print(f"    no finished fixtures found for league {league_external_id}")
        return []

    fixtures = sorted(fixtures, key=lambda f: f["fixture"]["date"], reverse=True)[:max_fixtures]

    db_match_ids = []
    for f in fixtures:
        fx = f["fixture"]
        teams = f["teams"]
        goals = f["goals"]
        home_club_id = get_db_club_id(conn, teams["home"]["id"], club_cache)
        away_club_id = get_db_club_id(conn, teams["away"]["id"], club_cache)

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


def run(league_ids, season, max_fixtures):
    conn = get_conn()
    completed = []
    club_cache = {}
    for league_id in league_ids:
        print(f"Fetching fixtures for league {league_id} / season {season} ...")
        try:
            db_league_id = get_db_league_id(conn, league_id)
            if db_league_id is None:
                print(f"  league {league_id} not found in DB yet — run ingest.py for it first, skipping")
                continue
            matches = upsert_matches_for_league(conn, league_id, season, db_league_id, max_fixtures, club_cache)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT match_id FROM player_match_stats WHERE match_id = ANY(%s)",
                    ([m[0] for m in matches],),
                )
                already_done = {row[0] for row in cur.fetchall()}
            todo = [m for m in matches if m[0] not in already_done]
            skipped = len(matches) - len(todo)

            print(f"  {len(matches)} matches recorded"
                  + (f" ({skipped} already have stats, skipping)" if skipped else "")
                  + f" — fetching stats for {len(todo)}...")
            for i, (db_match_id, fixture_external_id) in enumerate(todo, 1):
                upsert_player_stats_for_match(conn, db_match_id, fixture_external_id, club_cache)
                if i % 25 == 0:
                    print(f"    ...{i}/{len(todo)} matches processed")
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
    parser.add_argument("--max-fixtures", type=int, default=600,
                         help="Recent finished matches to pull PER LEAGUE. Each costs 1 API request.")
    args = parser.parse_args()

    LEAGUE_IDS = [
        39, 140, 78, 135, 61,
        88, 94, 203, 71, 98, 253, 179, 62,
        40, 144, 262, 128,
    ]

    ids = args.league if args.league else (LEAGUE_IDS if args.all_leagues else [])
    if not ids:
        parser.error("Provide --league <id> (repeatable) or --all-leagues")

    run(ids, args.season, args.max_fixtures)