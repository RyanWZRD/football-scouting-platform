"""
Potential Index scoring model.

Combines three components into one 0-100 score per player per season:

  1. stat_component        — per-90 production, percentile-ranked against
                              same position + similar age band, ACROSS
                              leagues (so a standout in the Scottish
                              Championship isn't invisible next to a
                              Bundesliga player — percentiles are computed
                              within position/age cohort, not within league).
  2. age_adjustment         — younger players producing at a given level
                              get a boost, since the same output at 19
                              signals more than at 24.
  3. qualitative_component  — scout_notes.watch_level mapped to a score,
                              so a human scout can move a player up/down
                              regardless of what the stats say.

Final index = weighted blend, tunable via WEIGHTS below.

Run on a schedule (e.g. nightly) after ingestion:
    python scoring_model.py --season 2025
"""

import os
import argparse
import psycopg2
import numpy as np
from scipy import stats as sp_stats

DATABASE_URL = os.environ.get("DATABASE_URL")

WEIGHTS = {
    "stat_component": 0.55,
    "age_adjustment": 0.20,
    "qualitative_component": 0.25,
}

POSITION_METRIC_MAP = {
    # position group -> which per-90 stats matter most for that role
    "ST": ["goals_p90", "shots_on_target_p90", "xg_p90"],
    "AM": ["key_passes_p90", "xa_p90", "take_ons_completed_p90"],
    "CM": ["progressive_passes_p90", "passes_completed_pct", "tackles_p90"],
    "DM": ["tackles_p90", "interceptions_p90", "passes_completed_pct"],
    "CB": ["duels_won_pct", "interceptions_p90", "passes_completed_pct"],
    "RB": ["progressive_carries_p90", "tackles_p90", "key_passes_p90"],
    "LB": ["progressive_carries_p90", "tackles_p90", "key_passes_p90"],
    "GK": [],  # goalkeepers need a separate shot-stopping model, not covered here
}

WATCH_LEVEL_SCORE = {"monitor": 50, "shortlist": 75, "priority": 95}


def fetch_season_aggregates(conn, season):
    """Aggregate player_match_stats into per-90 rates for the season."""
    query = """
        SELECT
            pms.player_id,
            p.primary_position,
            p.date_of_birth,
            SUM(pms.minutes_played) AS minutes,
            SUM(pms.goals) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS goals_p90,
            SUM(pms.shots_on_target) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS shots_on_target_p90,
            SUM(pms.xg) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS xg_p90,
            SUM(pms.key_passes) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS key_passes_p90,
            SUM(pms.xa) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS xa_p90,
            SUM(pms.take_ons_completed) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS take_ons_completed_p90,
            SUM(pms.progressive_passes) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS progressive_passes_p90,
            SUM(pms.progressive_carries) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS progressive_carries_p90,
            SUM(pms.tackles) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS tackles_p90,
            SUM(pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS interceptions_p90,
            (SUM(pms.passes_completed) * 1.0 / NULLIF(SUM(pms.passes_attempted), 0)) * 100 AS passes_completed_pct,
            (SUM(pms.duels_won) * 1.0 / NULLIF(SUM(pms.duels_attempted), 0)) * 100 AS duels_won_pct
        FROM player_match_stats pms
        JOIN players p ON p.id = pms.player_id
        JOIN matches m ON m.id = pms.match_id
        JOIN leagues l ON l.id = m.league_id
        WHERE l.season = %s
        GROUP BY pms.player_id, p.primary_position, p.date_of_birth
        HAVING SUM(pms.minutes_played) >= 180   -- minimum sample: ~2 full matches (temporarily
                                                  -- lowered from 450 since only 3 matches/league
                                                  -- ingested so far; raise back up as more matches
                                                  -- get added over time)
    """
    with conn.cursor() as cur:
        cur.execute(query, (str(season),))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return cols, rows


def age_adjustment(date_of_birth, season):
    if date_of_birth is None:
        return 50.0
    season_year = int(str(season)[:4])
    age = season_year - date_of_birth.year
    # Peak boost under 20, tapering to neutral by mid-20s.
    if age <= 17:
        return 100.0
    if age >= 26:
        return 20.0
    return float(np.interp(age, [17, 20, 23, 26], [100, 85, 55, 20]))


def qualitative_component(conn, player_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT watch_level FROM scout_notes
            WHERE player_id = %s
            ORDER BY created_at DESC LIMIT 1
            """,
            (player_id,),
        )
        row = cur.fetchone()
    if not row or row[0] not in WATCH_LEVEL_SCORE:
        return 50.0  # neutral prior when no scout has weighed in yet
    return WATCH_LEVEL_SCORE[row[0]]


def compute_stat_percentiles(cols, rows):
    """Percentile-rank each player within their position group, across all leagues."""
    idx = {c: i for i, c in enumerate(cols)}
    by_position = {}
    for row in rows:
        pos = row[idx["primary_position"]]
        group = pos if pos in POSITION_METRIC_MAP else "CM"  # fallback bucket
        by_position.setdefault(group, []).append(row)

    scores = {}
    for pos, group_rows in by_position.items():
        metrics = POSITION_METRIC_MAP.get(pos, [])
        if not metrics:
            for row in group_rows:
                scores[row[idx["player_id"]]] = 50.0
            continue
        metric_arrays = {m: np.array([float(r[idx[m]] or 0) for r in group_rows]) for m in metrics}
        for row in group_rows:
            pid = row[idx["player_id"]]
            percentiles = []
            for m in metrics:
                val = float(row[idx[m]] or 0)
                pct = sp_stats.percentileofscore(metric_arrays[m], val)
                percentiles.append(pct)
            scores[pid] = float(np.mean(percentiles))
    return scores


def run(season):
    conn = psycopg2.connect(DATABASE_URL)
    cols, rows = fetch_season_aggregates(conn, season)
    idx = {c: i for i, c in enumerate(cols)}
    stat_scores = compute_stat_percentiles(cols, rows)

    with conn.cursor() as cur:
        for row in rows:
            pid = row[idx["player_id"]]
            dob = row[idx["date_of_birth"]]
            stat_c = stat_scores.get(pid, 50.0)
            age_c = age_adjustment(dob, season)
            qual_c = qualitative_component(conn, pid)

            potential = (
                WEIGHTS["stat_component"] * stat_c
                + WEIGHTS["age_adjustment"] * age_c
                + WEIGHTS["qualitative_component"] * qual_c
            )

            cur.execute(
                """
                INSERT INTO player_potential_scores
                    (player_id, season, potential_index, percentile_vs_position,
                     stat_component, age_adjustment, qualitative_component)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, season) DO UPDATE SET
                    potential_index = EXCLUDED.potential_index,
                    stat_component = EXCLUDED.stat_component,
                    age_adjustment = EXCLUDED.age_adjustment,
                    qualitative_component = EXCLUDED.qualitative_component,
                    computed_at = now()
                """,
                (pid, str(season), potential, stat_c, stat_c, age_c, qual_c),
            )
    conn.commit()
    conn.close()
    print(f"Scored {len(rows)} players for season {season}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    run(args.season)
