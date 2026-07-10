"""
Potential Index scoring model.

Combines up to three components into one 0-100 score per player per season:

  1. stat_component        — per-90 production, blended 60% average / 40%
                              best-stat within position-relevant metrics,
                              percentile-ranked against the same position
                              group across ALL leagues (so a standout in a
                              smaller league isn't invisible next to a big-
                              league player). The average/peak blend means a
                              genuine specialist (elite at one thing, merely
                              average elsewhere) gets real credit instead of
                              being diluted toward mediocre by simple averaging.
  2. age_adjustment         — younger players producing at a given level
                              get a boost, since the same output at 19
                              signals more than at 24.
  3. qualitative_component  — scout_notes.watch_level mapped to a score.
                              IMPORTANT: only included when a player actually
                              has a real scout_notes entry. Without one, this
                              component is excluded entirely (not defaulted to
                              a neutral 50) and its weight is redistributed
                              proportionally across stat/age — otherwise every
                              unscoutd player would be capped well below 100
                              purely by a placeholder that isn't real data.

Run after ingestion + a fixtures refresh:
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

# Blend between the average of a player's position-relevant percentiles and
# their single BEST percentile. Pure averaging dilutes genuine specialists
# (elite tackler, merely average passer) toward mediocre; pure peak-taking
# would overrate one-off flukes. This blend rewards real standout skill
# without ignoring the rest of the profile.
PEAK_BLEND_WEIGHT = 0.40  # 40% best-stat, 60% average-of-all-relevant-stats

# Position groups match players.primary_position exactly, as stored in the DB.
POSITION_METRIC_MAP = {
    "Attacker": ["goals_p90", "shots_on_target_p90", "key_passes_p90", "take_ons_completed_p90"],
    "Midfielder": ["key_passes_p90", "tackles_p90", "passes_completed_pct", "take_ons_completed_p90"],
    "Defender": ["tackles_p90", "interceptions_p90", "duels_won_pct", "passes_completed_pct"],
    "Goalkeeper": ["save_pct", "passes_completed_pct"],
}
DEFAULT_METRICS = ["key_passes_p90", "tackles_p90", "passes_completed_pct"]

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
            SUM(pms.key_passes) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS key_passes_p90,
            SUM(pms.take_ons_completed) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS take_ons_completed_p90,
            SUM(pms.tackles) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS tackles_p90,
            SUM(pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS interceptions_p90,
            (SUM(pms.passes_completed) * 1.0 / NULLIF(SUM(pms.passes_attempted), 0)) * 100 AS passes_completed_pct,
            (SUM(pms.duels_won) * 1.0 / NULLIF(SUM(pms.duels_attempted), 0)) * 100 AS duels_won_pct,
            (SUM(pms.saves) * 1.0 / NULLIF(SUM(pms.saves) + SUM(pms.goals_conceded), 0)) * 100 AS save_pct
        FROM player_match_stats pms
        JOIN players p ON p.id = pms.player_id
        JOIN matches m ON m.id = pms.match_id
        JOIN leagues l ON l.id = m.league_id
        WHERE l.season = %s
        GROUP BY pms.player_id, p.primary_position, p.date_of_birth
        HAVING SUM(pms.minutes_played) >= 450   -- ~5 full matches minimum sample
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
    if age <= 17:
        return 100.0
    if age >= 26:
        return 20.0
    return float(np.interp(age, [17, 20, 23, 26], [100, 85, 55, 20]))


def qualitative_component(conn, player_id):
    """Returns None if no real scout_notes entry exists — the caller then
    excludes this component entirely rather than defaulting to a fake
    neutral score that would artificially cap everyone's potential index."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watch_level FROM scout_notes WHERE player_id = %s ORDER BY created_at DESC LIMIT 1",
            (player_id,),
        )
        row = cur.fetchone()
    if not row:
        return None  # no scouting action of any kind on record
    return WATCH_LEVEL_SCORE.get(row[0], 50)  # a real note with an unmapped level -> neutral


def compute_stat_percentiles(cols, rows):
    """Percentile-rank each player within their real position group, across
    all leagues, then blend average-of-relevant-stats with best-single-stat."""
    idx = {c: i for i, c in enumerate(cols)}
    by_position = {}
    for row in rows:
        pos = row[idx["primary_position"]]
        group = pos if pos in POSITION_METRIC_MAP else None
        by_position.setdefault(group, []).append(row)

    scores = {}
    for pos, group_rows in by_position.items():
        metrics = POSITION_METRIC_MAP.get(pos, DEFAULT_METRICS)
        metric_arrays = {
            m: np.array([float(r[idx[m]] or 0) for r in group_rows if m in idx])
            for m in metrics if m in idx
        }
        for row in group_rows:
            pid = row[idx["player_id"]]
            percentiles = []
            for m in metrics:
                if m not in idx:
                    continue
                val = float(row[idx[m]] or 0)
                pct = sp_stats.percentileofscore(metric_arrays[m], val)
                percentiles.append(pct)
            if not percentiles:
                scores[pid] = 50.0
                continue
            avg_pct = float(np.mean(percentiles))
            best_pct = float(np.max(percentiles))
            scores[pid] = (1 - PEAK_BLEND_WEIGHT) * avg_pct + PEAK_BLEND_WEIGHT * best_pct
    return scores


def run(season):
    conn = psycopg2.connect(DATABASE_URL)
    cols, rows = fetch_season_aggregates(conn, season)
    idx = {c: i for i, c in enumerate(cols)}
    stat_scores = compute_stat_percentiles(cols, rows)

    processed = 0
    with conn.cursor() as cur:
        for row in rows:
            pid = row[idx["player_id"]]
            dob = row[idx["date_of_birth"]]
            stat_c = stat_scores.get(pid, 50.0)
            age_c = age_adjustment(dob, season)
            qual_c = qualitative_component(conn, pid)

            if qual_c is None:
                # No real scouting data — redistribute its weight
                # proportionally across stat/age rather than faking a
                # neutral qualitative score that caps everyone's ceiling.
                remaining = WEIGHTS["stat_component"] + WEIGHTS["age_adjustment"]
                w_stat = WEIGHTS["stat_component"] / remaining
                w_age = WEIGHTS["age_adjustment"] / remaining
                potential = w_stat * stat_c + w_age * age_c
                qual_c_store = None
            else:
                potential = (
                    WEIGHTS["stat_component"] * stat_c
                    + WEIGHTS["age_adjustment"] * age_c
                    + WEIGHTS["qualitative_component"] * qual_c
                )
                qual_c_store = qual_c

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
                (pid, str(season), potential, stat_c, stat_c, age_c, qual_c_store),
            )
            # Also append to history (never overwritten) so trend sparklines
            # have real data to show as this runs night after night.
            cur.execute(
                """
                INSERT INTO player_potential_history (player_id, season, potential_index)
                VALUES (%s, %s, %s)
                """,
                (pid, str(season), potential),
            )

            processed += 1
            # Commit periodically — a single transaction spanning thousands
            # of inserts risks hitting a statement/lock timeout on the
            # database (this happened in practice: "canceling statement due
            # to statement timeout" partway through a ~17,000-player run).
            if processed % 200 == 0:
                conn.commit()
                print(f"  ...{processed}/{len(rows)} scored")
    conn.commit()
    conn.close()
    print(f"Scored {len(rows)} players for season {season}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    run(args.season)