"""
Corrects players.current_club_id using real match evidence, fixing a
systemic bug: per-club roster ingestion processes leagues in a fixed
order, so if a player appears on two different clubs' squad lists
simultaneously (common when a source API hasn't fully removed a departed
player yet), whichever league happened to be processed LAST silently
"wins" — even if that club is actually wrong and the player has been
playing real minutes elsewhere all season (e.g. Igor Jesus showing as
Botafogo despite 37 real matches logged for Nottingham Forest).

This script trusts match_stats data instead: for each player, whichever
club they have the most minutes_played for THIS SEASON is treated as
their real current club.

Run AFTER fixtures_ingest.py (needs real match data to work from) and
BEFORE scoring_model.py (so scores reflect the corrected club/league).

Usage:
    export DATABASE_URL=...
    python fix_club_assignments.py --season 2025
"""

import os
import argparse
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def run(season):
    conn = get_conn()
    with conn.cursor() as cur:
        # For each player, find the club they have the MOST minutes with
        # this season, and compare to their currently-stored club.
        # BUG FIX: the original query had no deterministic tie-breaker.
        # When a player has genuinely similar total minutes split across
        # two clubs (a mid-season loan, or cup vs domestic appearances
        # landing under different club_ids), PostgreSQL has no reliable
        # way to pick between rows tied on minutes — the "winner" could
        # differ from one run to the next, causing this script to flip
        # a player's club assignment back and forth on successive nights
        # even though nothing had genuinely changed. Confirmed directly:
        # this was inflating player_club_transfers with hundreds of fake
        # "transfers" per club that never happened. Fixed by adding a
        # real, deterministic tie-breaker — most recent match date wins.
        cur.execute("""
            SELECT DISTINCT ON (pms.player_id)
                pms.player_id, pms.club_id, p.current_club_id,
                SUM(pms.minutes_played) OVER (PARTITION BY pms.player_id, pms.club_id) AS minutes,
                MAX(m.match_date) OVER (PARTITION BY pms.player_id, pms.club_id) AS latest_match
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            JOIN leagues l ON l.id = m.league_id
            JOIN players p ON p.id = pms.player_id
            WHERE l.season = %s
            ORDER BY pms.player_id, minutes DESC, latest_match DESC
        """, (str(season),))
        rows = cur.fetchall()

    mismatches = [(pid, real_club, current_club) for pid, real_club, current_club, _ in rows
                  if real_club != current_club]

    print(f"Checked {len(rows)} players with real match data this season.")
    print(f"Found {len(mismatches)} whose stored club doesn't match their actual match evidence.")

    if not mismatches:
        conn.close()
        return

    with conn.cursor() as cur:
        for i, (pid, real_club, current_club) in enumerate(mismatches, 1):
            cur.execute("UPDATE players SET current_club_id = %s WHERE id = %s", (real_club, pid))
            if i % 200 == 0:
                conn.commit()
                print(f"  ...{i}/{len(mismatches)} corrected")
    conn.commit()
    conn.close()
    print(f"Done. Corrected {len(mismatches)} players' club assignments based on real match evidence.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    run(args.season)