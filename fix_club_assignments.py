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

HONEST HISTORY on this specific piece of logic — confirmed directly,
twice, with two real, opposite cases on 2026-08-04:
A timestamp-based guard (only correct when match evidence is newer
than the player's current updated_at) was added earlier to stop a
different real case — Elliot Anderson being reverted to his old club
using stale evidence, because his new club had zero minutes logged yet
during the pre-season window. That guard has now been REMOVED, because
it was confirmed to cause a worse, opposite failure: ingest.py resets
EVERY player's updated_at to "today" on every single nightly run,
regardless of whether the club it just set is even correct — so the
guard was silently blocking genuinely correct fixes too (confirmed:
Tijjani Reijnders, 1,640 real minutes for Manchester City, was stuck
showing AC Milan indefinitely because of this exact guard).
The Anderson-style problem is temporary and self-corrects naturally
once a new season starts accumulating real evidence — no guard needed.
The Reijnders-style problem, without this fix, could persist
indefinitely if a source squad listing is never cleaned up. Between a
temporary, self-correcting issue and a potentially permanent one, this
script now always trusts real match evidence, unconditionally — the
original, pre-guard behavior.

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

        cur.execute("SELECT player_id FROM protected_club_assignments")
        protected_ids = {r[0] for r in cur.fetchall()}

    mismatches = [(pid, real_club, current_club) for pid, real_club, current_club, _, _ in rows
                  if real_club != current_club and pid not in protected_ids]
    skipped_protected = sum(1 for pid, real_club, current_club, _, _ in rows
                             if real_club != current_club and pid in protected_ids)

    print(f"Checked {len(rows)} players with real match data this season.")
    if skipped_protected:
        print(f"Skipped {skipped_protected} on the protected list (known, confirmed recent transfers).")
    print(f"Found {len(mismatches)} whose stored club doesn't match real match evidence.")

    # Log this run so the dashboard can honestly show how many
    # mismatches were found and silently auto-corrected, rather than
    # that information only ever appearing in this console output once.
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO club_assignment_correction_log (players_checked, corrections_made) VALUES (%s, %s)",
            (len(rows), len(mismatches)),
        )
    conn.commit()

    if not mismatches:
        conn.close()
        return

    with conn.cursor() as cur:
        for i, (pid, real_club, current_club) in enumerate(mismatches, 1):
            cur.execute("UPDATE players SET current_club_id = %s WHERE id = %s", (real_club, pid))
            cur.execute(
                "INSERT INTO club_assignment_corrections (player_id, old_club_id, new_club_id) VALUES (%s, %s, %s)",
                (pid, current_club, real_club),
            )
            if i % 200 == 0:
                conn.commit()
                print(f"  ...{i}/{len(mismatches)} corrected")
    conn.commit()
    conn.close()
    print(f"Done. Corrected {len(mismatches)} players' club assignments based on genuinely current match evidence.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    run(args.season)