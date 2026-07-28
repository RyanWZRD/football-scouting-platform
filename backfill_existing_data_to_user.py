"""
One-time backfill: assigns all EXISTING shortlist/note and pipeline
data (which had no owner before Multi-User Support existed) to a
single, specified user account. Run this ONCE, after registering your
account through the app's new login UI (or via /auth/register
directly), and before relying on the app for shortlist/pipeline work.

Usage:
    export DATABASE_URL=...
    python backfill_existing_data_to_user.py --email you@example.com
"""

import os
import argparse
import psycopg2


def run(email):
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email.strip().lower(),))
        row = cur.fetchone()
        if not row:
            print(f"No account found for {email!r} — register through the app first, then re-run this.")
            conn.close()
            return
        user_id = row[0]

        cur.execute("UPDATE scout_notes SET user_id = %s WHERE user_id IS NULL", (user_id,))
        notes_updated = cur.rowcount

        cur.execute("UPDATE recruitment_pipeline SET user_id = %s WHERE user_id IS NULL", (user_id,))
        pipeline_updated = cur.rowcount

    conn.commit()
    conn.close()
    print(f"Backfilled {notes_updated} scout notes and {pipeline_updated} pipeline entries to user_id {user_id} ({email}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    args = parser.parse_args()
    run(args.email)