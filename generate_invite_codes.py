"""
Generates one or more real, random, one-time-use invite codes for
registration. Only someone with database access (you) can run this —
codes aren't generated anywhere in the app itself.

Usage:
    export DATABASE_URL=...
    python generate_invite_codes.py --count 3
"""

import os
import secrets
import argparse
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def generate_code():
    # Genuinely random, URL-safe, short enough to type/share easily
    return secrets.token_urlsafe(6)


def run(count):
    conn = psycopg2.connect(DATABASE_URL)
    codes = []
    with conn.cursor() as cur:
        for _ in range(count):
            code = generate_code()
            cur.execute("INSERT INTO invite_codes (code) VALUES (%s)", (code,))
            codes.append(code)
    conn.commit()
    conn.close()

    print(f"Generated {count} invite code(s):")
    for c in codes:
        print(f"  {c}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    run(args.count)