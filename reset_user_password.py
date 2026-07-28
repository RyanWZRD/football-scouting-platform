"""
Resets a user's password directly via the database. Genuinely safe in
a way the removed /auth/reset-password endpoint wasn't: this only
works for someone who already has real database access (DATABASE_URL),
not anyone who merely knows an account's email plus the app's shared
API key.

Usage:
    export DATABASE_URL=...
    python reset_user_password.py --email ryantloft@gmail.com --new-password "SomeNewPassword123"
"""

import os
import argparse
import bcrypt
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def run(email, new_password):
    email = email.strip().lower()
    if len(new_password) < 8:
        print("New password must be at least 8 characters.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if not row:
            print(f"No account found for {email!r}.")
            conn.close()
            return
        cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (hash_password(new_password), row[0]))
    conn.commit()
    conn.close()
    print(f"Password reset for {email!r}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--new-password", required=True)
    args = parser.parse_args()
    run(args.email, args.new_password)