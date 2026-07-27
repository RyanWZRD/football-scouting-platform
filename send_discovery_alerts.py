"""
Checks The Autonomous Scout's top discoveries against what's already
been alerted on, and sends real Web Push notifications for genuinely
new ones. Designed to run periodically — e.g. once daily alongside the
existing nightly ingest job — not on every single request.

Reuses the live, deployed /players/autonomous-discovery endpoint
directly rather than duplicating its SQL logic here, so there's only
one place this scoring logic needs to be maintained.

Usage:
    export DATABASE_URL=...
    export VAPID_PRIVATE_KEY=...   (PEM format, same value set on Render)
    export VAPID_CLAIMS_EMAIL=mailto:you@example.com
    export API_BASE_URL=https://football-scouting-api-so8h.onrender.com
    export API_ACCESS_KEY=scout9x7k2mQpL4vRt8
    python send_discovery_alerts.py
"""

import os
import json
import requests
import psycopg2
from pywebpush import webpush, WebPushException

DATABASE_URL = os.environ.get("DATABASE_URL")
API_BASE_URL = os.environ.get("API_BASE_URL")
API_ACCESS_KEY = os.environ.get("API_ACCESS_KEY")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY")
VAPID_CLAIMS_EMAIL = os.environ.get("VAPID_CLAIMS_EMAIL", "mailto:admin@example.com")


def run():
    if not VAPID_PRIVATE_KEY:
        print("VAPID_PRIVATE_KEY not set — can't send real pushes without it.")
        return

    resp = requests.get(
        f"{API_BASE_URL}/players/autonomous-discovery",
        headers={"X-API-Key": API_ACCESS_KEY}, params={"limit": 5}, timeout=15,
    )
    resp.raise_for_status()
    discoveries = resp.json().get("discoveries", [])

    if not discoveries:
        print("No discoveries returned right now.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT id, endpoint, p256dh_key, auth_key FROM push_subscriptions")
        subscribers = cur.fetchall()

    if not subscribers:
        print("No push subscribers yet.")
        conn.close()
        return

    sent_count = 0
    for d in discoveries:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM sent_discovery_alerts WHERE player_id = %s", (d["id"],))
            already_sent = cur.fetchone()
        if already_sent:
            continue

        payload = json.dumps({
            "title": "🧠 New Top Discovery",
            "body": f"{d['full_name']} ({d['club']}) — discovery score {d['discovery_score']}",
        })

        for sub_id, endpoint, p256dh, auth in subscribers:
            try:
                webpush(
                    subscription_info={
                        "endpoint": endpoint,
                        "keys": {"p256dh": p256dh, "auth": auth},
                    },
                    data=payload,
                    vapid_private_key=VAPID_PRIVATE_KEY,
                    vapid_claims={"sub": VAPID_CLAIMS_EMAIL},
                )
                sent_count += 1
            except WebPushException as e:
                print(f"  Push failed for subscription {sub_id}: {e}")
                # A 410/404 genuinely means the subscription is dead —
                # clean it up rather than retrying it forever.
                if e.response is not None and e.response.status_code in (404, 410):
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM push_subscriptions WHERE id = %s", (sub_id,))
                    conn.commit()

        with conn.cursor() as cur:
            cur.execute("INSERT INTO sent_discovery_alerts (player_id) VALUES (%s) ON CONFLICT DO NOTHING", (d["id"],))
        conn.commit()

    conn.close()
    print(f"Done. Sent {sent_count} push notifications for genuinely new discoveries.")


if __name__ == "__main__":
    run()