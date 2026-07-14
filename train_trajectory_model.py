"""
Trains a genuine machine learning model to predict player trajectory —
the platform's first real step from transparent rule-based percentiles
to actual predictive modeling.

HONEST FRAMING, since this is genuinely different from everything else
built today: rather than predicting each player's OWN future from their
OWN limited history (trend history is still weeks old for most players,
not enough for that to be reliable per-player), this learns GENERAL
PATTERNS from the cross-section of players who DO have enough history —
"players who look like this tend to trend up/down by this much" — then
applies that learned pattern to any player, even ones without much
history of their own yet. This will genuinely improve as more trend
history and multi-season data accumulate over time; right now it's a
real MVP, not a mature model.

Usage:
    export DATABASE_URL=...
    pip install scikit-learn joblib --break-system-packages
    python train_trajectory_model.py
"""

import os
import joblib
import numpy as np
import psycopg2
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

DATABASE_URL = os.environ.get("DATABASE_URL")
MIN_TRAINING_SAMPLES = 50  # below this, the model isn't trustworthy enough to ship
MODEL_PATH = "trajectory_model.joblib"

POSITIONS = ["Goalkeeper", "Defender", "Midfielder", "Attacker"]


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_training_data(conn):
    """For every player with 2+ trend history points spanning a real gap,
    compute their OBSERVED daily trend rate — this is the real-world
    ground truth the model learns from, not a hand-crafted assumption."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH bounds AS (
                SELECT player_id, MIN(computed_at) AS first_at, MAX(computed_at) AS last_at
                FROM player_potential_history
                GROUP BY player_id
                HAVING COUNT(*) >= 2 AND MAX(computed_at) - MIN(computed_at) >= interval '2 days'
            ),
            first_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS first_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.first_at
            ),
            last_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS last_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.last_at
            )
            SELECT p.id, p.primary_position, p.date_of_birth,
                   pps.potential_index, pps.stat_component, pps.age_adjustment,
                   fv.first_val, lv.last_val,
                   EXTRACT(EPOCH FROM (b.last_at - b.first_at)) / 86400.0 AS days_elapsed
            FROM bounds b
            JOIN first_vals fv ON fv.player_id = b.player_id
            JOIN last_vals lv ON lv.player_id = b.player_id
            JOIN players p ON p.id = b.player_id
            JOIN LATERAL (
                SELECT potential_index, stat_component, age_adjustment FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.primary_position IS NOT NULL AND p.date_of_birth IS NOT NULL
        """)
        return cur.fetchall()


def build_features(rows):
    """Converts raw DB rows into a real feature matrix — age, one-hot
    position, and the existing stat/age components already computed by
    scoring_model.py, since those are themselves already meaningful
    signal, not raw noise."""
    X, y = [], []
    from datetime import date
    today = date.today()
    for row in rows:
        pid, position, dob, potential, stat_c, age_adj, first_val, last_val, days = row
        if days <= 0 or stat_c is None or age_adj is None:
            continue
        age = (today - dob).days / 365.25
        position_onehot = [1.0 if position == p else 0.0 for p in POSITIONS]
        features = [age, potential or 50, stat_c, age_adj] + position_onehot
        daily_trend = (last_val - first_val) / days
        X.append(features)
        y.append(daily_trend)
    return np.array(X), np.array(y)


def run():
    conn = get_conn()
    rows = fetch_training_data(conn)
    conn.close()

    print(f"Found {len(rows)} players with enough trend history to train from.")
    if len(rows) < MIN_TRAINING_SAMPLES:
        print(f"That's below the minimum ({MIN_TRAINING_SAMPLES}) needed for a trustworthy model.")
        print("Not training yet — re-run this in a few weeks once more trend history has accumulated.")
        return

    X, y = build_features(rows)
    print(f"Training on {len(X)} real examples with {X.shape[1]} features each.")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, max_depth=6, min_samples_leaf=5, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    # Honest baseline check: is this model actually better than the
    # simplest possible guess (zero trend for everyone)? With this little
    # historical depth, real observed daily trends are small, so a model
    # error that LOOKS small could still be barely better than trivial —
    # this is the check that actually tells us whether it's adding value.
    naive_mae = mean_absolute_error(y_test, np.zeros_like(y_test))
    improvement_pct = round(100 * (naive_mae - mae) / naive_mae, 1) if naive_mae > 0 else 0

    print(f"\nModel trained. Mean absolute error on held-out test data: {mae:.4f} potential-points/day")
    print(f"Naive baseline (predicting zero trend for everyone): {naive_mae:.4f} potential-points/day")
    if improvement_pct <= 0:
        print(f"Model does NOT meaningfully beat the naive baseline ({improvement_pct}%) — not trustworthy enough to rely on yet.")
        print("This is a real, useful signal that more data is needed, not a failure to fix.")
        print("NOT saving this model — deploying something that performs worse than a trivial guess would be")
        print("actively misleading, even though it would look sophisticated. Re-run this in a few more weeks")
        print("once more trend history has accumulated, and check again.")
        return

    if improvement_pct > 10:
        print(f"Model beats the naive baseline by {improvement_pct}% — genuinely adding real value, not just noise.")
    else:
        print(f"Model only beats the naive baseline by {improvement_pct}% — a real but modest edge. Honest expectation: this is early days.")
    print("HONEST CONTEXT: this error rate reflects how young the training data still is —")
    print("expect this to improve as more trend history and multi-season data accumulate.")

    joblib.dump({"model": model, "positions": POSITIONS, "feature_names": ["age", "current_potential", "stat_component", "age_adjustment"] + [f"pos_{p}" for p in POSITIONS]}, MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH} — commit this file to the repo so the API can load it.")


if __name__ == "__main__":
    run()