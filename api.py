"""
API layer: Postgres (schema.sql) -> JSON -> dashboard.

This is the piece that turns the static demo into a live app. Run it
anywhere Python runs (locally, Render, Railway, Fly.io) and point the
dashboard's API_BASE_URL at it.

Usage:
    export DATABASE_URL=postgresql://user:pass@host/dbname
    pip install fastapi uvicorn psycopg2-binary
    uvicorn api:app --host 0.0.0.0 --port 8000

Endpoints:
    GET /health
    GET /leagues
    GET /players?league=Eredivisie&position=CM&max_age=21&sort=potential&limit=50
    GET /players/{player_id}          -> full dossier incl. match log + scout notes
    POST /players/{player_id}/watch   -> shortlist/monitor/priority a player (writes a scout_notes row)
"""

import os
import json
import time
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import anthropic
import re
import requests

DATABASE_URL = os.environ.get("DATABASE_URL")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
FOOTBALL_API_KEY = os.environ.get("FOOTBALL_API_KEY")  # for on-demand match event lookups
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")  # for real embedded highlights (free tier, ~100 searches/day)

# Simple shared-secret protection. Set this in Render's environment variables
# and pass the same value as a header from your dashboard: X-API-Key: <value>
# Leave API_ACCESS_KEY unset to disable the check (useful for local testing).
API_ACCESS_KEY = os.environ.get("API_ACCESS_KEY")

app = FastAPI(title="Cross-League Scouting API")

# Restrict to your actual deployed frontend domain(s) rather than "*".
# Add more origins to this list as you deploy the dashboard elsewhere.
ALLOWED_ORIGINS = [
    "https://scoutindex.netlify.app",
    "https://ryanwzrd.github.io",
    "http://localhost:3000",  # local testing
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def check_api_key(x_api_key: Optional[str] = Header(None)):
    if API_ACCESS_KEY and x_api_key != API_ACCESS_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return True


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


ALLOWED_IMAGE_HOSTS = {"media.api-sports.io"}


@app.get("/image-proxy")
def image_proxy(url: str):
    """Re-serves a player photo from our own domain with permissive CORS
    headers. Needed because html2canvas can't safely export cross-origin
    images onto a canvas unless the source server allows it — API-Football's
    photo CDN doesn't, so exported share-card images showed a blank circle
    instead of the real photo. Restricted to a known trusted host to avoid
    this becoming an open image-fetching proxy."""
    from urllib.parse import urlparse
    host = urlparse(url).hostname
    if host not in ALLOWED_IMAGE_HOSTS:
        raise HTTPException(status_code=400, detail="URL host not allowed")
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch image: {e}")
    return Response(
        content=resp.content,
        media_type=resp.headers.get("content-type", "image/png"),
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=86400"},
    )


@app.get("/status")
def data_status():
    """When the pipeline last actually completed — scoring_model.py runs
    last in the nightly workflow and stamps computed_at on every scored
    player, so its max value is a reliable "data last refreshed at" marker."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(computed_at) AS last_updated FROM player_potential_scores")
        row = cur.fetchone()
        cur.execute("SELECT count(*) AS cnt FROM player_potential_scores")
        count_row = cur.fetchone()
    conn.close()
    return {"last_updated": row["last_updated"], "scored_players": count_row["cnt"]}


# Our 17 tracked league external IDs, for filtering the global live-scores response
TRACKED_LEAGUE_IDS = {39, 140, 78, 135, 61, 88, 94, 203, 71, 98, 253, 179, 62, 40, 144, 262, 128}


def get_flag_url(cur, country_name):
    """Cached flag lookup — checks our own table first, only ever calls
    the free REST Countries API (no key needed) for a country we've
    genuinely never seen before, then caches it permanently."""
    cur.execute("SELECT flag_url FROM country_flags WHERE country_name = %s", (country_name,))
    row = cur.fetchone()
    if row:
        return row["flag_url"]
    try:
        resp = requests.get(f"https://restcountries.com/v3.1/name/{country_name}", params={"fields": "flags"}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            flag_url = data[0].get("flags", {}).get("png") if data else None
        else:
            flag_url = None
    except Exception:
        flag_url = None
    cur.execute(
        "INSERT INTO country_flags (country_name, flag_url) VALUES (%s, %s) "
        "ON CONFLICT (country_name) DO UPDATE SET flag_url = EXCLUDED.flag_url",
        (country_name, flag_url),
    )
    return flag_url


def classify_archetype(position, pr):
    """Shared, single source of truth for archetype rules — used by both
    the player dossier and Team of the Season, so the two never drift out
    of sync with slightly different logic. `pr` is a dict of percentile
    ranks (0-100) on: goals, assists, key_passes, defensive, take_ons, pass_acc."""
    if position == "Attacker":
        if pr["goals"] >= 70 and pr["key_passes"] < 50 and pr["take_ons"] < 50:
            return "Poacher"
        elif pr["assists"] >= 65 or pr["key_passes"] >= 70:
            return "Creator"
        elif pr["take_ons"] >= 70:
            return "Dribbler / Winger"
        return "All-Round Forward"
    elif position == "Midfielder":
        if pr["defensive"] >= 70 and (pr["goals"] + pr["assists"]) < 80:
            return "Defensive Midfielder"
        elif pr["key_passes"] >= 70 and pr["pass_acc"] >= 60:
            return "Playmaker"
        elif pr["defensive"] >= 55 and (pr["goals"] >= 50 or pr["assists"] >= 50):
            return "Box-to-Box Midfielder"
        return "All-Round Midfielder"
    elif position == "Defender":
        if pr["pass_acc"] >= 70 and pr["defensive"] < 60:
            return "Ball-Playing Defender"
        elif pr["defensive"] >= 65:
            return "Stopper"
        return "All-Round Defender"
    elif position == "Goalkeeper":
        return "Sweeper-Keeper" if pr["pass_acc"] >= 65 else "Shot-Stopper"
    return None


def percentile_rank(target_val, all_vals):
    """What percentage of a peer group a value beats — used for archetype
    classification. Returns a neutral 50 if we can't compute it honestly
    (missing data), rather than a misleadingly confident number."""
    if target_val is None or not all_vals:
        return 50
    below = sum(1 for v in all_vals if v is not None and v < target_val)
    comparable = [v for v in all_vals if v is not None]
    return round((below / len(comparable)) * 100, 1) if comparable else 50



# Simple in-memory cache — protects against rapid repeated calls (e.g. quick
# tab-switching) from each triggering a fresh API-Football request. Resets
# on server restart, which is fine for something this short-lived.
_live_cache = {"data": None, "fetched_at": 0}
LIVE_CACHE_SECONDS = 20


@app.get("/live")
def live_scores(authorized: bool = Depends(check_api_key)):
    """Currently in-progress matches across all tracked leagues, in ONE
    API-Football request (their live=all endpoint returns everything at
    once, regardless of league count — cost doesn't scale with coverage).
    Cached briefly server-side as an extra safety buffer."""
    if not FOOTBALL_API_KEY:
        raise HTTPException(status_code=503, detail="FOOTBALL_API_KEY not configured on the server.")

    now = time.time()
    if _live_cache["data"] is not None and (now - _live_cache["fetched_at"]) < LIVE_CACHE_SECONDS:
        return {"matches": _live_cache["data"], "cached": True}

    try:
        resp = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"live": "all"},
            timeout=10,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        raw = resp.json().get("response", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch live scores: {e}")

    matches = []
    for f in raw:
        if f["league"]["id"] not in TRACKED_LEAGUE_IDS:
            continue
        matches.append({
            "league": f["league"]["name"],
            "home_club": f["teams"]["home"]["name"],
            "away_club": f["teams"]["away"]["name"],
            "home_score": f["goals"]["home"],
            "away_score": f["goals"]["away"],
            "elapsed": f["fixture"]["status"]["elapsed"],
            "status_short": f["fixture"]["status"]["short"],
        })

    _live_cache["data"] = matches
    _live_cache["fetched_at"] = now
    return {"matches": matches, "cached": False}


@app.get("/clubs/record")
def club_record(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """This club's W-D-L record this season, derived from existing match
    results — same logic as /standings, scoped to one club."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        cur.execute("""
            WITH club_matches AS (
                SELECT home_club_id AS club_id, home_score AS gf, away_score AS ga,
                    CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
                    CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS draw,
                    CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss
                FROM matches m JOIN clubs c ON c.id = m.home_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND c.name = %s
                UNION ALL
                SELECT away_club_id, away_score, home_score,
                    CASE WHEN away_score > home_score THEN 1 ELSE 0 END,
                    CASE WHEN away_score = home_score THEN 1 ELSE 0 END,
                    CASE WHEN away_score < home_score THEN 1 ELSE 0 END
                FROM matches m JOIN clubs c ON c.id = m.away_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND c.name = %s
            )
            SELECT COUNT(*) AS played, SUM(win) AS won, SUM(draw) AS drawn, SUM(loss) AS lost,
                   SUM(gf) AS gf, SUM(ga) AS ga
            FROM club_matches
        """, (league_id, club, league_id, club))
        record = cur.fetchone()
    conn.close()
    return record


@app.get("/players/most-improved")
def most_improved(limit: int = Query(10, le=50), authorized: bool = Depends(check_api_key)):
    """Players whose potential score has risen the most since trend
    tracking began — genuinely unique, using accumulated history data
    (needs 2+ tracked snapshots per player to show anything)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH bounds AS (
                SELECT player_id, MIN(computed_at) AS first_at, MAX(computed_at) AS last_at
                FROM player_potential_history
                GROUP BY player_id
                HAVING COUNT(*) >= 2
            ),
            first_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS first_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.first_at
            ),
            last_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS last_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.last_at
            )
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   fv.first_val, lv.last_val, (lv.last_val - fv.first_val) AS delta
            FROM first_vals fv
            JOIN last_vals lv ON lv.player_id = fv.player_id
            JOIN players p ON p.id = fv.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (lv.last_val - fv.first_val) > 0
            ORDER BY delta DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/shortlist/alerts")
def shortlist_alerts(limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Currently-shortlisted players who just had a standout match (high
    rating, a goal, or an assist) — surfaces what actually deserves your
    attention rather than making you check every player individually."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM (
                SELECT DISTINCT ON (p.id)
                    p.id, p.full_name, p.photo_url, cl.name AS club,
                    pms.rating, pms.goals, pms.assists, m.match_date,
                    CASE WHEN m.home_club_id = pms.club_id THEN away_cl.name ELSE home_cl.name END AS opponent
                FROM players p
                LEFT JOIN clubs cl ON cl.id = p.current_club_id
                JOIN LATERAL (
                    SELECT watch_level FROM scout_notes sn
                    WHERE sn.player_id = p.id
                    ORDER BY created_at DESC LIMIT 1
                ) latest_note ON true
                JOIN player_match_stats pms ON pms.player_id = p.id
                JOIN matches m ON m.id = pms.match_id
                LEFT JOIN clubs home_cl ON home_cl.id = m.home_club_id
                LEFT JOIN clubs away_cl ON away_cl.id = m.away_club_id
                WHERE latest_note.watch_level = 'shortlist'
                  AND (pms.rating >= 7.5 OR pms.goals >= 1 OR pms.assists >= 1)
                ORDER BY p.id, m.match_date DESC
            ) per_player_latest
            ORDER BY match_date DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return sorted(rows, key=lambda r: r["match_date"], reverse=True)


@app.get("/team-of-season")
def team_of_season(league: str, authorized: bool = Depends(check_api_key)):
    """Top-ranked players by potential per position within a league —
    enough per position (up to 8) to fill any formation, letting the
    frontend slot them in based on whichever formation is selected. Each
    candidate also includes their Tactical Archetype, computed against a
    shared peer group per position (fetched once, not once per candidate)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        result = {}
        for position, take in [("Goalkeeper", 3), ("Defender", 8), ("Midfielder", 8), ("Attacker", 6)]:
            cur.execute("""
                SELECT p.id, p.full_name, p.photo_url, pps.potential_index
                FROM players p
                JOIN clubs cl ON cl.id = p.current_club_id
                LEFT JOIN LATERAL (
                    SELECT potential_index FROM player_potential_scores
                    WHERE player_id = p.id ORDER BY season DESC LIMIT 1
                ) pps ON true
                WHERE cl.league_id = %s AND p.primary_position = %s AND pps.potential_index IS NOT NULL
                ORDER BY pps.potential_index DESC
                LIMIT %s
            """, (league_id, position, take))
            candidates = cur.fetchall()

            # Peer group fetched ONCE per position, shared across all of
            # this position's candidates — much cheaper than recomputing
            # per player, and archetype rules are identical to the dossier's.
            cur.execute("""
                SELECT player_id,
                       SUM(goals) * 90.0 / NULLIF(SUM(minutes_played), 0) AS goals_p90,
                       SUM(assists) * 90.0 / NULLIF(SUM(minutes_played), 0) AS assists_p90,
                       SUM(key_passes) * 90.0 / NULLIF(SUM(minutes_played), 0) AS key_passes_p90,
                       SUM(tackles + interceptions) * 90.0 / NULLIF(SUM(minutes_played), 0) AS defensive_p90,
                       SUM(take_ons_attempted) * 90.0 / NULLIF(SUM(minutes_played), 0) AS take_ons_p90,
                       AVG(NULLIF(passes_completed, 0)::float / NULLIF(passes_attempted, 0)) * 100 AS pass_acc
                FROM player_match_stats pms
                JOIN players p3 ON p3.id = pms.player_id
                WHERE p3.primary_position = %s
                GROUP BY player_id
                HAVING SUM(minutes_played) >= 450
            """, (position,))
            peer_rows = cur.fetchall()
            peer_by_id = {r["player_id"]: r for r in peer_rows}

            for c in candidates:
                target_row = peer_by_id.get(c["id"])
                if target_row and len(peer_rows) >= 10:
                    pr = {
                        "goals": percentile_rank(target_row["goals_p90"], [r["goals_p90"] for r in peer_rows]),
                        "assists": percentile_rank(target_row["assists_p90"], [r["assists_p90"] for r in peer_rows]),
                        "key_passes": percentile_rank(target_row["key_passes_p90"], [r["key_passes_p90"] for r in peer_rows]),
                        "defensive": percentile_rank(target_row["defensive_p90"], [r["defensive_p90"] for r in peer_rows]),
                        "take_ons": percentile_rank(target_row["take_ons_p90"], [r["take_ons_p90"] for r in peer_rows]),
                        "pass_acc": percentile_rank(target_row["pass_acc"], [r["pass_acc"] for r in peer_rows]),
                    }
                    c["archetype"] = classify_archetype(position, pr)
                else:
                    c["archetype"] = None

            result[position] = candidates
    conn.close()
    return result


@app.get("/players/clean-sheets")
def clean_sheets(limit: int = Query(8, le=20), authorized: bool = Depends(check_api_key)):
    """Goalkeepers ranked by clean sheets this season (matches with 0 goals
    conceded, playing at least 60 minutes to count as a real appearance).
    Free — derived entirely from existing match_stats data."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   COUNT(*) AS clean_sheets
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            LEFT JOIN clubs cl ON cl.id = pms.club_id
            WHERE p.primary_position = 'Goalkeeper'
              AND pms.goals_conceded = 0 AND pms.minutes_played >= 60
            GROUP BY p.id, p.full_name, p.photo_url, cl.name
            ORDER BY clean_sheets DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/debuts")
def debut_tracker(limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Players with exactly ONE match appearance this season — a genuine
    debut, not just someone with limited minutes. Ordered by most recent."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH single_appearance AS (
                SELECT pms.player_id, MIN(pms.id) AS stat_id, COUNT(*) AS appearances
                FROM player_match_stats pms
                WHERE pms.minutes_played > 0
                GROUP BY pms.player_id
                HAVING COUNT(*) = 1
            )
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   m.match_date, pms.minutes_played, pms.rating
            FROM single_appearance sa
            JOIN player_match_stats pms ON pms.id = sa.stat_id
            JOIN players p ON p.id = sa.player_id
            JOIN matches m ON m.id = pms.match_id
            LEFT JOIN clubs cl ON cl.id = pms.club_id
            LEFT JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            ORDER BY m.match_date DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/most-capped")
def most_capped(limit: int = Query(8, le=20), authorized: bool = Depends(check_api_key)):
    """Players with the most real international caps — genuinely new data,
    a quality signal completely separate from club performance."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   pic.team_name, SUM(pic.appearances) AS total_caps
            FROM player_international_caps pic
            JOIN players p ON p.id = pic.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, pic.team_name
            ORDER BY total_caps DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/home-away")
def home_away_split(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """A club's record split by home vs away — free, derived entirely from
    existing match results."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        def split_record(is_home):
            side = "home" if is_home else "away"
            other = "away" if is_home else "home"
            cur.execute(f"""
                SELECT COUNT(*) AS played,
                    SUM(CASE WHEN {side}_score > {other}_score THEN 1 ELSE 0 END) AS won,
                    SUM(CASE WHEN {side}_score = {other}_score THEN 1 ELSE 0 END) AS drawn,
                    SUM(CASE WHEN {side}_score < {other}_score THEN 1 ELSE 0 END) AS lost,
                    SUM({side}_score) AS gf, SUM({other}_score) AS ga
                FROM matches m
                JOIN clubs c ON c.id = m.{side}_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND c.name = %s
            """, (league_id, club))
            return cur.fetchone()

        home_record = split_record(True)
        away_record = split_record(False)
    conn.close()
    return {"home": home_record, "away": away_record}


@app.get("/players/{player_id}/projection")
def player_projection(player_id: int, authorized: bool = Depends(check_api_key)):
    """A simple linear-trend projection from accumulated potential-score
    history. Deliberately conservative about confidence — trend tracking
    only recently began, so early results should read as illustrative,
    not a real prediction. Returns available=False until there's enough
    history for this to mean anything at all."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT potential_index, computed_at FROM player_potential_history
            WHERE player_id = %s ORDER BY computed_at ASC
        """, (player_id,))
        points = cur.fetchall()
    conn.close()

    if len(points) < 5:
        return {"available": False, "days_tracked": 0, "points_tracked": len(points)}

    first_at = points[0]["computed_at"]
    xs = [(p["computed_at"] - first_at).total_seconds() / 86400 for p in points]
    ys = [p["potential_index"] for p in points]
    n = len(points)
    sum_x, sum_y = sum(xs), sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_x2 = sum(x * x for x in xs)
    denom = (n * sum_x2 - sum_x ** 2)
    if denom == 0:
        return {"available": False, "days_tracked": 0, "points_tracked": len(points)}
    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    days_tracked = xs[-1]
    projected_90d = max(0, min(100, intercept + slope * (xs[-1] + 90)))
    confidence = "low" if days_tracked < 14 else "moderate" if days_tracked < 30 else "reasonable"

    return {
        "available": True,
        "current": ys[-1],
        "projected_90d": round(projected_90d, 1),
        "days_tracked": round(days_tracked, 1),
        "points_tracked": n,
        "confidence": confidence,
    }


@app.get("/players/breakout-candidates")
def breakout_candidates(limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """A composite signal combining several things separately: rising
    trend, youth relative to current quality, and strong per-90 output
    despite limited minutes (a real debut-era efficiency signal, not
    padded by a huge sample). Computed in clear Python steps rather than
    one dense SQL query, specifically so the logic is easy to review.

    Weights: 40% current potential (a real floor of quality), 20% youth
    bonus (age <=23 scaled), 20% recent trend improvement (if tracked),
    20% output-per-90 efficiency at limited minutes (rewards flashes of
    real quality, not just accumulated stats over a full season)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   pps.potential_index, p.date_of_birth,
                   stats.goals, stats.assists, stats.minutes_played
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN LATERAL (
                SELECT SUM(goals) AS goals, SUM(assists) AS assists, SUM(minutes_played) AS minutes_played
                FROM player_match_stats WHERE player_id = p.id
            ) stats ON true
            WHERE pps.potential_index IS NOT NULL AND stats.minutes_played BETWEEN 180 AND 1500
        """)
        candidates = cur.fetchall()

        cur.execute("""
            SELECT player_id,
                   (SELECT potential_index FROM player_potential_history h2
                    WHERE h2.player_id = h1.player_id ORDER BY computed_at ASC LIMIT 1) AS earliest_val,
                   (SELECT potential_index FROM player_potential_history h2
                    WHERE h2.player_id = h1.player_id ORDER BY computed_at DESC LIMIT 1) AS latest_val
            FROM player_potential_history h1
            GROUP BY player_id
        """)
        trend_by_player = {row["player_id"]: (row["latest_val"] - row["earliest_val"]) for row in cur.fetchall()}
    conn.close()

    scored = []
    for c in candidates:
        age = None
        if c["date_of_birth"]:
            age = (datetime.now().date() - c["date_of_birth"]).days / 365.25
        age_bonus = max(0, min(100, (23 - age) * (100 / 7))) if age is not None else 0  # scaled so age 16 (youngest realistic pro) ≈ 100, age 23+ = 0

        trend_delta = trend_by_player.get(c["id"], 0)
        trend_bonus = max(0, min(100, trend_delta * 5))  # a +20 potential swing maxes this out

        minutes = c["minutes_played"] or 1
        output_per90 = ((c["goals"] or 0) + (c["assists"] or 0)) * 90 / minutes
        efficiency_bonus = min(100, output_per90 * 50)  # ~2 contributions per 90 maxes this out

        breakout_score = (
            (c["potential_index"] or 0) * 0.4
            + age_bonus * 0.2
            + trend_bonus * 0.2
            + efficiency_bonus * 0.2
        )
        scored.append({**c, "breakout_score": round(breakout_score, 1)})

    scored.sort(key=lambda r: r["breakout_score"], reverse=True)
    return scored[:limit]


@app.get("/clubs/tactical-fit")
def tactical_fit(club: str, league: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Infers a club's real playing style from their own squad's average
    per-90 numbers — possession tendency (pass accuracy, pass volume) and
    combativeness (tackles+interceptions per 90) — then ranks OTHER
    players across the database by how closely their own profile matches
    it. A genuinely different kind of insight than raw ability: who would
    actually suit THIS club's system, not just who's good in general.

    Deliberately simple, 2-axis similarity (euclidean distance) rather
    than an opaque black-box score — the two axes are visible in the
    response so the fit is explainable, not just a mystery number."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        # Club's own squad profile — averaged across players with a
        # meaningful sample (180+ minutes), so one cameo doesn't skew it.
        cur.execute("""
            SELECT AVG(stats.pass_accuracy_pct) AS avg_pass_acc,
                   AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS avg_combativeness
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                       SUM(minutes_played) AS minutes_played,
                       AVG(pass_accuracy_pct) AS pass_accuracy_pct
                FROM player_match_stats WHERE player_id = p.id
            ) stats ON true
            WHERE cl.name = %s AND cl.league_id = %s AND stats.minutes_played >= 180
        """, (club, league_id))
        club_profile = cur.fetchone()

        if not club_profile or club_profile["avg_pass_acc"] is None:
            conn.close()
            return {"club_profile": None, "candidates": []}

        # Every tracked player's own profile, same two axes, excluding
        # this club's own players (no point suggesting a transfer target
        # who's already there).
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club, p.primary_position,
                   AVG(stats.pass_accuracy_pct) AS pass_acc,
                   AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS combativeness,
                   pps.potential_index
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                       SUM(minutes_played) AS minutes_played,
                       AVG(pass_accuracy_pct) AS pass_accuracy_pct
                FROM player_match_stats WHERE player_id = p.id
            ) stats ON true
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE stats.minutes_played >= 450 AND cl.name != %s AND pps.potential_index IS NOT NULL
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, p.primary_position, pps.potential_index
            HAVING AVG(stats.pass_accuracy_pct) IS NOT NULL
        """, (club,))
        candidates = cur.fetchall()
    conn.close()

    target_pass = club_profile["avg_pass_acc"]
    target_comb = club_profile["avg_combativeness"] or 0

    # Z-score normalize both axes across the candidate pool before
    # computing distance — pass accuracy (~0-100 range) and combativeness
    # (~0-10 range) are on very different scales, so a raw euclidean
    # distance would let pass accuracy dominate almost entirely even when
    # both differences are equally realistic. Normalizing first means each
    # axis contributes based on how many standard deviations away it is,
    # not its raw numeric size.
    pass_vals = [c["pass_acc"] for c in candidates if c["pass_acc"] is not None]
    comb_vals = [c["combativeness"] or 0 for c in candidates]
    pass_mean, pass_std = (sum(pass_vals) / len(pass_vals), (sum((v - sum(pass_vals) / len(pass_vals)) ** 2 for v in pass_vals) / len(pass_vals)) ** 0.5) if pass_vals else (0, 1)
    comb_mean, comb_std = (sum(comb_vals) / len(comb_vals), (sum((v - sum(comb_vals) / len(comb_vals)) ** 2 for v in comb_vals) / len(comb_vals)) ** 0.5) if comb_vals else (0, 1)
    pass_std = pass_std or 1  # avoid division by zero if every candidate is identical
    comb_std = comb_std or 1

    scored = []
    for c in candidates:
        pass_diff = ((c["pass_acc"] or 0) - target_pass) / pass_std
        comb_diff = ((c["combativeness"] or 0) - target_comb) / comb_std
        distance = (pass_diff ** 2 + comb_diff ** 2) ** 0.5
        scored.append({**c, "fit_distance": round(distance, 2)})

    scored.sort(key=lambda r: r["fit_distance"])
    return {
        "club_profile": {"avg_pass_accuracy": round(target_pass, 1), "avg_combativeness": round(target_comb, 2)},
        "candidates": scored[:limit],
    }


@app.get("/clubs/recruitment-priorities")
def recruitment_priorities(club: str, league: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """The genuine synthesis feature — combines tactical fit, raw ability,
    how much we actually trust that ability (confidence), and upside (youth)
    into ONE ranked, actionable list: who should this club realistically be
    looking at right now. Weights: 35% tactical fit, 35% potential, 15%
    confidence, 15% youth — deliberately balanced so no single factor alone
    can carry a recommendation (a great tactical fit with almost no real
    minutes still won't rank highly, and vice versa)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        cur.execute("""
            SELECT AVG(stats.pass_accuracy_pct) AS avg_pass_acc,
                   AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS avg_combativeness
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                       SUM(minutes_played) AS minutes_played,
                       AVG(pass_accuracy_pct) AS pass_accuracy_pct
                FROM player_match_stats WHERE player_id = p.id
            ) stats ON true
            WHERE cl.name = %s AND cl.league_id = %s AND stats.minutes_played >= 180
        """, (club, league_id))
        club_profile = cur.fetchone()
        if not club_profile or club_profile["avg_pass_acc"] is None:
            conn.close()
            return {"club_profile": None, "candidates": []}

        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club, p.primary_position, p.date_of_birth,
                   AVG(stats.pass_accuracy_pct) AS pass_acc,
                   AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS combativeness,
                   SUM(stats.minutes_played) AS total_minutes,
                   pps.potential_index
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                       SUM(minutes_played) AS minutes_played,
                       AVG(pass_accuracy_pct) AS pass_accuracy_pct
                FROM player_match_stats WHERE player_id = p.id
            ) stats ON true
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE stats.minutes_played >= 450 AND cl.name != %s AND pps.potential_index IS NOT NULL
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, p.primary_position, p.date_of_birth, pps.potential_index
            HAVING AVG(stats.pass_accuracy_pct) IS NOT NULL
        """, (club,))
        candidates = cur.fetchall()
    conn.close()

    target_pass = club_profile["avg_pass_acc"]
    target_comb = club_profile["avg_combativeness"] or 0

    pass_vals = [c["pass_acc"] for c in candidates if c["pass_acc"] is not None]
    comb_vals = [c["combativeness"] or 0 for c in candidates]
    pass_mean = sum(pass_vals) / len(pass_vals) if pass_vals else 0
    pass_std = ((sum((v - pass_mean) ** 2 for v in pass_vals) / len(pass_vals)) ** 0.5) if pass_vals else 1
    comb_mean = sum(comb_vals) / len(comb_vals) if comb_vals else 0
    comb_std = ((sum((v - comb_mean) ** 2 for v in comb_vals) / len(comb_vals)) ** 0.5) if comb_vals else 1
    pass_std = pass_std or 1
    comb_std = comb_std or 1

    scored = []
    for c in candidates:
        pass_diff = ((c["pass_acc"] or 0) - target_pass) / pass_std
        comb_diff = ((c["combativeness"] or 0) - target_comb) / comb_std
        distance = (pass_diff ** 2 + comb_diff ** 2) ** 0.5
        fit_score = max(0, 100 - distance * 25)

        minutes = c["total_minutes"] or 0
        confidence_bonus = 100 if minutes >= 1800 else 75 if minutes >= 900 else 50 if minutes >= 300 else 25

        age = (datetime.now().date() - c["date_of_birth"]).days / 365.25 if c["date_of_birth"] else None
        youth_bonus = max(0, min(100, (23 - age) * (100 / 7))) if age is not None else 50

        composite = (
            fit_score * 0.35
            + (c["potential_index"] or 0) * 0.35
            + confidence_bonus * 0.15
            + youth_bonus * 0.15
        )
        scored.append({
            "id": c["id"], "full_name": c["full_name"], "photo_url": c["photo_url"],
            "club": c["club"], "position": c["primary_position"],
            "potential_index": round(c["potential_index"], 1) if c["potential_index"] else None,
            "priority_score": round(composite, 1),
        })

    scored.sort(key=lambda r: r["priority_score"], reverse=True)
    return {
        "club_profile": {"avg_pass_accuracy": round(target_pass, 1), "avg_combativeness": round(target_comb, 2)},
        "candidates": scored[:limit],
    }


def detect_squad_needs(cur, club, league_id):
    """Which position groups are genuinely thin for a club — same
    thresholds as the client-side depth warning already shown in Club
    Profile, kept consistent so the two never disagree with each other."""
    cur.execute("""
        SELECT p.primary_position, COUNT(*) AS n
        FROM players p
        JOIN clubs cl ON cl.id = p.current_club_id
        WHERE cl.name = %s AND cl.league_id = %s AND p.primary_position IS NOT NULL
        GROUP BY p.primary_position
    """, (club, league_id))
    counts = {r["primary_position"]: r["n"] for r in cur.fetchall()}
    thresholds = {"Goalkeeper": 2, "Defender": 4, "Midfielder": 4, "Attacker": 4}
    return [pos for pos, threshold in thresholds.items() if counts.get(pos, 0) <= threshold]


class IntelligenceFeedRequest(BaseModel):
    favorites: list  # [{"club": str, "league": str}, ...]


@app.post("/intelligence-feed")
def intelligence_feed(body: IntelligenceFeedRequest, authorized: bool = Depends(check_api_key)):
    """The genuine synthesis feature: for every club you've favorited,
    automatically detects real squad needs (thin positions) and
    cross-references them against actual recruitment candidates who fit
    BOTH that specific position AND the club's tactical style — not just
    generically good players. This is the first feature that reasons
    across several other features rather than being a standalone signal."""
    conn = get_conn()
    results = []
    with conn.cursor() as cur:
        for fav in body.favorites[:10]:  # cap to keep this fast and bounded
            club, league = fav.get("club"), fav.get("league")
            if not club or not league:
                continue

            cur.execute("""
                SELECT l.id FROM leagues l
                LEFT JOIN countries co ON co.id = l.country_id
                WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            """, (league,))
            row = cur.fetchone()
            if not row:
                continue
            league_id = row["id"]

            needs = detect_squad_needs(cur, club, league_id)
            if not needs:
                continue

            # Club's own tactical profile, same logic as recruitment-priorities.
            cur.execute("""
                SELECT AVG(stats.pass_accuracy_pct) AS avg_pass_acc,
                       AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS avg_combativeness
                FROM players p
                JOIN clubs cl ON cl.id = p.current_club_id
                JOIN LATERAL (
                    SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                           SUM(minutes_played) AS minutes_played, AVG(pass_accuracy_pct) AS pass_accuracy_pct
                    FROM player_match_stats WHERE player_id = p.id
                ) stats ON true
                WHERE cl.name = %s AND cl.league_id = %s AND stats.minutes_played >= 180
            """, (club, league_id))
            club_profile = cur.fetchone()
            if not club_profile or club_profile["avg_pass_acc"] is None:
                continue
            target_pass = club_profile["avg_pass_acc"]
            target_comb = club_profile["avg_combativeness"] or 0

            club_recommendations = []
            for position in needs:
                cur.execute("""
                    SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                           AVG(stats.pass_accuracy_pct) AS pass_acc,
                           AVG((stats.tackles + stats.interceptions) * 90.0 / NULLIF(stats.minutes_played, 0)) AS combativeness,
                           pps.potential_index
                    FROM players p
                    JOIN clubs cl ON cl.id = p.current_club_id
                    JOIN LATERAL (
                        SELECT SUM(tackles) AS tackles, SUM(interceptions) AS interceptions,
                               SUM(minutes_played) AS minutes_played, AVG(pass_accuracy_pct) AS pass_accuracy_pct
                        FROM player_match_stats WHERE player_id = p.id
                    ) stats ON true
                    LEFT JOIN LATERAL (
                        SELECT potential_index FROM player_potential_scores
                        WHERE player_id = p.id ORDER BY season DESC LIMIT 1
                    ) pps ON true
                    WHERE stats.minutes_played >= 450 AND cl.name != %s
                      AND p.primary_position = %s AND pps.potential_index IS NOT NULL
                    GROUP BY p.id, p.full_name, p.photo_url, cl.name, pps.potential_index
                    HAVING AVG(stats.pass_accuracy_pct) IS NOT NULL
                """, (club, position))
                candidates = cur.fetchall()
                if not candidates:
                    continue

                pass_vals = [c["pass_acc"] for c in candidates if c["pass_acc"] is not None]
                comb_vals = [c["combativeness"] or 0 for c in candidates]
                pass_mean = sum(pass_vals) / len(pass_vals) if pass_vals else 0
                pass_std = ((sum((v - pass_mean) ** 2 for v in pass_vals) / len(pass_vals)) ** 0.5) if pass_vals else 1
                comb_mean = sum(comb_vals) / len(comb_vals) if comb_vals else 0
                comb_std = ((sum((v - comb_mean) ** 2 for v in comb_vals) / len(comb_vals)) ** 0.5) if comb_vals else 1
                pass_std = pass_std or 1
                comb_std = comb_std or 1

                best = None
                best_score = -1
                for c in candidates:
                    pass_diff = ((c["pass_acc"] or 0) - target_pass) / pass_std
                    comb_diff = ((c["combativeness"] or 0) - target_comb) / comb_std
                    distance = (pass_diff ** 2 + comb_diff ** 2) ** 0.5
                    fit_score = max(0, 100 - distance * 25)
                    composite = fit_score * 0.5 + (c["potential_index"] or 0) * 0.5
                    if composite > best_score:
                        best_score = composite
                        best = c

                if best:
                    club_recommendations.append({
                        "position_needed": position,
                        "player": {"id": best["id"], "full_name": best["full_name"], "photo_url": best["photo_url"], "club": best["club"]},
                        "match_score": round(best_score, 1),
                    })

            if club_recommendations:
                results.append({"club": club, "league": league, "needs": needs, "recommendations": club_recommendations})

    conn.close()
    return results


@app.get("/scout/track-record")
def scout_track_record(authorized: bool = Depends(check_api_key)):
    """The first feature about YOUR judgment, not the players' — for every
    player you've ever shortlisted, checks whether their potential has
    genuinely risen since you flagged them, and whether they've since
    moved clubs (a real signal someone else noticed them too). Honest
    caveat: trend history only recently started being tracked, so the
    'delta since shortlisting' will be small/near-zero for most players
    right now — this becomes genuinely meaningful over the coming weeks
    as both shortlisting activity and trend history accumulate."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH first_shortlisted AS (
                SELECT player_id, MIN(created_at) AS shortlisted_at
                FROM scout_notes WHERE watch_level = 'shortlist'
                GROUP BY player_id
            )
            SELECT fs.player_id, fs.shortlisted_at, p.full_name, p.photo_url, cl.name AS club,
                   pps.potential_index AS current_potential,
                   (SELECT h.potential_index FROM player_potential_history h
                    WHERE h.player_id = fs.player_id AND h.computed_at >= fs.shortlisted_at
                    ORDER BY h.computed_at ASC LIMIT 1) AS potential_at_shortlisting,
                   (SELECT COUNT(*) FROM player_club_transfers t
                    WHERE t.player_id = fs.player_id AND t.changed_at > fs.shortlisted_at) AS transfers_since
            FROM first_shortlisted fs
            JOIN players p ON p.id = fs.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = fs.player_id ORDER BY season DESC LIMIT 1
            ) pps ON true
            ORDER BY fs.shortlisted_at DESC
        """)
        rows = cur.fetchall()
    conn.close()

    players = []
    deltas = []
    for r in rows:
        delta = None
        if r["current_potential"] is not None and r["potential_at_shortlisting"] is not None:
            delta = round(r["current_potential"] - r["potential_at_shortlisting"], 1)
            deltas.append(delta)
        players.append({
            "id": r["player_id"], "full_name": r["full_name"], "photo_url": r["photo_url"], "club": r["club"],
            "shortlisted_at": r["shortlisted_at"],
            "current_potential": round(r["current_potential"], 1) if r["current_potential"] is not None else None,
            "delta_since_shortlisting": delta,
            "moved_clubs_since": r["transfers_since"] > 0,
        })

    summary = {
        "total_shortlisted": len(players),
        "with_trend_data": len(deltas),
        "avg_delta": round(sum(deltas) / len(deltas), 1) if deltas else None,
        "risen_count": sum(1 for d in deltas if d > 0),
        "moved_clubs_count": sum(1 for p in players if p["moved_clubs_since"]),
    }
    return {"summary": summary, "players": players}


@app.get("/scout/discovery")
def scout_discovery(limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """A genuine recommendation engine: infers a real scouting profile from
    everyone you've already shortlisted (typical position, age range,
    potential tier), then searches for players who match that SAME
    profile but aren't shortlisted yet. Not generic 'best players' — this
    is shaped by your own demonstrated taste. Needs at least 3 shortlisted
    players with real position/age data to infer a meaningful profile."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (p.id) p.id, p.primary_position, p.date_of_birth, pps.potential_index
            FROM players p
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE latest_note.watch_level = 'shortlist'
        """)
        shortlisted = cur.fetchall()

        usable = [s for s in shortlisted if s["primary_position"] and s["date_of_birth"] and s["potential_index"] is not None]
        if len(usable) < 3:
            conn.close()
            return {"profile": None, "discoveries": [],
                    "message": f"Shortlist {3 - len(usable)} more player{'s' if 3 - len(usable) != 1 else ''} with known position/age to unlock this — needs a real pattern to learn from."}

        # Infer the profile: most common position, age range, potential tier.
        position_counts = {}
        for s in usable:
            position_counts[s["primary_position"]] = position_counts.get(s["primary_position"], 0) + 1
        top_position = max(position_counts, key=position_counts.get)

        ages = [(datetime.now().date() - s["date_of_birth"]).days / 365.25 for s in usable]
        avg_age = sum(ages) / len(ages)
        age_min, age_max = max(15, avg_age - 4), avg_age + 4

        potentials = [s["potential_index"] for s in usable]
        avg_potential = sum(potentials) / len(potentials)
        potential_floor = max(0, avg_potential - 15)  # a reasonable band around your historical picks

        already_shortlisted_ids = {s["id"] for s in shortlisted}

        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   pps.potential_index, p.date_of_birth
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.primary_position = %s AND pps.potential_index >= %s
              AND p.date_of_birth IS NOT NULL
            ORDER BY pps.potential_index DESC
            LIMIT 200
        """, (top_position, potential_floor))
        candidates = cur.fetchall()
    conn.close()

    discoveries = []
    for c in candidates:
        if c["id"] in already_shortlisted_ids:
            continue
        age = (datetime.now().date() - c["date_of_birth"]).days / 365.25
        if not (age_min <= age <= age_max):
            continue
        discoveries.append({
            "id": c["id"], "full_name": c["full_name"], "photo_url": c["photo_url"],
            "club": c["club"], "league": c["league_display"],
            "potential_index": round(c["potential_index"], 1), "age": round(age, 1),
        })
        if len(discoveries) >= limit:
            break

    return {
        "profile": {
            "typical_position": top_position,
            "typical_age_range": f"{round(age_min)}-{round(age_max)}",
            "typical_potential_tier": round(avg_potential, 1),
            "based_on": len(usable),
        },
        "discoveries": discoveries,
    }


@app.get("/leagues/strength")
def league_strength(authorized: bool = Depends(check_api_key)):
    """Average potential score per tracked league — genuine meta-context
    for comparing quality across your 17 leagues, not just within one.
    Requires 20+ scored players in a league to appear, so a league that's
    barely been ingested yet doesn't show a misleadingly high/low average
    from a tiny, unrepresentative sample."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   AVG(pps.potential_index) AS avg_potential, COUNT(*) AS scored_players
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            GROUP BY league_display
            HAVING COUNT(*) >= 20
            ORDER BY avg_potential DESC
        """)
        rows = cur.fetchall()
    conn.close()
    return [{"league": r["league_display"], "avg_potential": round(r["avg_potential"], 1), "scored_players": r["scored_players"]} for r in rows]


@app.get("/clubs/continuity")
def squad_continuity(limit: int = Query(15, le=50), authorized: bool = Depends(check_api_key)):
    """Which clubs have retained their core vs churned heavily, using the
    real transfer log. Honest caveat: this log was only recently cleaned
    of historical noise (see earlier in the build), so early results will
    be sparse — it gets more meaningful as more genuine transfers happen
    and get tracked going forward."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.name AS club, COUNT(*) FILTER (WHERE t.new_club_id = cl.id) AS arrivals,
                   COUNT(*) FILTER (WHERE t.old_club_id = cl.id) AS departures
            FROM clubs cl
            LEFT JOIN player_club_transfers t ON t.new_club_id = cl.id OR t.old_club_id = cl.id
            GROUP BY cl.name
            HAVING COUNT(*) > 0
            ORDER BY (COUNT(*) FILTER (WHERE t.new_club_id = cl.id) + COUNT(*) FILTER (WHERE t.old_club_id = cl.id)) DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/leagues/table-predictor")
def table_predictor(league: str, authorized: bool = Depends(check_api_key)):
    """An illustrative alternate table ranked purely by squad quality
    (average potential score), shown alongside each club's REAL current
    league position for honest comparison — deliberately NOT a real
    forecast. Squad quality alone doesn't determine outcomes: form,
    injuries, tactics, and management all matter enormously and none of
    that is in this data. This is here to spot over/under-performers
    relative to squad quality, not to predict results."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        cur.execute("""
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential, COUNT(*) AS squad_size
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.league_id = %s
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
            ORDER BY avg_potential DESC
        """, (league_id,))
        squad_ranking = cur.fetchall()

        cur.execute("""
            WITH club_matches AS (
                SELECT home_club_id AS club_id,
                    CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE league_id = %s AND status = 'finished'
                UNION ALL
                SELECT away_club_id,
                    CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE league_id = %s AND status = 'finished'
            )
            SELECT c.name AS club, SUM(pts) AS points
            FROM club_matches cm JOIN clubs c ON c.id = cm.club_id
            GROUP BY c.name ORDER BY points DESC
        """, (league_id, league_id))
        real_table = {r["club"]: i + 1 for i, r in enumerate(cur.fetchall())}
    conn.close()

    result = []
    for i, r in enumerate(squad_ranking):
        real_pos = real_table.get(r["club"])
        result.append({
            "club": r["club"], "squad_quality_rank": i + 1,
            "avg_potential": round(r["avg_potential"], 1),
            "real_table_position": real_pos,
            "delta": (real_pos - (i + 1)) if real_pos else None,  # positive = overperforming their squad quality
        })
    return result


@app.get("/transfers")
def recent_transfers(limit: int = Query(20, le=100), authorized: bool = Depends(check_api_key)):
    """Recent club changes, detected automatically by a database trigger
    whenever ingestion updates a player's current_club_id."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                p.id AS player_id, p.full_name, p.primary_position,
                old_cl.name AS old_club, new_cl.name AS new_club,
                l.name AS league, t.changed_at
            FROM player_club_transfers t
            JOIN players p ON p.id = t.player_id
            LEFT JOIN clubs old_cl ON old_cl.id = t.old_club_id
            LEFT JOIN clubs new_cl ON new_cl.id = t.new_club_id
            LEFT JOIN leagues l ON l.id = new_cl.league_id
            ORDER BY t.changed_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/fixtures")
def fixtures(
    league: Optional[str] = None,
    club: Optional[str] = None,
    upcoming: bool = True,
    limit: int = Query(50, le=200),
    authorized: bool = Depends(check_api_key),
):
    """Upcoming (status='scheduled') or recent results (status='finished'),
    filterable by league and/or club. 'Competition' in this context means
    the league itself — we only ingest fixtures for tracked leagues, not
    separate cup/continental competitions."""
    conn = get_conn()
    filters = ["m.status = %s"]
    params = [("scheduled" if upcoming else "finished")]

    if league:
        filters.append("(l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s")
        params.append(league)
    if club:
        filters.append("(home_cl.name = %s OR away_cl.name = %s)")
        params.append(club)
        params.append(club)

    order = "m.match_date ASC" if upcoming else "m.match_date DESC"
    query = f"""
        SELECT
            m.id, m.match_date, m.status, m.home_score, m.away_score,
            l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
            home_cl.name AS home_club, away_cl.name AS away_club
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        LEFT JOIN countries co ON co.id = l.country_id
        LEFT JOIN clubs home_cl ON home_cl.id = m.home_club_id
        LEFT JOIN clubs away_cl ON away_cl.id = m.away_club_id
        WHERE {" AND ".join(filters)}
        ORDER BY {order}
        LIMIT %s
    """
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/fixtures/{match_id}/boxscore")
def match_boxscore(match_id: int, authorized: bool = Depends(check_api_key)):
    """Every player's stats for a specific match, split by team — entirely
    free, since this is just querying data already ingested via the normal
    match-stats pipeline. No new API-Football calls."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.id, m.match_date, m.home_score, m.away_score,
                   home_cl.id AS home_club_id, home_cl.name AS home_club,
                   away_cl.id AS away_club_id, away_cl.name AS away_club
            FROM matches m
            LEFT JOIN clubs home_cl ON home_cl.id = m.home_club_id
            LEFT JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.id = %s
        """, (match_id,))
        match = cur.fetchone()
        if not match:
            conn.close()
            raise HTTPException(status_code=404, detail="Match not found")

        cur.execute("""
            SELECT p.full_name, p.photo_url, pms.club_id, pms.minutes_played,
                   pms.goals, pms.assists, pms.yellow_cards, pms.red_cards, pms.rating
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            WHERE pms.match_id = %s
            ORDER BY pms.goals DESC, pms.rating DESC NULLS LAST
        """, (match_id,))
        players = cur.fetchall()
    conn.close()
    return {
        "match": match,
        "home_players": [p for p in players if p["club_id"] == match["home_club_id"]],
        "away_players": [p for p in players if p["club_id"] == match["away_club_id"]],
    }


@app.get("/fixtures/{match_id}/events")
def match_events(match_id: int, authorized: bool = Depends(check_api_key)):
    """Full minute-by-minute event timeline (goals, cards, subs). On-demand
    and permanently cached — a finished match's history never changes, so
    the 1 API-Football request this costs is paid at most ONCE per match,
    ever, no matter how many times it's viewed afterward."""
    if not FOOTBALL_API_KEY:
        raise HTTPException(status_code=503, detail="FOOTBALL_API_KEY not configured on the server.")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT events FROM match_events_cache WHERE match_id = %s", (match_id,))
        cached = cur.fetchone()
        if cached:
            conn.close()
            return {"events": cached["events"], "cached": True}

        cur.execute("SELECT external_id FROM matches WHERE id = %s", (match_id,))
        match = cur.fetchone()
        if not match:
            conn.close()
            raise HTTPException(status_code=404, detail="Match not found")

    try:
        resp = requests.get(
            "https://v3.football.api-sports.io/fixtures/events",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"fixture": match["external_id"]},
            timeout=10,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        events = resp.json().get("response", [])
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=502, detail=f"Failed to fetch match events: {e}")

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO match_events_cache (match_id, events) VALUES (%s, %s) "
            "ON CONFLICT (match_id) DO UPDATE SET events = EXCLUDED.events",
            (match_id, json.dumps(events)),
        )
    conn.commit()
    conn.close()
    return {"events": events, "cached": False}


@app.get("/players/{player_id}/biography")
def player_biography(player_id: int, authorized: bool = Depends(check_api_key)):
    """A player's real Wikipedia biography — career narrative, honors,
    background — genuinely new context beyond raw stats. Free, no API key
    needed. On-demand and cached permanently, same pattern as match events.
    Our stored names are often abbreviated (e.g. 'N. Woltemade'), so this
    uses Wikipedia's own search first to find the right page, then fetches
    its summary — more robust than guessing an exact title."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM player_biography_cache WHERE player_id = %s", (player_id,))
        cached = cur.fetchone()
        if cached:
            conn.close()
            return dict(cached)

        cur.execute("SELECT full_name FROM players WHERE id = %s", (player_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Player not found")
        full_name = row["full_name"]

    headers = {"User-Agent": "CrossLeagueScoutingIndex/1.0 (personal scouting tool)"}
    try:
        search_resp = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={"action": "query", "list": "search", "srsearch": f"{full_name} footballer",
                    "format": "json", "srlimit": 1},
            headers=headers, timeout=8,
        )
        search_results = search_resp.json().get("query", {}).get("search", [])
    except Exception:
        search_results = []

    result = {"player_id": player_id, "found": False, "wikipedia_title": None,
              "summary": None, "thumbnail_url": None, "wikipedia_url": None}

    if search_results:
        title = search_results[0]["title"]
        try:
            summary_resp = requests.get(
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{title.replace(' ', '_')}",
                headers=headers, timeout=8,
            )
            if summary_resp.status_code == 200:
                data = summary_resp.json()
                result = {
                    "player_id": player_id, "found": True, "wikipedia_title": title,
                    "summary": data.get("extract"),
                    "thumbnail_url": data.get("thumbnail", {}).get("source"),
                    "wikipedia_url": data.get("content_urls", {}).get("desktop", {}).get("page"),
                }
        except Exception:
            pass

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO player_biography_cache
                (player_id, wikipedia_title, summary, thumbnail_url, wikipedia_url, found)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                wikipedia_title = EXCLUDED.wikipedia_title, summary = EXCLUDED.summary,
                thumbnail_url = EXCLUDED.thumbnail_url, wikipedia_url = EXCLUDED.wikipedia_url,
                found = EXCLUDED.found, cached_at = now()
        """, (player_id, result["wikipedia_title"], result["summary"],
              result["thumbnail_url"], result["wikipedia_url"], result["found"]))
    conn.commit()
    conn.close()
    return result


@app.get("/players/{player_id}/news")
def player_news(player_id: int, limit: int = Query(5, le=15), authorized: bool = Depends(check_api_key)):
    """Real, current news headlines for a player via Google News RSS —
    free, no API key needed, no 'developer use only' restriction (unlike
    NewsAPI.org's free tier, which explicitly forbids production use).
    Deliberately NOT cached — news is inherently time-sensitive, so this
    fetches genuinely fresh results every time it's requested."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT full_name FROM players WHERE id = %s", (player_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Player not found")

    import xml.etree.ElementTree as ET
    query = requests.utils.quote(f"{row['full_name']} football")
    try:
        resp = requests.get(
            f"https://news.google.com/rss/search?q={query}&hl=en-GB&gl=GB&ceid=GB:en",
            headers={"User-Agent": "CrossLeagueScoutingIndex/1.0"}, timeout=8,
        )
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item")[:limit]:
            items.append({
                "title": item.findtext("title"),
                "link": item.findtext("link"),
                "published": item.findtext("pubDate"),
                "source": item.findtext("source"),
            })
        return items
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch news: {e}")


@app.get("/players/{player_id}/highlights")
def player_highlights(player_id: int, authorized: bool = Depends(check_api_key)):
    """A real embedded highlight video, not just a search link — uses
    YouTube's Data API (free tier, ~100 searches/day, so this is cached
    permanently once found rather than re-searched on every view). Needs
    YOUTUBE_API_KEY configured on the server — returns a clear message if
    it isn't, rather than failing silently."""
    if not YOUTUBE_API_KEY:
        return {"found": False, "message": "YOUTUBE_API_KEY not configured on the server yet."}

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM player_highlights_cache WHERE player_id = %s", (player_id,))
        cached = cur.fetchone()
        if cached:
            conn.close()
            return dict(cached)

        cur.execute("SELECT full_name FROM players WHERE id = %s", (player_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Player not found")
        full_name = row["full_name"]

    result = {"player_id": player_id, "found": False, "video_id": None, "title": None, "thumbnail_url": None}
    try:
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet", "q": f"{full_name} highlights", "type": "video",
                "maxResults": 1, "order": "relevance", "key": YOUTUBE_API_KEY,
            },
            timeout=8,
        )
        items = resp.json().get("items", [])
        if items:
            snippet = items[0]["snippet"]
            result = {
                "player_id": player_id, "found": True,
                "video_id": items[0]["id"]["videoId"],
                "title": snippet["title"],
                "thumbnail_url": snippet["thumbnails"]["medium"]["url"],
            }
    except Exception:
        pass

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO player_highlights_cache (player_id, video_id, title, thumbnail_url, found)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                video_id = EXCLUDED.video_id, title = EXCLUDED.title,
                thumbnail_url = EXCLUDED.thumbnail_url, found = EXCLUDED.found, cached_at = now()
        """, (player_id, result["video_id"], result["title"], result["thumbnail_url"], result["found"]))
    conn.commit()
    conn.close()
    return result


@app.get("/standings")
def standings(league: str, authorized: bool = Depends(check_api_key)):
    """Full league table (P/W/D/L/GF/GA/GD/Pts), computed entirely from
    match results already ingested — no new API-Football calls."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id FROM leagues l
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (league,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="League not found")
        league_id = row["id"]

        cur.execute("""
            WITH club_matches AS (
                SELECT home_club_id AS club_id, home_score AS gf, away_score AS ga,
                    CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS pts,
                    CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
                    CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS draw,
                    CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss
                FROM matches WHERE league_id = %s AND status = 'finished'
                UNION ALL
                SELECT away_club_id, away_score, home_score,
                    CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END,
                    CASE WHEN away_score > home_score THEN 1 ELSE 0 END,
                    CASE WHEN away_score = home_score THEN 1 ELSE 0 END,
                    CASE WHEN away_score < home_score THEN 1 ELSE 0 END
                FROM matches WHERE league_id = %s AND status = 'finished'
            )
            SELECT c.name AS club, COUNT(*) AS played, SUM(win) AS won, SUM(draw) AS drawn, SUM(loss) AS lost,
                   SUM(gf) AS gf, SUM(ga) AS ga, SUM(gf) - SUM(ga) AS gd, SUM(pts) AS points
            FROM club_matches cm
            JOIN clubs c ON c.id = cm.club_id
            GROUP BY c.name
            ORDER BY points DESC, gd DESC, gf DESC
        """, (league_id, league_id))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/h2h")
def head_to_head(club1: str, club2: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Last N meetings between two specific clubs, regardless of which
    league/season each match belongs to — free, existing match data."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.match_date, m.home_score, m.away_score,
                   home_cl.name AS home_club, away_cl.name AS away_club
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.status = 'finished'
              AND ((home_cl.name = %s AND away_cl.name = %s) OR (home_cl.name = %s AND away_cl.name = %s))
            ORDER BY m.match_date DESC
            LIMIT %s
        """, (club1, club2, club2, club1, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/form")
def club_form(club: str, league: str, limit: int = Query(5, le=10), authorized: bool = Depends(check_api_key)):
    """Last N results for a club, as W/D/L from that club's perspective —
    free, existing match data."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.match_date, m.home_score, m.away_score,
                   home_cl.name AS home_club, away_cl.name AS away_club
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            LEFT JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE m.status = 'finished'
              AND (home_cl.name = %s OR away_cl.name = %s)
              AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            ORDER BY m.match_date DESC
            LIMIT %s
        """, (club, club, league, limit))
        rows = cur.fetchall()

    form = []
    for r in rows:
        is_home = r["home_club"] == club
        gf = r["home_score"] if is_home else r["away_score"]
        ga = r["away_score"] if is_home else r["home_score"]
        form.append("W" if gf > ga else "L" if gf < ga else "D")
    conn.close()
    return {"form": form}


@app.get("/leagues")
def list_leagues(authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id, l.name,
                   l.name || ' (' || COALESCE(c.name, 'Unknown') || ')' AS league_display,
                   l.season, l.is_top5, c.name AS country
            FROM leagues l
            LEFT JOIN countries c ON c.id = l.country_id
            ORDER BY l.name
        """)
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players")
def list_players(
    league: Optional[str] = None,
    position: Optional[str] = None,
    max_age: Optional[int] = None,
    min_potential: Optional[float] = Query(None),
    search: Optional[str] = None,
    shortlist_only: bool = False,
    sort: str = Query("potential", enum=[
        "potential", "age", "name", "goals", "assists", "tackles",
        "interceptions", "saves", "yellow_cards", "minutes_played", "duel_win_pct", "pass_accuracy_pct",
    ]),
    exclude_top5: bool = False,
    season: Optional[str] = None,
    limit: int = Query(50, le=5000),
    authorized: bool = Depends(check_api_key),
):
    conn = get_conn()
    filters = []
    params = []

    base_query = """
        SELECT
            p.id, p.full_name, p.date_of_birth, p.primary_position, p.photo_url,
            cl.name AS club,
            l.name AS league,
            l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
            l.season,
            pps.potential_index, pps.stat_component, pps.age_adjustment,
            pps.qualitative_component,
            stats.appearances, stats.minutes_played, stats.goals, stats.assists,
            stats.shots, stats.shots_on_target, stats.key_passes, stats.tackles,
            stats.interceptions, stats.take_ons_attempted, stats.take_ons_completed,
            stats.passes_completed, stats.passes_attempted,
            CASE WHEN stats.passes_attempted > 0
                 THEN ROUND(100.0 * stats.passes_completed / stats.passes_attempted, 1)
                 ELSE NULL END AS pass_accuracy_pct,
            stats.avg_rating,
            stats.duels_won, stats.duels_attempted,
            CASE WHEN stats.duels_attempted > 0
                 THEN ROUND(100.0 * stats.duels_won / stats.duels_attempted, 1)
                 ELSE NULL END AS duel_win_pct,
            stats.saves, stats.goals_conceded,
            CASE WHEN (stats.saves + stats.goals_conceded) > 0
                 THEN ROUND(100.0 * stats.saves / (stats.saves + stats.goals_conceded), 1)
                 ELSE NULL END AS save_pct,
            stats.fouls_committed, stats.fouls_drawn,
            stats.yellow_cards, stats.red_cards,
            stats.penalties_won, stats.penalties_committed,
            stats.penalties_scored, stats.penalties_missed,
            stats.offsides,
            latest_note.watch_level,
            latest_injury.injury_type, latest_injury.reason, latest_injury.reported_date
        FROM players p
        LEFT JOIN clubs cl ON cl.id = p.current_club_id
        LEFT JOIN leagues l ON l.id = cl.league_id
        LEFT JOIN countries co ON co.id = l.country_id
        LEFT JOIN LATERAL (
            SELECT * FROM player_potential_scores
            WHERE player_id = p.id
            ORDER BY season DESC LIMIT 1
        ) pps ON true
        LEFT JOIN (
            SELECT
                player_id,
                COUNT(*) AS appearances,
                SUM(minutes_played) AS minutes_played,
                SUM(goals) AS goals,
                SUM(assists) AS assists,
                SUM(shots) AS shots,
                SUM(shots_on_target) AS shots_on_target,
                SUM(key_passes) AS key_passes,
                SUM(tackles) AS tackles,
                SUM(interceptions) AS interceptions,
                SUM(take_ons_attempted) AS take_ons_attempted,
                SUM(take_ons_completed) AS take_ons_completed,
                SUM(passes_completed) AS passes_completed,
                SUM(passes_attempted) AS passes_attempted,
                SUM(duels_won) AS duels_won,
                SUM(duels_attempted) AS duels_attempted,
                SUM(saves) AS saves,
                SUM(goals_conceded) AS goals_conceded,
                SUM(fouls_committed) AS fouls_committed,
                SUM(fouls_drawn) AS fouls_drawn,
                SUM(yellow_cards) AS yellow_cards,
                SUM(red_cards) AS red_cards,
                SUM(penalties_won) AS penalties_won,
                SUM(penalties_committed) AS penalties_committed,
                SUM(penalties_scored) AS penalties_scored,
                SUM(penalties_missed) AS penalties_missed,
                SUM(offsides) AS offsides,
                ROUND(AVG(rating), 1) AS avg_rating
            FROM player_match_stats
            GROUP BY player_id
        ) stats ON stats.player_id = p.id
        LEFT JOIN LATERAL (
            SELECT watch_level FROM scout_notes sn
            WHERE sn.player_id = p.id
            ORDER BY created_at DESC LIMIT 1
        ) latest_note ON true
        LEFT JOIN LATERAL (
            -- API-Football's /injuries endpoint returns the WHOLE season's
            -- injury history, not a current snapshot — a 30-day window is a
            -- rough but far more honest proxy for "likely still relevant"
            -- than showing something from months ago as if it's current.
            SELECT injury_type, reason, reported_date FROM player_injuries pi
            WHERE pi.player_id = p.id AND pi.reported_date >= (CURRENT_DATE - INTERVAL '30 days')
            ORDER BY reported_date DESC NULLS LAST, ingested_at DESC LIMIT 1
        ) latest_injury ON true
    """

    if season:
        filters.append("pps.season = %s")
        params.append(season)
    if league:
        # Filter by the disambiguated display value (e.g. "Serie A (Brazil)"),
        # not plain name — multiple countries can share a league name.
        filters.append("(l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s")
        params.append(league)
    if position:
        filters.append("p.primary_position = %s")
        params.append(position)
    if max_age:
        filters.append("date_part('year', age(p.date_of_birth)) <= %s")
        params.append(max_age)
    if min_potential:
        filters.append("pps.potential_index >= %s")
        params.append(min_potential)
    if search:
        filters.append("(p.full_name ILIKE %s OR cl.name ILIKE %s)")
        params.append(f"%{search}%")
        params.append(f"%{search}%")
    if shortlist_only:
        filters.append("latest_note.watch_level = 'shortlist'")
    if exclude_top5:
        filters.append("l.is_top5 = false")

    if filters:
        base_query += " WHERE " + " AND ".join(filters)

    sort_map = {
        "potential": "pps.potential_index DESC NULLS LAST",
        "age": "p.date_of_birth DESC",
        "name": "p.full_name ASC",
        "goals": "stats.goals DESC NULLS LAST",
        "assists": "stats.assists DESC NULLS LAST",
        "tackles": "stats.tackles DESC NULLS LAST",
        "interceptions": "stats.interceptions DESC NULLS LAST",
        "saves": "stats.saves DESC NULLS LAST",
        "yellow_cards": "stats.yellow_cards DESC NULLS LAST",
        "minutes_played": "stats.minutes_played DESC NULLS LAST",
        "duel_win_pct": "duel_win_pct DESC NULLS LAST",
        "pass_accuracy_pct": "pass_accuracy_pct DESC NULLS LAST",
    }
    base_query += f" ORDER BY {sort_map[sort]} LIMIT %s"
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(base_query, params)
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/{player_id}")
def player_dossier(player_id: int, authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, cl.name AS club, l.name AS league
            FROM players p
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            WHERE p.id = %s
        """, (player_id,))
        player = cur.fetchone()
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        cur.execute("""
            SELECT * FROM player_potential_scores
            WHERE player_id = %s ORDER BY season DESC LIMIT 1
        """, (player_id,))
        score = cur.fetchone()

        cur.execute("""
            SELECT author, note, tags, watch_level, created_at
            FROM scout_notes WHERE player_id = %s
            ORDER BY created_at DESC
        """, (player_id,))
        notes = cur.fetchall()

        cur.execute("""
            SELECT m.match_date, cl.name AS opponent,
                   CASE WHEN m.home_club_id = pms.club_id THEN true ELSE false END AS is_home,
                   m.home_score, m.away_score, pms.minutes_played,
                   pms.goals, pms.assists, pms.rating, pms.yellow_cards, pms.red_cards
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            LEFT JOIN clubs cl ON cl.id = CASE
                WHEN m.home_club_id = pms.club_id THEN m.away_club_id
                ELSE m.home_club_id END
            WHERE pms.player_id = %s
            ORDER BY m.match_date DESC LIMIT 20
        """, (player_id,))
        recent_matches = cur.fetchall()

        cur.execute("""
            SELECT potential_index, computed_at
            FROM player_potential_history
            WHERE player_id = %s
            ORDER BY computed_at ASC
        """, (player_id,))
        history = cur.fetchall()

        cur.execute("""
            SELECT id, technical, physical, mental, tactical, notes, created_at
            FROM player_scout_ratings
            WHERE player_id = %s
            ORDER BY created_at DESC
        """, (player_id,))
        scout_ratings = cur.fetchall()

        # Positional versatility — real per-match position data, already
        # captured during ingestion but never surfaced until now.
        cur.execute("""
            SELECT position_played, COUNT(*) AS matches
            FROM player_match_stats
            WHERE player_id = %s AND position_played IS NOT NULL AND minutes_played > 0
            GROUP BY position_played
            ORDER BY matches DESC
        """, (player_id,))
        positions_played = cur.fetchall()

        # Consistency — rating variance across real matches. Needs a
        # minimum sample (5+) to mean anything; STDDEV_SAMP returns NULL
        # for n<2 in Postgres, and a single-match sample is meaningless
        # regardless, so we gate in Python below.
        cur.execute("""
            SELECT AVG(rating) AS avg_rating, STDDEV_SAMP(rating) AS rating_stddev, COUNT(*) AS matches
            FROM player_match_stats
            WHERE player_id = %s AND rating IS NOT NULL AND rating > 0
        """, (player_id,))
        consistency_row = cur.fetchone()
        consistency = None
        if consistency_row and consistency_row["matches"] >= 5:
            consistency = {
                "avg_rating": round(consistency_row["avg_rating"], 2),
                "stddev": round(consistency_row["rating_stddev"], 2) if consistency_row["rating_stddev"] is not None else 0,
                "matches": consistency_row["matches"],
            }

        # Scouting Confidence — an honesty layer on the potential score
        # itself. A rating built on 2,000 real minutes deserves more trust
        # than one built on 90 minutes of a single substitute cameo, even
        # if the number looks identical. Uses TRUE total minutes (no
        # rating filter) — a match without a recorded rating still counts
        # as real minutes played.
        cur.execute("""
            SELECT SUM(minutes_played) AS total_minutes
            FROM player_match_stats WHERE player_id = %s
        """, (player_id,))
        minutes_row = cur.fetchone()
        total_minutes = (minutes_row["total_minutes"] if minutes_row else None) or 0
        if total_minutes >= 1800:
            confidence_tier = "high"
        elif total_minutes >= 900:
            confidence_tier = "good"
        elif total_minutes >= 300:
            confidence_tier = "moderate"
        else:
            confidence_tier = "low"
        scouting_confidence = {"tier": confidence_tier, "minutes": total_minutes}

        # Tactical Archetype — classifies WHAT KIND of player this is
        # (Playmaker, Poacher, Ball-Playing Defender, etc.), not just their
        # broad position. Computed on-demand against real same-position
        # peers (450+ minutes each, so the peer group itself is meaningful)
        # using percentile rank on a handful of per-90 stats, then simple,
        # transparent threshold rules — not a black-box model, so the
        # reasoning stays explainable. Only computed if the player
        # themselves has 450+ minutes, so a tiny sample doesn't get a
        # confident-sounding label it hasn't earned.
        archetype = None
        position = player.get("primary_position")
        if position and total_minutes >= 450:
            cur.execute("""
                SELECT player_id,
                       SUM(goals) * 90.0 / NULLIF(SUM(minutes_played), 0) AS goals_p90,
                       SUM(assists) * 90.0 / NULLIF(SUM(minutes_played), 0) AS assists_p90,
                       SUM(key_passes) * 90.0 / NULLIF(SUM(minutes_played), 0) AS key_passes_p90,
                       SUM(tackles + interceptions) * 90.0 / NULLIF(SUM(minutes_played), 0) AS defensive_p90,
                       SUM(take_ons_attempted) * 90.0 / NULLIF(SUM(minutes_played), 0) AS take_ons_p90,
                       AVG(NULLIF(passes_completed, 0)::float / NULLIF(passes_attempted, 0)) * 100 AS pass_acc
                FROM player_match_stats pms
                JOIN players p3 ON p3.id = pms.player_id
                WHERE p3.primary_position = %s
                GROUP BY player_id
                HAVING SUM(minutes_played) >= 450
            """, (position,))
            peer_rows = cur.fetchall()

            target_row = next((r for r in peer_rows if r["player_id"] == player_id), None)
            if target_row and len(peer_rows) >= 10:  # need a real peer group to rank against
                pr = {
                    "goals": percentile_rank(target_row["goals_p90"], [r["goals_p90"] for r in peer_rows]),
                    "assists": percentile_rank(target_row["assists_p90"], [r["assists_p90"] for r in peer_rows]),
                    "key_passes": percentile_rank(target_row["key_passes_p90"], [r["key_passes_p90"] for r in peer_rows]),
                    "defensive": percentile_rank(target_row["defensive_p90"], [r["defensive_p90"] for r in peer_rows]),
                    "take_ons": percentile_rank(target_row["take_ons_p90"], [r["take_ons_p90"] for r in peer_rows]),
                    "pass_acc": percentile_rank(target_row["pass_acc"], [r["pass_acc"] for r in peer_rows]),
                }
                archetype = classify_archetype(position, pr)

        # League-Adjusted Rating — a second, cross-league-normalized score.
        # Deliberately conservative: only ever DEFLATES a score for a
        # weaker league, never inflates one for a stronger league. Reason:
        # our potential_index already ranks players by GLOBAL percentile
        # (not within-league), so a strong-league player's high percentile
        # is already fairly earned against tough competition — but a
        # weak-league player's raw stats (goals, etc.) may be inflated
        # simply by facing weaker opposition, which the global percentile
        # doesn't fully correct for. This surfaces that gap explicitly
        # rather than pretending a 75 means the same thing everywhere.
        league_adjusted = None
        if score and player.get("league") and player.get("club"):
            cur.execute("""
                SELECT l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                       AVG(pps.potential_index) AS avg_potential, COUNT(*) AS n
                FROM players p2
                JOIN clubs cl2 ON cl2.id = p2.current_club_id
                JOIN leagues l ON l.id = cl2.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                JOIN LATERAL (
                    SELECT potential_index FROM player_potential_scores
                    WHERE player_id = p2.id ORDER BY season DESC LIMIT 1
                ) pps ON true
                GROUP BY league_display
                HAVING COUNT(*) >= 20
            """)
            league_averages = {r["league_display"]: r["avg_potential"] for r in cur.fetchall()}
            this_league_avg = None
            for name, avg in league_averages.items():
                if player["league"] in name:
                    this_league_avg = avg
                    break
            if this_league_avg and league_averages:
                global_avg = sum(league_averages.values()) / len(league_averages)
                factor = min(1.0, this_league_avg / global_avg) if global_avg > 0 else 1.0
                league_adjusted = round(score["potential_index"] * factor, 1)

        # Composite Scouting Grade — one letter grade synthesizing the raw
        # rating with how much we actually trust it. A high score built on
        # a tiny sample gets explicitly CAPPED, not just footnoted — the
        # grade itself encodes "don't fully trust this yet," rather than
        # showing an A+ next to a quiet asterisk nobody reads.
        grade = None
        if score and score.get("potential_index") is not None:
            base = league_adjusted if league_adjusted is not None else score["potential_index"]
            if base >= 90: raw_grade = "A+"
            elif base >= 80: raw_grade = "A"
            elif base >= 70: raw_grade = "B+"
            elif base >= 60: raw_grade = "B"
            elif base >= 50: raw_grade = "C+"
            elif base >= 40: raw_grade = "C"
            else: raw_grade = "D"

            grade_order = ["D", "C", "C+", "B", "B+", "A", "A+"]
            cap = {"low": "B", "moderate": "A", "good": "A+", "high": "A+"}[confidence_tier]
            if grade_order.index(raw_grade) > grade_order.index(cap):
                grade = cap
            else:
                grade = raw_grade

        cur.execute("""
            SELECT team_name, competition_name, appearances, goals, assists, minutes_played
            FROM player_international_caps
            WHERE player_id = %s
            ORDER BY appearances DESC NULLS LAST
        """, (player_id,))
        international_caps = cur.fetchall()
        for cap in international_caps:
            cap["flag_url"] = get_flag_url(cur, cap["team_name"])
        conn.commit()  # persist any newly-cached flags from get_flag_url

    conn.close()
    return {
        "player": player,
        "score": score,
        "scout_notes": notes,
        "recent_matches": recent_matches,
        "history": history,
        "scout_ratings": scout_ratings,
        "positions_played": positions_played,
        "consistency": consistency,
        "scouting_confidence": scouting_confidence,
        "league_adjusted_rating": league_adjusted,
        "archetype": archetype,
        "grade": grade,
        "international_caps": international_caps,
    }


class WatchRequest(BaseModel):
    watch_level: Optional[str] = None  # 'monitor' | 'shortlist' | 'priority' | None to clear
    note: Optional[str] = None
    author: Optional[str] = "dashboard"


@app.post("/players/{player_id}/watch")
def set_watch_level(player_id: int, body: WatchRequest, authorized: bool = Depends(check_api_key)):
    if body.watch_level is not None and body.watch_level not in ("monitor", "shortlist", "priority"):
        raise HTTPException(status_code=400, detail="watch_level must be monitor, shortlist, priority, or null")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM players WHERE id = %s", (player_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Player not found")

        note_text = body.note or (
            f"Marked as {body.watch_level} via dashboard" if body.watch_level
            else "Removed from shortlist via dashboard"
        )
        cur.execute(
            """
            INSERT INTO scout_notes (player_id, author, note, watch_level)
            VALUES (%s, %s, %s, %s)
            RETURNING id, watch_level, created_at
            """,
            (player_id, body.author, note_text, body.watch_level),
        )
        result = cur.fetchone()
    conn.commit()
    conn.close()
    return result


class ScoutRatingRequest(BaseModel):
    technical: int
    physical: int
    mental: int
    tactical: int
    notes: Optional[str] = None


@app.post("/players/{player_id}/scout-rating")
def save_scout_rating(player_id: int, body: ScoutRatingRequest, authorized: bool = Depends(check_api_key)):
    """Structured 1-10 evaluation across four real scouting dimensions —
    replaces the old blunt watch_level flag as the qualitative signal the
    scoring model actually uses, once one exists for a player."""
    for field, value in [("technical", body.technical), ("physical", body.physical),
                          ("mental", body.mental), ("tactical", body.tactical)]:
        if not (1 <= value <= 10):
            raise HTTPException(status_code=400, detail=f"{field} must be between 1 and 10")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM players WHERE id = %s", (player_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Player not found")

        cur.execute(
            """
            INSERT INTO player_scout_ratings (player_id, technical, physical, mental, tactical, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, technical, physical, mental, tactical, notes, created_at
            """,
            (player_id, body.technical, body.physical, body.mental, body.tactical, body.notes),
        )
        result = cur.fetchone()
    conn.commit()
    conn.close()
    return result



# ---------------------------------------------------------------------------
# "Ask the Index" — natural language Q&A over the database.
# Two-step: (1) an LLM call turns the question into a single read-only SQL
# query against the actual schema, which we validate and execute; (2) a
# second LLM call turns the raw results back into a plain-English answer.
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTION = """
Tables (Postgres):

leagues(id, name, season TEXT e.g. '2025', country_id, is_top5 BOOLEAN)
countries(id, name)
clubs(id, name, league_id, country_id)
players(id, full_name, date_of_birth, primary_position TEXT — one of
    'Attacker','Midfielder','Defender','Goalkeeper', current_club_id)
matches(id, league_id, home_club_id, away_club_id, match_date, home_score, away_score)
player_match_stats(id, player_id, match_id, club_id, minutes_played,
    goals, assists, shots, shots_on_target, key_passes,
    passes_completed, passes_attempted, take_ons_attempted, take_ons_completed,
    tackles, interceptions, duels_won, duels_attempted, rating,
    saves, goals_conceded, fouls_committed, fouls_drawn,
    yellow_cards, red_cards, penalties_won, penalties_committed,
    penalties_scored, penalties_missed, offsides)
    -- one row per player per match. This is the source of truth for all
    -- season totals and per-90 rates — always SUM/AVG across this table
    -- grouped by player_id, joined to matches->leagues for season/league
    -- filtering, rather than assuming any pre-aggregated column exists.
    -- NOTE: this table also has xg, xa, progressive_passes, progressive_carries
    -- columns that exist but are NEVER populated (always 0/null) — never
    -- use them to answer a question; if asked about xG, say it isn't tracked.
player_potential_scores(player_id, season, potential_index 0-100,
    stat_component, age_adjustment, qualitative_component)
scout_notes(player_id, author, note, watch_level — 'monitor'/'shortlist'/'priority', created_at)

Relationships: players.current_club_id -> clubs.id -> clubs.league_id -> leagues.id
clubs.country_id / leagues.country_id -> countries.id
matches.league_id -> leagues.id ; player_match_stats.match_id -> matches.id

Season in this database is '2025' (most recently completed full season for most
leagues) unless the user specifies otherwise. Per-90 rate = SUM(stat) * 90.0 /
SUM(minutes_played), only for players with a meaningful minutes sample (use
HAVING SUM(minutes_played) >= 450 for "who is best at X" style ranking
questions, to avoid tiny-sample noise, unless the user asks about a specific
named player where any sample is fine).
"""

SQL_FEWSHOT_EXAMPLES = """
Q: Who has scored the most goals this season?
SQL:
SELECT p.full_name, cl.name AS club, l.name AS league, SUM(pms.goals) AS goals
FROM player_match_stats pms
JOIN players p ON p.id = pms.player_id
JOIN matches m ON m.id = pms.match_id
JOIN leagues l ON l.id = m.league_id
LEFT JOIN clubs cl ON cl.id = pms.club_id
WHERE l.season = '2025'
GROUP BY p.full_name, cl.name, l.name
ORDER BY goals DESC
LIMIT 10;

Q: Best young defenders outside the top 5 leagues by tackles per 90
SQL:
SELECT p.full_name, cl.name AS club, l.name AS league,
       DATE_PART('year', AGE(p.date_of_birth)) AS age,
       SUM(pms.tackles) * 90.0 / SUM(pms.minutes_played) AS tackles_p90,
       SUM(pms.minutes_played) AS minutes
FROM player_match_stats pms
JOIN players p ON p.id = pms.player_id
JOIN matches m ON m.id = pms.match_id
JOIN leagues l ON l.id = m.league_id
LEFT JOIN clubs cl ON cl.id = pms.club_id
WHERE l.season = '2025' AND p.primary_position = 'Defender'
      AND l.is_top5 = false
      AND DATE_PART('year', AGE(p.date_of_birth)) <= 21
GROUP BY p.full_name, cl.name, l.name, p.date_of_birth
HAVING SUM(pms.minutes_played) >= 450
ORDER BY tackles_p90 DESC
LIMIT 10;

Q: Show me my shortlisted players
SQL:
SELECT p.full_name, cl.name AS club, l.name AS league, sn.watch_level
FROM scout_notes sn
JOIN players p ON p.id = sn.player_id
LEFT JOIN clubs cl ON cl.id = p.current_club_id
LEFT JOIN leagues l ON l.id = cl.league_id
WHERE sn.watch_level = 'shortlist'
ORDER BY p.full_name;
"""

FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|grant|revoke|create|copy|execute|call|vacuum|reindex)\b",
    re.IGNORECASE,
)


def validate_readonly_sql(sql: str) -> str:
    """Raises ValueError if the SQL isn't a safe, single, read-only statement."""
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        raise ValueError("Empty query generated.")
    if ";" in cleaned:
        raise ValueError("Multiple statements are not allowed.")
    if not re.match(r"^\s*(select|with)\b", cleaned, re.IGNORECASE):
        raise ValueError("Only SELECT/WITH queries are allowed.")
    if FORBIDDEN_SQL_KEYWORDS.search(cleaned):
        raise ValueError("Query contains a disallowed keyword.")
    return cleaned


def extract_sql(text: str) -> str:
    """Pull SQL out of a ```sql fenced block if present, else use as-is."""
    match = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else text.strip()


class AskRequest(BaseModel):
    question: str


@app.post("/ask")
def ask_the_index(body: AskRequest, authorized: bool = Depends(check_api_key)):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured on the server.")
    if not body.question or not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Step 1: question -> SQL
    sql_response = client.messages.create(
        model="claude-sonnet-5",
        max_tokens=800,
        system=(
            "You write a single read-only PostgreSQL query to answer football "
            "scouting questions against the schema below. Respond with ONLY the "
            "SQL query in a ```sql code block — no prose, no explanation.\n\n"
            + SCHEMA_DESCRIPTION + "\n\nExamples:\n" + SQL_FEWSHOT_EXAMPLES
        ),
        messages=[{"role": "user", "content": body.question}],
    )
    raw_sql = "".join(b.text for b in sql_response.content if hasattr(b, "text"))
    sql = extract_sql(raw_sql)

    try:
        sql = validate_readonly_sql(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Could not safely answer that question: {e}")

    # Execute with a hard row cap and a read-only transaction as defense in depth.
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(sql)
            rows = cur.fetchmany(200)
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=400, detail=f"Query failed: {e}")
    conn.close()

    # Step 2: results -> plain-English answer
    import json
    answer_response = client.messages.create(
        model="claude-sonnet-5",
        max_tokens=600,
        system=(
            "You are a football scouting assistant. Given a user's question and "
            "real query results from a database (2025 season, 17 leagues including "
            "the top 5 plus talent-pipeline leagues), answer conversationally and "
            "factually, citing specific names and numbers from the results. If "
            "results are empty, say so plainly rather than guessing. Keep it concise."
        ),
        messages=[{
            "role": "user",
            "content": f"Question: {body.question}\n\nResults (JSON):\n{json.dumps(rows, default=str)}",
        }],
    )
    answer = "".join(b.text for b in answer_response.content if hasattr(b, "text"))

    return {"question": body.question, "answer": answer, "sql": sql, "rows": rows}