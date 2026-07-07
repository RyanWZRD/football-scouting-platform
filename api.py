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
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL")

# Simple shared-secret protection. Set this in Render's environment variables
# and pass the same value as a header from your dashboard: X-API-Key: <value>
# Leave API_ACCESS_KEY unset to disable the check (useful for local testing).
API_ACCESS_KEY = os.environ.get("API_ACCESS_KEY")

app = FastAPI(title="Cross-League Scouting API")

# Restrict to your actual deployed frontend domain(s) rather than "*".
# Add more origins to this list as you deploy the dashboard elsewhere.
ALLOWED_ORIGINS = [
    "https://scoutindex.netlify.app",
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


@app.get("/leagues")
def list_leagues(authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id, l.name, l.season, l.is_top5, c.name AS country
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
    sort: str = Query("potential", enum=["potential", "age", "name"]),
    season: Optional[str] = None,
    limit: int = Query(50, le=200),
    authorized: bool = Depends(check_api_key),
):
    conn = get_conn()
    filters = []
    params = []

    base_query = """
        SELECT
            p.id, p.full_name, p.date_of_birth, p.primary_position,
            cl.name AS club, l.name AS league, l.season,
            pps.potential_index, pps.stat_component, pps.age_adjustment,
            pps.qualitative_component,
            stats.minutes_played, stats.goals, stats.assists,
            stats.shots_on_target, stats.key_passes, stats.tackles,
            stats.interceptions, stats.take_ons_completed,
            CASE WHEN stats.passes_attempted > 0
                 THEN ROUND(100.0 * stats.passes_completed / stats.passes_attempted, 1)
                 ELSE NULL END AS pass_accuracy_pct,
            stats.avg_rating,
            CASE WHEN stats.duels_attempted > 0
                 THEN ROUND(100.0 * stats.duels_won / stats.duels_attempted, 1)
                 ELSE NULL END AS duel_win_pct,
            latest_note.watch_level
        FROM players p
        LEFT JOIN clubs cl ON cl.id = p.current_club_id
        LEFT JOIN leagues l ON l.id = cl.league_id
        LEFT JOIN player_potential_scores pps ON pps.player_id = p.id
        LEFT JOIN (
            SELECT
                player_id,
                SUM(minutes_played) AS minutes_played,
                SUM(goals) AS goals,
                SUM(assists) AS assists,
                SUM(shots_on_target) AS shots_on_target,
                SUM(key_passes) AS key_passes,
                SUM(tackles) AS tackles,
                SUM(interceptions) AS interceptions,
                SUM(take_ons_completed) AS take_ons_completed,
                SUM(passes_completed) AS passes_completed,
                SUM(passes_attempted) AS passes_attempted,
                SUM(duels_won) AS duels_won,
                SUM(duels_attempted) AS duels_attempted,
                ROUND(AVG(rating), 1) AS avg_rating
            FROM player_match_stats
            GROUP BY player_id
        ) stats ON stats.player_id = p.id
        LEFT JOIN LATERAL (
            SELECT watch_level FROM scout_notes sn
            WHERE sn.player_id = p.id
            ORDER BY created_at DESC LIMIT 1
        ) latest_note ON true
    """

    if season:
        filters.append("pps.season = %s")
        params.append(season)
    if league:
        filters.append("l.name = %s")
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

    if filters:
        base_query += " WHERE " + " AND ".join(filters)

    sort_map = {
        "potential": "pps.potential_index DESC NULLS LAST",
        "age": "p.date_of_birth DESC",
        "name": "p.full_name ASC",
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
            SELECT m.match_date, cl.name AS opponent, pms.minutes_played,
                   pms.goals, pms.assists, pms.xg, pms.xa, pms.rating
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            LEFT JOIN clubs cl ON cl.id = CASE
                WHEN m.home_club_id = pms.club_id THEN m.away_club_id
                ELSE m.home_club_id END
            WHERE pms.player_id = %s
            ORDER BY m.match_date DESC LIMIT 10
        """, (player_id,))
        recent_matches = cur.fetchall()

    conn.close()
    return {
        "player": player,
        "score": score,
        "scout_notes": notes,
        "recent_matches": recent_matches,
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