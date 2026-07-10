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
        "interceptions", "saves", "duel_win_pct", "pass_accuracy_pct",
    ]),
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

        cur.execute("""
            SELECT potential_index, computed_at
            FROM player_potential_history
            WHERE player_id = %s
            ORDER BY computed_at ASC
        """, (player_id,))
        history = cur.fetchall()

    conn.close()
    return {
        "player": player,
        "score": score,
        "scout_notes": notes,
        "recent_matches": recent_matches,
        "history": history,
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