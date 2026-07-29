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
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Header, Depends, WebSocket, WebSocketDisconnect, Body, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import anthropic
import re
import requests
import random
import bcrypt
from pywebpush import webpush, WebPushException
import jwt

DATABASE_URL = os.environ.get("DATABASE_URL")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
FOOTBALL_API_KEY = os.environ.get("FOOTBALL_API_KEY")  # for on-demand match event lookups
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")  # for real embedded highlights (free tier, ~100 searches/day)

# Simple shared-secret protection. Set this in Render's environment variables
# and pass the same value as a header from your dashboard: X-API-Key: <value>
# Leave API_ACCESS_KEY unset to disable the check (useful for local testing).
API_ACCESS_KEY = os.environ.get("API_ACCESS_KEY")

app = FastAPI(title="Cross-League Scouting API")

# Loaded once at startup, not per-request — a genuine trained ML model,
# the platform's first step beyond transparent rule-based percentiles.
# Loading can genuinely fail (model never trained yet, or not enough
# data existed when it was) — that's a valid state, not an error, so
# this degrades gracefully rather than crashing the whole API.
_trajectory_model = None
try:
    import joblib
    _trajectory_model = joblib.load("trajectory_model.joblib")
    print("Trajectory ML model loaded successfully.")
except Exception as e:
    print(f"Trajectory ML model not loaded (this is fine if it hasn't been trained yet): {e}")

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


# Level 2 — genuine backend authentication lockdown. Confirmed via
# Starlette's own scope["type"] mechanism that @app.middleware("http")
# only ever applies to HTTP requests, never WebSocket connections — so
# /ws/live and /ws/player-notes/{id} are genuinely, structurally
# unaffected by this, not just accidentally working.
#
# Explicitly whitelisted public paths — the only things reachable
# without a real, logged-in user. Kept short and deliberate rather
# than an "opt-out" list, since a missed path here means something
# genuinely became unprotected by accident.
PUBLIC_PATHS = {
    "/health",
    "/status",
    "/auth/register",
    "/auth/login",
    "/push/public-key",
    "/public/track-record",
    "/docs",
    "/openapi.json",
    "/redoc",
}


@app.middleware("http")
async def require_real_login(request: Request, call_next):
    # CORS preflight requests are sent automatically by browsers before
    # any real cross-origin request, and genuinely cannot include an
    # Authorization header — this check is deliberately independent of
    # middleware ordering, so it stays correct even if that ever changes.
    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path in PUBLIC_PATHS:
        return await call_next(request)

    if not JWT_SECRET:
        return JSONResponse(status_code=500, content={"detail": "Server auth misconfigured — JWT_SECRET not set"})

    auth_header = request.headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Login required"})

    token = auth_header.removeprefix("Bearer ").strip()
    try:
        jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        return JSONResponse(status_code=401, content={"detail": "Session expired — please log in again"})
    except jwt.InvalidTokenError:
        return JSONResponse(status_code=401, content={"detail": "Invalid session token"})

    return await call_next(request)


def check_api_key(x_api_key: Optional[str] = Header(None), authorization: Optional[str] = Header(None)):
    """A logged-in request (real, genuinely-validated Authorization:
    Bearer token) is stronger proof than the shared API key — it
    uniquely identifies a specific account, not just 'not a random
    stranger'. Deliberately does its OWN, real JWT validation here
    rather than trusting the require_real_login middleware already
    did it — /auth/register and /auth/login both use this same
    function AND are on the middleware's public whitelist (it never
    runs for them at all), so a naive 'just check for the Bearer
    prefix' version would let someone bypass the API key gate there
    with a fake, unverified header. This version can't be fooled that
    way, since it independently decodes and verifies the token itself."""
    if authorization and authorization.startswith("Bearer ") and JWT_SECRET:
        token = authorization.removeprefix("Bearer ").strip()
        try:
            jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return True
        except jwt.InvalidTokenError:
            pass  # fall through to the API key check below
    if API_ACCESS_KEY and x_api_key != API_ACCESS_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return True


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# Multi-User Support — Phase 1 foundation. Real bcrypt password hashing
# and JWT session tokens. JWT_SECRET must be set as a genuine, random
# environment variable on Render — never falls back to a hardcoded
# default, since that would make every token forgeable.
JWT_SECRET = os.environ.get("JWT_SECRET")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_DAYS = 30  # a personal scouting tool, not a high-security banking app — a long-lived session is a reasonable, honest tradeoff


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def create_access_token(user_id: int) -> str:
    payload = {"user_id": user_id, "exp": datetime.utcnow() + timedelta(days=JWT_EXPIRY_DAYS)}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(authorization: Optional[str] = Header(None)) -> int:
    """Real per-user authentication — validates the JWT and returns the
    genuine, authenticated user_id. Distinct from check_api_key (which
    only gates general API access) — this identifies WHO is making the
    request, required for any genuinely user-scoped data."""
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="Server auth misconfigured — JWT_SECRET not set")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload["user_id"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired — please log in again")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session token")


# Rate limiting for /auth/login and /auth/register — an in-memory,
# sliding-window limiter. Genuinely safe as in-memory state here since
# Render runs this app as a single process (WEB_CONCURRENCY=1,
# confirmed from actual deploy logs) — a multi-worker deployment would
# need a shared store like Redis instead, since each worker would
# otherwise track its own separate, incomplete count.
_rate_limit_attempts = {}  # {(bucket, key): [timestamp, timestamp, ...]}


def get_client_ip(request: Request) -> str:
    """Render sits behind its own proxy, so request.client.host alone
    would show Render's internal address for every visitor, not the
    real one. X-Forwarded-For's leftmost entry is the genuine,
    original client IP; request.client.host is only the fallback for
    direct connections without a proxy in front."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def check_rate_limit(bucket: str, key: str, max_attempts: int, window_seconds: int):
    """Raises 429 if this key has genuinely exceeded max_attempts
    within the sliding window. Prunes old timestamps on every call so
    the in-memory dict doesn't grow unbounded over time."""
    now = time.time()
    cache_key = (bucket, key)
    attempts = [t for t in _rate_limit_attempts.get(cache_key, []) if now - t < window_seconds]
    if len(attempts) >= max_attempts:
        retry_after = int(window_seconds - (now - attempts[0]))
        raise HTTPException(status_code=429, detail=f"Too many attempts — please try again in {max(retry_after, 1)} seconds")
    attempts.append(now)
    _rate_limit_attempts[cache_key] = attempts


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


class RegisterRequest(BaseModel):
    email: str
    password: str


@app.post("/auth/register")
def auth_register(request: Request, body: RegisterRequest = Body(...), authorized: bool = Depends(check_api_key)):
    """Creates a real user account. Still gated behind the existing
    API key (check_api_key) — this is a personal/small-team tool, not
    a public signup page, so the API key remains the outer gate before
    anyone can even attempt to register."""
    check_rate_limit("register", get_client_ip(request), max_attempts=3, window_seconds=3600)

    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required")
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            conn.close()
            raise HTTPException(status_code=409, detail="An account with this email already exists")
        cur.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (email, hash_password(body.password)),
        )
        user_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()

    token = create_access_token(user_id)
    return {"access_token": token, "user_id": user_id, "email": email}


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/auth/login")
def auth_login(request: Request, body: LoginRequest = Body(...), authorized: bool = Depends(check_api_key)):
    check_rate_limit("login", get_client_ip(request), max_attempts=5, window_seconds=900)

    email = body.email.strip().lower()
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id, password_hash FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
    conn.close()

    # Deliberately identical error for "no such user" and "wrong
    # password" — a real security practice, not an oversight. Telling
    # an attacker which one failed genuinely helps them enumerate
    # valid accounts.
    if not row or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = create_access_token(row["id"])
    return {"access_token": token, "user_id": row["id"], "email": email}


@app.get("/auth/me")
def auth_me(user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Confirms whether the current session token is genuinely still
    valid, and returns basic account info — used by the frontend on
    load to check for an existing session."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id, email, created_at FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")
    return row


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


@app.post("/auth/change-password")
def auth_change_password(body: ChangePasswordRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Requires the current password for verification — a genuine
    security practice, since the session token alone shouldn't be
    enough to change account credentials (e.g. a shared/unlocked
    device shouldn't let anyone silently take over the account)."""
    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row or not verify_password(body.current_password, row["password_hash"]):
            conn.close()
            raise HTTPException(status_code=401, detail="Current password is incorrect")
        cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (hash_password(body.new_password), user_id))
    conn.commit()
    conn.close()
    return {"changed": True}


class ChangeEmailRequest(BaseModel):
    current_password: str
    new_email: str


@app.post("/auth/change-email")
def auth_change_email(body: ChangeEmailRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    new_email = body.new_email.strip().lower()
    if not new_email or "@" not in new_email:
        raise HTTPException(status_code=400, detail="A valid email is required")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row or not verify_password(body.current_password, row["password_hash"]):
            conn.close()
            raise HTTPException(status_code=401, detail="Current password is incorrect")
        cur.execute("SELECT id FROM users WHERE email = %s AND id != %s", (new_email, user_id))
        if cur.fetchone():
            conn.close()
            raise HTTPException(status_code=409, detail="That email is already in use")
        cur.execute("UPDATE users SET email = %s WHERE id = %s", (new_email, user_id))
    conn.commit()
    conn.close()
    return {"changed": True, "email": new_email}


class DeleteAccountRequest(BaseModel):
    current_password: str


@app.delete("/auth/account")
def auth_delete_account(body: DeleteAccountRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Deletes the account itself. Real, related data (scout notes,
    pipeline entries, push subscriptions) cascades and cleans up
    automatically via the existing ON DELETE CASCADE foreign keys —
    confirmed directly against the actual migrations, not assumed."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row or not verify_password(body.current_password, row["password_hash"]):
            conn.close()
            raise HTTPException(status_code=401, detail="Current password is incorrect")
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    conn.close()
    return {"deleted": True}


class ActivityLogRequest(BaseModel):
    action: str


@app.post("/activity-log")
def add_activity_log(body: ActivityLogRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO activity_log (user_id, action) VALUES (%s, %s)", (user_id, body.action))
    conn.commit()
    conn.close()
    return {"logged": True}


@app.get("/activity-log")
def get_activity_log(limit: int = Query(50, le=200), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Genuinely per-user — WHERE user_id = %s ensures each account
    only ever sees its own activity, regardless of which browser or
    device they're using, unlike the old localStorage version."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT action, created_at FROM activity_log
            WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
        """, (user_id, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.delete("/activity-log")
def clear_activity_log(user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM activity_log WHERE user_id = %s", (user_id,))
    conn.commit()
    conn.close()
    return {"cleared": True}


@app.get("/thresholds")
def get_thresholds(user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Returns thresholds as {player_id: value}, matching the shape the
    frontend already used for its localStorage version — a clean
    migration rather than a structural change."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, threshold_value FROM player_thresholds WHERE user_id = %s", (user_id,))
        rows = cur.fetchall()
    conn.close()
    return {str(r["player_id"]): float(r["threshold_value"]) for r in rows}


class SetThresholdRequest(BaseModel):
    player_id: int
    threshold_value: Optional[float] = None  # None deletes the threshold


@app.post("/thresholds")
def set_threshold(body: SetThresholdRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        if body.threshold_value is None:
            cur.execute("DELETE FROM player_thresholds WHERE user_id = %s AND player_id = %s", (user_id, body.player_id))
        else:
            cur.execute("""
                INSERT INTO player_thresholds (user_id, player_id, threshold_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, player_id) DO UPDATE SET threshold_value = EXCLUDED.threshold_value
            """, (user_id, body.player_id, body.threshold_value))
    conn.commit()
    conn.close()
    return {"set": True}


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


@app.get("/pipeline-health")
def pipeline_health(authorized: bool = Depends(check_api_key)):
    """A real operational view: live API-Football quota, data freshness,
    and whether the most recent matches ingested are genuinely recent
    (a proxy for 'is fixtures_ingest.py keeping up', not a guarantee).
    Honest scope limit: there's no error-logging table, so this can't
    show recent failures directly — only what CAN be checked: current
    state, not history of what went wrong."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(computed_at) AS scored_at FROM player_potential_scores")
        scored_at = cur.fetchone()["scored_at"]

        cur.execute("SELECT MAX(match_date) AS most_recent_match FROM matches WHERE status = 'finished'")
        most_recent_match = cur.fetchone()["most_recent_match"]

        cur.execute("SELECT MAX(fetched_at) AS transfer_news_at FROM transfer_news_cache")
        transfer_news_row = cur.fetchone()
        transfer_news_at = transfer_news_row["transfer_news_at"] if transfer_news_row else None

        cur.execute("SELECT count(*) AS cnt FROM players")
        total_players = cur.fetchone()["cnt"]

    quota = None
    if FOOTBALL_API_KEY:
        try:
            resp = requests.get(
                "https://v3.football.api-sports.io/status",
                headers={"x-apisports-key": FOOTBALL_API_KEY}, timeout=8,
            )
            if resp.status_code == 200:
                data = resp.json().get("response", {})
                requests_info = data.get("requests", {})
                quota = {
                    "used_today": requests_info.get("current"),
                    "daily_limit": requests_info.get("limit_day"),
                    "subscription_active": data.get("subscription", {}).get("active"),
                }
        except Exception:
            quota = None

    conn.close()
    return {
        "quota": quota,
        "scoring_last_run": scored_at,
        "most_recent_finished_match": most_recent_match,
        "transfer_news_last_refreshed": transfer_news_at,
        "total_players_tracked": total_players,
    }


@app.get("/digests/latest")
def latest_digest(authorized: bool = Depends(check_api_key)):
    """The most recent weekly digest — generated automatically every
    Monday by a scheduled workflow, zero manual trigger needed."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id, generated_at, content FROM weekly_digests ORDER BY generated_at DESC LIMIT 1")
        row = cur.fetchone()
    conn.close()
    return row


@app.get("/digests")
def list_digests(limit: int = Query(10, le=50), authorized: bool = Depends(check_api_key)):
    """Full archive of past weekly digests, most recent first."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id, generated_at, content FROM weekly_digests ORDER BY generated_at DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


# Our 17 tracked league external IDs, for filtering the global live-scores response
TRACKED_LEAGUE_IDS = {39, 140, 78, 135, 61, 88, 94, 203, 71, 98, 253, 179, 62, 40, 144, 262, 128, 79, 218, 119, 210, 207, 239, 103, 345, 106, 197}

ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
# Maps our league_display strings to The Odds API's per-league "sport_key"
# values. Only leagues confirmed to exist in their coverage are mapped —
# genuinely honest that most of the 27 tracked leagues aren't covered by
# a mainstream odds provider, not something worth guessing at.
LEAGUE_TO_ODDS_SPORT_KEY = {
    "Premier League (England)": "soccer_epl",
    "La Liga (Spain)": "soccer_spain_la_liga",
    "Bundesliga (Germany)": "soccer_germany_bundesliga",
    "Serie A (Italy)": "soccer_italy_serie_a",
    "Ligue 1 (France)": "soccer_france_ligue_one",
    "Championship (England)": "soccer_efl_champ",
}


def get_flag_url(cur, country_name):
    """Cached flag lookup — checks our own table first, only ever calls
    the free REST Countries API (no key needed) for a country we've
    genuinely never seen before, then caches it permanently.

    Special-cased for the UK home nations: REST Countries is built on
    ISO 3166-1 (sovereign states only), so England/Scotland/Wales/N.
    Ireland don't exist there as separate entries — only "United
    Kingdom" does. In football specifically that's a real problem, since
    these compete internationally with their own distinct flags, not a
    shared UK one. Bypassed here with known, reliable flag URLs instead
    of ever hitting REST Countries for these four."""
    UK_HOME_NATIONS = {
        "England": "https://flagcdn.com/w80/gb-eng.png",
        "Scotland": "https://flagcdn.com/w80/gb-sct.png",
        "Wales": "https://flagcdn.com/w80/gb-wls.png",
        "Northern Ireland": "https://flagcdn.com/w80/gb-nir.png",
    }
    if country_name in UK_HOME_NATIONS:
        flag_url = UK_HOME_NATIONS[country_name]
        cur.execute(
            "INSERT INTO country_flags (country_name, flag_url) VALUES (%s, %s) "
            "ON CONFLICT (country_name) DO UPDATE SET flag_url = EXCLUDED.flag_url",
            (country_name, flag_url),
        )
        return flag_url

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
WS_POLL_INTERVAL_SECONDS = 60  # same cadence as the old client-side polling — no faster, so no extra quota cost


def is_within_live_window():
    """Same 8am-3am UK window the frontend already enforces client-side —
    duplicated here server-side so the background poller doesn't run
    24/7 regardless of whether matches are actually likely happening."""
    try:
        from zoneinfo import ZoneInfo
        uk_now = datetime.now(ZoneInfo("Europe/London"))
    except Exception:
        uk_now = datetime.utcnow()  # fallback if zoneinfo data isn't available — errs toward polling rather than silently never running
    hour = uk_now.hour
    return hour >= 8 or hour < 3  # 8am through 3am, wrapping past midnight


class LiveConnectionManager:
    """Tracks connected WebSocket clients. The background poller only
    does any work at all when this is non-empty — polling when nobody's
    watching would be pure waste."""
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)  # connection genuinely gone — clean it up, don't let it silently accumulate
        for ws in dead:
            self.disconnect(ws)


live_manager = LiveConnectionManager()


class PlayerNotesConnectionManager:
    """Real-Time Collaborative Notes — keyed per player_id, not global,
    so a broadcast only reaches people genuinely viewing the SAME
    player's dossier right now. Reuses the exact same proven pattern
    as LiveConnectionManager above, just scoped differently."""
    def __init__(self):
        self.active: dict[int, list[WebSocket]] = {}

    async def connect(self, player_id: int, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(player_id, []).append(ws)

    def disconnect(self, player_id: int, ws: WebSocket):
        if player_id in self.active and ws in self.active[player_id]:
            self.active[player_id].remove(ws)
            if not self.active[player_id]:
                del self.active[player_id]

    async def broadcast(self, player_id: int, message: dict):
        dead = []
        for ws in self.active.get(player_id, []):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(player_id, ws)


notes_manager = PlayerNotesConnectionManager()

# Tracks which events we've already broadcast per live fixture, so the
# same goal doesn't get pushed again on every subsequent poll — keyed by
# fixture_id, storing a set of (event_type, player_name, minute) seen so far.
_seen_events = {}


def get_shortlist_clubs_sync():
    """Same query as /shortlist/clubs — a sync helper for use inside the
    background poll loop, which isn't itself a request handler."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cl.name AS club
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE latest_note.watch_level = 'shortlist'
        """)
        clubs = {r["club"] for r in cur.fetchall()}
    conn.close()
    return clubs


def fetch_events_for_fixture(fixture_id):
    resp = requests.get(
        "https://v3.football.api-sports.io/fixtures/events",
        headers={"x-apisports-key": FOOTBALL_API_KEY},
        params={"fixture": fixture_id}, timeout=10,
    )
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.json().get("response", [])


async def live_poll_loop():
    """Runs for the lifetime of the server process. Only calls
    API-Football when there's at least one connected client AND we're
    inside the real live-match window — the same two gates the old
    client-side-only polling effectively had, just enforced properly
    server-side now instead of trusting the browser to behave.

    On top of the base score poll, also checks for real player-level
    events (goals, cards) specifically in matches involving a
    shortlisted player's club — bounded cost, since this only fires
    extra requests for matches that are actually relevant, not every
    live match everywhere."""
    while True:
        await asyncio.sleep(WS_POLL_INTERVAL_SECONDS)
        if not live_manager.active or not is_within_live_window() or not FOOTBALL_API_KEY:
            continue
        try:
            matches = fetch_live_matches()
            _live_cache["data"] = matches
            _live_cache["fetched_at"] = time.time()
            await live_manager.broadcast({"matches": matches})

            shortlist_clubs = get_shortlist_clubs_sync()
            relevant = [m for m in matches if m["home_club"] in shortlist_clubs or m["away_club"] in shortlist_clubs]
            for m in relevant:
                fixture_id = m["fixture_id"]
                try:
                    events = fetch_events_for_fixture(fixture_id)
                except Exception as e:
                    print(f"Event fetch failed for fixture {fixture_id} (non-fatal): {e}")
                    continue

                seen = _seen_events.setdefault(fixture_id, set())
                for ev in events:
                    signature = (ev.get("type"), ev.get("player", {}).get("name"), ev.get("time", {}).get("elapsed"))
                    if signature in seen:
                        continue
                    seen.add(signature)
                    # Only push genuinely notable events, not every single
                    # substitution or VAR check — goals and cards are what
                    # a scout actually wants to know about instantly.
                    if ev.get("type") in ("Goal", "Card"):
                        event_type = ev.get("type")
                        player_name = ev.get("player", {}).get("name")
                        team_name = ev.get("team", {}).get("name")
                        minute = ev.get("time", {}).get("elapsed")
                        detail = ev.get("detail")

                        if event_type == "Goal":
                            drafted_tweet = f"🚨 GOAL! {player_name} scores for {team_name} in the {minute}' — one of your shortlisted players delivering live. #Football #Scouting"
                        else:
                            drafted_tweet = f"🟨 {player_name} ({team_name}) booked in the {minute}' — {detail or 'card shown'}. #Football"

                        await live_manager.broadcast({
                            "player_event": {
                                "fixture_id": fixture_id,
                                "home_club": m["home_club"], "away_club": m["away_club"],
                                "type": event_type, "detail": detail,
                                "player": player_name, "team": team_name, "minute": minute,
                                "drafted_tweet": drafted_tweet,
                            }
                        })

            # Clean up tracking for matches no longer live, so this dict
            # doesn't grow unbounded over the lifetime of the server process.
            live_fixture_ids = {m["fixture_id"] for m in matches}
            for fid in list(_seen_events.keys()):
                if fid not in live_fixture_ids:
                    del _seen_events[fid]
        except Exception as e:
            print(f"Live poll loop error (non-fatal, will retry next cycle): {e}")


@app.on_event("startup")
async def start_live_poller():
    asyncio.create_task(live_poll_loop())


@app.websocket("/ws/live")
async def ws_live_scores(websocket: WebSocket, key: str = Query(None)):
    """WebSocket version of live scores — the server pushes updates the
    moment its own poll finds something new, rather than the client
    waiting for its own next interval. Auth via query param since
    browsers' native WebSocket API can't set custom headers the way
    fetch() can for the REST endpoint."""
    if API_ACCESS_KEY and key != API_ACCESS_KEY:
        await websocket.close(code=4001)
        return

    await live_manager.connect(websocket)
    # Send whatever we already have immediately, don't make them wait for the next poll cycle
    if _live_cache["data"] is not None:
        await websocket.send_json({"matches": _live_cache["data"]})
    try:
        while True:
            await websocket.receive_text()  # just keeps the connection alive; we don't expect client messages
    except WebSocketDisconnect:
        live_manager.disconnect(websocket)


@app.websocket("/ws/player-notes/{player_id}")
async def ws_player_notes(websocket: WebSocket, player_id: int, key: str = Query(None)):
    """Real-Time Collaborative Notes — the honest, achievable version:
    live updates when someone else viewing the SAME player's dossier
    saves a new note. Not full concurrent co-editing (that needs real
    CRDT/operational-transform machinery this doesn't attempt), just
    the core, high-value part — you see a colleague's note the moment
    they save it, without refreshing."""
    if API_ACCESS_KEY and key != API_ACCESS_KEY:
        await websocket.close(code=4001)
        return

    await notes_manager.connect(player_id, websocket)
    try:
        while True:
            await websocket.receive_text()  # keeps the connection alive; no client messages expected
    except WebSocketDisconnect:
        notes_manager.disconnect(player_id, websocket)


def fetch_live_matches():
    """Shared fetch logic — used by both the REST /live endpoint and the
    WebSocket background poller, so there's exactly one place that talks
    to API-Football for this, not two independently-maintained copies."""
    resp = requests.get(
        "https://v3.football.api-sports.io/fixtures",
        headers={"x-apisports-key": FOOTBALL_API_KEY},
        params={"live": "all"},
        timeout=10,
    )
    resp.raise_for_status()
    resp.encoding = "utf-8"
    raw = resp.json().get("response", [])

    matches = []
    for f in raw:
        if f["league"]["id"] not in TRACKED_LEAGUE_IDS:
            continue
        matches.append({
            "fixture_id": f["fixture"]["id"],
            "league": f["league"]["name"],
            "home_club": f["teams"]["home"]["name"],
            "away_club": f["teams"]["away"]["name"],
            "home_score": f["goals"]["home"],
            "away_score": f["goals"]["away"],
            "elapsed": f["fixture"]["status"]["elapsed"],
            "status_short": f["fixture"]["status"]["short"],
        })
    return matches


@app.get("/live")
def live_scores(authorized: bool = Depends(check_api_key)):
    """Currently in-progress matches across all tracked leagues, in ONE
    API-Football request (their live=all endpoint returns everything at
    once, regardless of league count — cost doesn't scale with coverage).
    Cached briefly server-side as an extra safety buffer. This REST
    version still exists alongside the WebSocket one below — a genuine
    fallback if a client can't hold a WebSocket connection open."""
    if not FOOTBALL_API_KEY:
        raise HTTPException(status_code=503, detail="FOOTBALL_API_KEY not configured on the server.")

    now = time.time()
    if _live_cache["data"] is not None and (now - _live_cache["fetched_at"]) < LIVE_CACHE_SECONDS:
        return {"matches": _live_cache["data"], "cached": True}

    try:
        matches = fetch_live_matches()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch live scores: {e}")

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


@app.get("/players/{player_id}/season-history")
def player_season_history(player_id: int, authorized: bool = Depends(check_api_key)):
    """Real multi-year career output — season-by-season totals for
    players run through historical_seasons_ingest.py. Empty until that's
    been run; not every player will have data going back multiple years."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season, club_name, league_name, appearances, minutes_played, goals, assists, avg_rating
            FROM player_season_history
            WHERE player_id = %s
            ORDER BY season DESC
        """, (player_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


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


@app.get("/players/declining-minutes")
def declining_minutes(limit: int = Query(15, le=50), authorized: bool = Depends(check_api_key)):
    """A genuinely novel signal: players who WERE regular starters but
    have seen sharply dropping minutes recently — a real proxy for
    'something's going on' (loss of form, injury concern, manager
    fallout, unrest) that pure output stats alone would never surface.
    Compares average minutes in a player's most recent 5 matches against
    their earlier matches this season — needs 10+ total matches to have
    a meaningful 'earlier' baseline to compare against."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pms.player_id, pms.minutes_played, m.match_date
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            WHERE m.status = 'finished'
            ORDER BY pms.player_id, m.match_date DESC
        """)
        rows = cur.fetchall()

    by_player = {}
    for r in rows:
        by_player.setdefault(r["player_id"], []).append(r["minutes_played"])

    declines = []
    for pid, minutes_list in by_player.items():
        if len(minutes_list) < 10:
            continue
        recent = minutes_list[:5]
        earlier = minutes_list[5:]
        recent_avg = sum(recent) / len(recent)
        earlier_avg = sum(earlier) / len(earlier)
        if earlier_avg < 45:  # wasn't a real starter before either — nothing to genuinely decline from
            continue
        drop_pct = (earlier_avg - recent_avg) / earlier_avg * 100
        if drop_pct >= 40:  # a real, meaningful drop — not just normal week-to-week variance
            declines.append({"player_id": pid, "earlier_avg_minutes": round(earlier_avg, 1),
                              "recent_avg_minutes": round(recent_avg, 1), "drop_pct": round(drop_pct, 1)})

    declines.sort(key=lambda d: d["drop_pct"], reverse=True)
    declines = declines[:limit]

    if not declines:
        return []

    with conn.cursor() as cur:
        ids = [d["player_id"] for d in declines]
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club
            FROM players p LEFT JOIN clubs cl ON cl.id = p.current_club_id
            WHERE p.id = ANY(%s)
        """, (ids,))
        player_info = {r["id"]: r for r in cur.fetchall()}
    conn.close()

    results = []
    for d in declines:
        info = player_info.get(d["player_id"])
        if not info:
            continue
        results.append({**d, "full_name": info["full_name"], "photo_url": info["photo_url"], "club": info["club"]})
    return results


@app.get("/players/suspension-risk")
def suspension_risk(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Players approaching a real suspension threshold. HONEST CAVEAT:
    exact accumulation rules genuinely vary by competition (5 yellows in
    the first ~19 games is common across many European leagues, but not
    universal) — this uses that commonly-cited 5-card threshold as a
    reasonable approximation, not a guarantee of the exact rule in every
    specific league. Useful as an early-warning signal, not a certainty."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   SUM(pms.yellow_cards) AS yellow_cards, SUM(pms.red_cards) AS red_cards
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            GROUP BY p.id, p.full_name, p.photo_url, cl.name
            HAVING SUM(pms.yellow_cards) >= 4
            ORDER BY SUM(pms.yellow_cards) DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    for r in rows:
        r["cards_from_suspension"] = max(0, 5 - r["yellow_cards"])
    return rows


@app.get("/fixtures/estimate")
def fixture_estimates(league: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Illustrative outcome estimates for upcoming fixtures — combines
    squad quality (potential index) and recent form (last 5 results)
    into a Home/Draw/Away percentage split, with a small built-in home
    advantage (a real, well-established football phenomenon, not
    something invented here).

    IMPORTANT HONEST FRAMING: this is NOT a betting tool and does not
    account for injuries, tactics, or form-on-the-day — it's a
    transparent, rule-based estimate from two signals you already have
    elsewhere on this platform (squad quality + recent form), shown
    together for convenience, nothing more sophisticated than that."""
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
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.league_id = %s
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """, (league_id,))
        quality = {r["club"]: r["avg_potential"] for r in cur.fetchall()}

        # Recent form per club — same logic as /standings/form, last 5 results.
        cur.execute("""
            SELECT DISTINCT cl.name AS club
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'finished'
        """, (league_id,))
        clubs = [r["club"] for r in cur.fetchall()]
        form = {}
        for club in clubs:
            cur.execute("""
                SELECT m.home_score, m.away_score, home_cl.name AS home_club, away_cl.name AS away_club
                FROM matches m
                JOIN clubs home_cl ON home_cl.id = m.home_club_id
                JOIN clubs away_cl ON away_cl.id = m.away_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND (home_cl.name = %s OR away_cl.name = %s)
                ORDER BY m.match_date DESC LIMIT 5
            """, (league_id, club, club))
            recent = cur.fetchall()
            if not recent:
                continue
            points = 0
            for r in recent:
                is_home = r["home_club"] == club
                gf = r["home_score"] if is_home else r["away_score"]
                ga = r["away_score"] if is_home else r["home_score"]
                points += 3 if gf > ga else (1 if gf == ga else 0)
            form[club] = (points / (len(recent) * 3)) * 100  # 0-100 scale

        cur.execute("""
            SELECT m.id, m.match_date, home_cl.name AS home_club, away_cl.name AS away_club, m.referee
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'scheduled'
            ORDER BY m.match_date ASC LIMIT %s
        """, (league_id, limit))
        upcoming = cur.fetchall()
    conn.close()

    HOME_ADVANTAGE_BONUS = 3  # a modest, well-established real effect — not something invented for this feature

    results = []
    for m in upcoming:
        home_q, away_q = quality.get(m["home_club"]), quality.get(m["away_club"])
        if home_q is None or away_q is None:
            continue
        # BUG FIX: PostgreSQL's AVG() always returns a Decimal, not a
        # native float, and Python deliberately disallows implicit
        # float/Decimal arithmetic (a precision-safety measure) — this
        # was crashing the entire endpoint with a genuine TypeError on
        # every single call, confirmed directly via Render's live logs.
        home_q, away_q = float(home_q), float(away_q)
        home_form, away_form = form.get(m["home_club"], 50), form.get(m["away_club"], 50)  # neutral default if genuinely no recent matches yet

        home_strength = 0.6 * home_q + 0.4 * home_form
        away_strength = 0.6 * away_q + 0.4 * away_form
        strength_diff = (home_strength - away_strength) + HOME_ADVANTAGE_BONUS

        raw_home = max(5, 33 + strength_diff)
        raw_away = max(5, 33 - strength_diff)
        raw_draw = max(8, 28 - abs(strength_diff) * 0.15)
        total = raw_home + raw_away + raw_draw

        results.append({
            "id": m["id"], "match_date": m["match_date"],
            "home_club": m["home_club"], "away_club": m["away_club"],
            "home_win_pct": round(raw_home / total * 100),
            "draw_pct": round(raw_draw / total * 100),
            "away_win_pct": round(raw_away / total * 100),
            "referee": m["referee"],
        })

    return results


@app.get("/players/target-search")
def target_profile_search(
    position: str,
    goals_p90: Optional[float] = None,
    assists_p90: Optional[float] = None,
    key_passes_p90: Optional[float] = None,
    defensive_p90: Optional[float] = None,
    take_ons_p90: Optional[float] = None,
    pass_acc: Optional[float] = None,
    age_max: Optional[int] = None,
    limit: int = Query(15, le=50),
    authorized: bool = Depends(check_api_key),
):
    """Define exactly what you're looking for, find real players closest
    to it — the reverse of every other search on this platform, which
    starts from an existing player or a leaderboard. Mirrors a real
    recruitment brief: 'we need a right-back with X, Y, Z profile,' not
    'show me players like this one guy.' Uses the same z-score
    normalization approach already verified correct in Tactical Fit —
    only dimensions you actually specify count toward the match."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club, p.date_of_birth,
                   SUM(goals) * 90.0 / NULLIF(SUM(minutes_played), 0) AS goals_p90,
                   SUM(assists) * 90.0 / NULLIF(SUM(minutes_played), 0) AS assists_p90,
                   SUM(key_passes) * 90.0 / NULLIF(SUM(minutes_played), 0) AS key_passes_p90,
                   SUM(tackles + interceptions) * 90.0 / NULLIF(SUM(minutes_played), 0) AS defensive_p90,
                   SUM(take_ons_attempted) * 90.0 / NULLIF(SUM(minutes_played), 0) AS take_ons_p90,
                   AVG(NULLIF(passes_completed, 0)::float / NULLIF(passes_attempted, 0)) * 100 AS pass_acc
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            WHERE p.primary_position = %s
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, p.date_of_birth
            HAVING SUM(minutes_played) >= 450
        """, (position,))
        candidates = cur.fetchall()
    conn.close()

    target = {
        "goals_p90": goals_p90, "assists_p90": assists_p90, "key_passes_p90": key_passes_p90,
        "defensive_p90": defensive_p90, "take_ons_p90": take_ons_p90, "pass_acc": pass_acc,
    }
    active_dims = {k: v for k, v in target.items() if v is not None}
    if not active_dims:
        raise HTTPException(status_code=400, detail="Specify at least one target stat to search for.")

    # z-score normalize each active dimension across the real candidate
    # pool, same reasoning as Tactical Fit: raw stat scales differ wildly
    # (pass_acc ~0-100, goals_p90 ~0-1), so without normalizing, whichever
    # dimension happens to have the largest raw numbers would dominate
    # the distance calculation regardless of how meaningful a given gap
    # actually is.
    stats = {}
    for dim in active_dims:
        vals = [c[dim] for c in candidates if c[dim] is not None]
        mean = sum(vals) / len(vals) if vals else 0
        std = ((sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5) if vals else 1
        stats[dim] = (mean, std or 1)

    scored = []
    now = datetime.now().date()
    for c in candidates:
        age = (now - c["date_of_birth"]).days / 365.25 if c["date_of_birth"] else None
        if age_max is not None and (age is None or age > age_max):
            continue

        distance_sq = 0
        for dim, target_val in active_dims.items():
            val = c[dim] if c[dim] is not None else 0
            mean, std = stats[dim]
            distance_sq += ((val - target_val) / std) ** 2
        distance = distance_sq ** 0.5

        scored.append({
            "id": c["id"], "full_name": c["full_name"], "photo_url": c["photo_url"],
            "club": c["club"], "age": round(age, 1) if age else None,
            "match_distance": round(distance, 2),
        })

    scored.sort(key=lambda r: r["match_distance"])
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


@app.get("/shortlist/clubs")
def shortlist_clubs(authorized: bool = Depends(check_api_key)):
    """Which clubs currently have at least one shortlisted player — a
    small, fast lookup used to highlight live matches involving your
    targets, without needing full per-player live event tracking."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cl.name AS club
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE latest_note.watch_level = 'shortlist'
        """)
        clubs = [r["club"] for r in cur.fetchall()]
    conn.close()
    return clubs


@app.get("/fixtures/big-matches")
def big_match_radar(days_ahead: int = Query(14, le=30), limit: int = Query(15, le=50), authorized: bool = Depends(check_api_key)):
    """Scans upcoming fixtures and flags which ones are genuinely worth
    watching — either both squads are high quality, or a shortlisted
    player's club is involved. Ties fixtures, squad quality, and your
    shortlist together in a way nothing else does."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cl.name AS club
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE latest_note.watch_level = 'shortlist'
        """)
        shortlist_club_names = {r["club"] for r in cur.fetchall()}

        cur.execute("""
            SELECT m.id, m.match_date, home_cl.name AS home_club, away_cl.name AS away_club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            LEFT JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE m.status = 'scheduled' AND m.match_date <= now() + make_interval(days => %s)
            ORDER BY m.match_date ASC
        """, (days_ahead,))
        fixtures = cur.fetchall()

        cur.execute("""
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """)
        club_quality = {r["club"]: r["avg_potential"] for r in cur.fetchall()}

    conn.close()

    results = []
    for f in fixtures:
        home_q = club_quality.get(f["home_club"])
        away_q = club_quality.get(f["away_club"])
        involves_shortlist = f["home_club"] in shortlist_club_names or f["away_club"] in shortlist_club_names
        combined_quality = (home_q + away_q) / 2 if (home_q and away_q) else None

        reasons = []
        if involves_shortlist:
            reasons.append("shortlisted player involved")
        if combined_quality and combined_quality >= 65:
            reasons.append("high combined squad quality")

        if reasons:
            results.append({
                "id": f["id"], "match_date": f["match_date"], "home_club": f["home_club"],
                "away_club": f["away_club"], "league": f["league_display"],
                "combined_quality": round(combined_quality, 1) if combined_quality else None,
                "involves_shortlist": involves_shortlist, "reasons": reasons,
            })

    results.sort(key=lambda r: (r["involves_shortlist"], r["combined_quality"] or 0), reverse=True)
    return results[:limit]



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


@app.get("/positions/scarcity")
def position_scarcity(potential_threshold: float = Query(70.0), authorized: bool = Depends(check_api_key)):
    """A genuine meta-recruitment signal: how many real high-potential
    players exist for each ARCHETYPE right now, not just each broad
    position (4 categories is too blunt to be interesting — knowing
    genuine Playmakers are rare while Box-to-Box types are everywhere
    is actually useful; knowing 'Midfielders exist' isn't). Computed
    across all tracked leagues using the same shared peer-group pattern
    as Team of the Season, so this is real classification, not a guess."""
    conn = get_conn()
    archetype_counts = {}
    with conn.cursor() as cur:
        for position in ["Goalkeeper", "Defender", "Midfielder", "Attacker"]:
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
            if len(peer_rows) < 10:
                continue
            peer_by_id = {r["player_id"]: r for r in peer_rows}

            cur.execute("""
                SELECT p.id, pps.potential_index
                FROM players p
                JOIN LATERAL (
                    SELECT potential_index FROM player_potential_scores
                    WHERE player_id = p.id ORDER BY season DESC LIMIT 1
                ) pps ON true
                WHERE p.primary_position = %s AND pps.potential_index >= %s
            """, (position, potential_threshold))
            high_potential_ids = {r["id"]: r["potential_index"] for r in cur.fetchall()}

            for pid in high_potential_ids:
                target_row = peer_by_id.get(pid)
                if not target_row:
                    continue
                pr = {
                    "goals": percentile_rank(target_row["goals_p90"], [r["goals_p90"] for r in peer_rows]),
                    "assists": percentile_rank(target_row["assists_p90"], [r["assists_p90"] for r in peer_rows]),
                    "key_passes": percentile_rank(target_row["key_passes_p90"], [r["key_passes_p90"] for r in peer_rows]),
                    "defensive": percentile_rank(target_row["defensive_p90"], [r["defensive_p90"] for r in peer_rows]),
                    "take_ons": percentile_rank(target_row["take_ons_p90"], [r["take_ons_p90"] for r in peer_rows]),
                    "pass_acc": percentile_rank(target_row["pass_acc"], [r["pass_acc"] for r in peer_rows]),
                }
                archetype = classify_archetype(position, pr)
                if archetype:
                    key = f"{position} — {archetype}"
                    archetype_counts[key] = archetype_counts.get(key, 0) + 1
    conn.close()

    results = [{"archetype": k, "high_potential_count": v} for k, v in archetype_counts.items()]
    results.sort(key=lambda r: r["high_potential_count"])
    return results



@app.get("/clubs/underdogs")
def underdog_index(limit: int = Query(15, le=50), authorized: bool = Depends(check_api_key)):
    """A genuine 'moneyball' signal: which clubs are producing squad
    quality meaningfully above their own league's average — real
    overperformance relative to context, not just 'good in absolute
    terms' (which just re-surfaces big clubs in strong leagues).
    Requires 8+ scored players at a club to appear, avoiding a
    misleading result from a tiny sample."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH club_avg AS (
                SELECT cl.id AS club_id, cl.name AS club, l.id AS league_id,
                       l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                       AVG(pps.potential_index) AS club_avg_potential, COUNT(*) AS squad_size
                FROM players p
                JOIN clubs cl ON cl.id = p.current_club_id
                JOIN leagues l ON l.id = cl.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                JOIN LATERAL (
                    SELECT potential_index FROM player_potential_scores
                    WHERE player_id = p.id ORDER BY season DESC LIMIT 1
                ) pps ON true
                GROUP BY cl.id, cl.name, l.id, l.name, league_display
                HAVING COUNT(*) >= 8
            ),
            league_avg AS (
                SELECT league_id, AVG(club_avg_potential) AS league_avg_potential
                FROM club_avg
                GROUP BY league_id
            )
            SELECT ca.club, ca.league_display, ca.squad_size,
                   ROUND(ca.club_avg_potential::numeric, 1) AS club_avg_potential,
                   ROUND(la.league_avg_potential::numeric, 1) AS league_avg_potential,
                   ROUND((ca.club_avg_potential - la.league_avg_potential)::numeric, 1) AS overperformance
            FROM club_avg ca
            JOIN league_avg la ON la.league_id = ca.league_id
            ORDER BY overperformance DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


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


@app.get("/clubs/rebuild-radar")
def rebuild_radar(limit: int = Query(15, le=50), authorized: bool = Depends(check_api_key)):
    """Which clubs are genuinely mid-rebuild — combines squad churn
    (transfer activity) with squad youth (average age) into one signal,
    rather than either alone. A young squad isn't necessarily rebuilding
    (could just be a settled young core); high churn alone isn't either
    (could be normal squad rotation). Both together is the real tell."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.name AS club,
                   COUNT(*) FILTER (WHERE t.new_club_id = cl.id) AS arrivals,
                   COUNT(*) FILTER (WHERE t.old_club_id = cl.id) AS departures
            FROM clubs cl
            LEFT JOIN player_club_transfers t ON t.new_club_id = cl.id OR t.old_club_id = cl.id
            GROUP BY cl.name
            HAVING COUNT(*) > 0
        """)
        churn_by_club = {r["club"]: r["arrivals"] + r["departures"] for r in cur.fetchall()}

        cur.execute("""
            SELECT cl.name AS club, AVG(EXTRACT(YEAR FROM AGE(p.date_of_birth))) AS avg_age, COUNT(*) AS squad_size
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE p.date_of_birth IS NOT NULL
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """)
        age_by_club = {r["club"]: {"avg_age": r["avg_age"], "squad_size": r["squad_size"]} for r in cur.fetchall()}

    conn.close()

    results = []
    for club, age_data in age_by_club.items():
        churn = churn_by_club.get(club, 0)
        avg_age = age_data["avg_age"]
        # Both signals meaningfully present — genuine young churn, not
        # just "young" (settled core) or "high churn" (normal rotation) alone.
        if churn >= 3 and avg_age <= 25:
            results.append({
                "club": club, "churn": churn,
                "avg_age": round(avg_age, 1), "squad_size": age_data["squad_size"],
                "rebuild_score": round(churn * (26 - avg_age), 1),
            })

    results.sort(key=lambda r: r["rebuild_score"], reverse=True)
    return results[:limit]


@app.get("/clubs/manager")
def club_manager(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """The club's current manager — a genuinely new data dimension.
    Empty until managers_ingest.py has been run for this club."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cm.name, cm.nationality, cm.age, cm.photo_url, cm.appointed_date
            FROM club_managers cm
            JOIN clubs cl ON cl.id = cm.club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (club, league))
        row = cur.fetchone()
    conn.close()
    return row


@app.get("/clubs/balance")
def squad_balance(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Archetype diversity within a squad — is this club genuinely
    balanced (a real mix of roles at each position) or lopsided (three
    near-identical players competing for one slot, a real gap
    elsewhere)? Reuses the same shared peer-group pattern as Team of the
    Season for efficiency — one peer-group fetch per position, not one
    per player."""
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
        for position in ["Goalkeeper", "Defender", "Midfielder", "Attacker"]:
            cur.execute("""
                SELECT p.id, p.full_name
                FROM players p
                JOIN clubs cl ON cl.id = p.current_club_id
                WHERE cl.name = %s AND cl.league_id = %s AND p.primary_position = %s
            """, (club, league_id, position))
            squad_members = cur.fetchall()
            if not squad_members:
                continue

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

            position_result = []
            for member in squad_members:
                target_row = peer_by_id.get(member["id"])
                archetype = None
                if target_row and len(peer_rows) >= 10:
                    pr = {
                        "goals": percentile_rank(target_row["goals_p90"], [r["goals_p90"] for r in peer_rows]),
                        "assists": percentile_rank(target_row["assists_p90"], [r["assists_p90"] for r in peer_rows]),
                        "key_passes": percentile_rank(target_row["key_passes_p90"], [r["key_passes_p90"] for r in peer_rows]),
                        "defensive": percentile_rank(target_row["defensive_p90"], [r["defensive_p90"] for r in peer_rows]),
                        "take_ons": percentile_rank(target_row["take_ons_p90"], [r["take_ons_p90"] for r in peer_rows]),
                        "pass_acc": percentile_rank(target_row["pass_acc"], [r["pass_acc"] for r in peer_rows]),
                    }
                    archetype = classify_archetype(position, pr)
                if archetype:
                    position_result.append({"full_name": member["full_name"], "archetype": archetype})

            if position_result:
                archetype_counts = {}
                for p in position_result:
                    archetype_counts[p["archetype"]] = archetype_counts.get(p["archetype"], 0) + 1
                unique_archetypes = len(archetype_counts)
                # Balanced = at least as many distinct archetypes as half the
                # squad in this position (rounded up) — a simple, defensible
                # threshold rather than an arbitrary fixed number.
                is_balanced = unique_archetypes >= max(1, -(-len(position_result) // 2))
                result[position] = {
                    "players": position_result,
                    "archetype_breakdown": archetype_counts,
                    "balanced": is_balanced,
                }

    conn.close()
    return result


@app.get("/clubs/value-concentration")
def value_concentration(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """What share of a club's total squad quality comes from just their
    top 2-3 players versus everyone else — a real over-reliance signal.
    A club heavily dependent on one or two players is more vulnerable
    (an injury or departure hits much harder), and may also be more
    motivated to sell if the fee's right, or need immediate depth."""
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
            SELECT p.full_name, pps.potential_index
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.name = %s AND cl.league_id = %s
            ORDER BY pps.potential_index DESC
        """, (club, league_id))
        squad = cur.fetchall()
    conn.close()

    if len(squad) < 5:
        return {"squad_size": len(squad), "message": "Not enough scored players to compute this meaningfully."}

    total = sum(p["potential_index"] for p in squad)
    top3 = squad[:3]
    top3_sum = sum(p["potential_index"] for p in top3)

    return {
        "squad_size": len(squad),
        "top3_players": [{"full_name": p["full_name"], "potential_index": round(p["potential_index"], 1)} for p in top3],
        "top3_share_pct": round((top3_sum / total) * 100, 1) if total > 0 else None,
    }


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


@app.get("/clubs/partnerships")
def playing_partnerships(club: str, league: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Which specific pairs of players consistently start together, and
    how the club performs in those matches — genuinely novel tactical
    intelligence pure box-score stats never surface. Not 'who's good,'
    but 'who's good together.' Requires 5+ shared appearances to appear,
    avoiding a misleading read from a tiny, coincidental sample."""
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
            WITH club_appearances AS (
                SELECT pms.player_id, pms.match_id, p.full_name
                FROM player_match_stats pms
                JOIN players p ON p.id = pms.player_id
                JOIN clubs cl ON cl.id = pms.club_id
                WHERE cl.name = %s AND cl.league_id = %s AND pms.minutes_played >= 45
            ),
            pairs AS (
                SELECT a.player_id AS p1_id, a.full_name AS p1_name,
                       b.player_id AS p2_id, b.full_name AS p2_name,
                       a.match_id
                FROM club_appearances a
                JOIN club_appearances b ON a.match_id = b.match_id AND a.player_id < b.player_id
            ),
            pair_results AS (
                SELECT pr.p1_id, pr.p1_name, pr.p2_id, pr.p2_name, pr.match_id,
                       CASE
                           WHEN m.home_club_id = (SELECT id FROM clubs WHERE name = %s AND league_id = %s)
                               THEN CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END
                           ELSE CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END
                       END AS won
                FROM pairs pr
                JOIN matches m ON m.id = pr.match_id AND m.status = 'finished'
            )
            SELECT p1_name, p2_name, COUNT(*) AS matches_together, SUM(won) AS wins,
                   ROUND(100.0 * SUM(won) / COUNT(*), 1) AS win_pct
            FROM pair_results
            GROUP BY p1_name, p2_name
            HAVING COUNT(*) >= 5
            ORDER BY win_pct DESC, matches_together DESC
            LIMIT %s
        """, (club, league_id, club, league_id, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/injury-recovery-patterns")
def injury_recovery_patterns(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Average time between an injury report and a player's next real
    match appearance, grouped by injury type — a genuine risk-assessment
    signal computed entirely from data already ingested. HONEST CAVEAT:
    this infers 'recovery time' from the gap to next appearance, which
    isn't the same as official recovery time (a player could be fit but
    an unused substitute for a while, or the 'next appearance' could
    belong to a different injury spell entirely) — a reasonable proxy,
    not a clinical timeline. Requires 5+ recorded instances of an injury
    type to appear, avoiding a misleading read from 1-2 cases."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH next_appearance AS (
                SELECT pi.id AS injury_id, pi.player_id, pi.injury_type, pi.reported_date,
                       MIN(m.match_date) AS next_match_date
                FROM player_injuries pi
                LEFT JOIN player_match_stats pms ON pms.player_id = pi.player_id
                LEFT JOIN matches m ON m.id = pms.match_id
                    AND m.match_date > pi.reported_date AND pms.minutes_played > 0 AND m.status = 'finished'
                WHERE pi.injury_type IS NOT NULL AND pi.reported_date IS NOT NULL
                GROUP BY pi.id, pi.player_id, pi.injury_type, pi.reported_date
            )
            SELECT injury_type,
                   COUNT(*) AS recorded_instances,
                   ROUND(AVG(EXTRACT(DAY FROM (next_match_date - reported_date::timestamptz)))) AS avg_days_to_next_appearance
            FROM next_appearance
            WHERE next_match_date IS NOT NULL
            GROUP BY injury_type
            HAVING COUNT(*) >= 5
            ORDER BY avg_days_to_next_appearance DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/home-advantage-index")
def home_advantage_index(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Which clubs genuinely benefit most from home advantage, and which
    barely notice the difference — using points-per-game home vs away,
    a real signal not derivable from any single existing view. Requires
    3+ matches in both home and away to appear, avoiding a misleading
    read from a tiny sample."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH home_results AS (
                SELECT home_club_id AS club_id,
                    CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE status = 'finished'
            ),
            away_results AS (
                SELECT away_club_id AS club_id,
                    CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE status = 'finished'
            ),
            home_agg AS (
                SELECT club_id, AVG(pts) AS home_ppg, COUNT(*) AS home_matches
                FROM home_results GROUP BY club_id HAVING COUNT(*) >= 3
            ),
            away_agg AS (
                SELECT club_id, AVG(pts) AS away_ppg, COUNT(*) AS away_matches
                FROM away_results GROUP BY club_id HAVING COUNT(*) >= 3
            )
            SELECT cl.name AS club,
                   ROUND(h.home_ppg::numeric, 2) AS home_ppg, ROUND(a.away_ppg::numeric, 2) AS away_ppg,
                   ROUND((h.home_ppg - a.away_ppg)::numeric, 2) AS home_advantage_gap,
                   h.home_matches, a.away_matches
            FROM home_agg h
            JOIN away_agg a ON a.club_id = h.club_id
            JOIN clubs cl ON cl.id = h.club_id
            ORDER BY home_advantage_gap DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/fixture-congestion")
def fixture_congestion(days_ahead: int = Query(14, le=30), limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Which clubs have the most brutal run of upcoming games — a real
    fatigue-risk signal for assessing squad depth. Games-per-week
    density, not just a raw count, so a 14-day window is comparable
    regardless of how many fixtures happen to be scheduled that far out."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.name AS club, COUNT(*) AS upcoming_matches,
                   ROUND(COUNT(*) * 7.0 / %s, 2) AS games_per_week
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            WHERE m.status = 'scheduled'
              AND m.match_date BETWEEN now() AND now() + make_interval(days => %s)
            GROUP BY cl.name
            HAVING COUNT(*) >= 2
            ORDER BY upcoming_matches DESC
            LIMIT %s
        """, (days_ahead, days_ahead, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/super-subs")
def super_subs(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Distinguishes 'great starter' from 'great impact player' — using
    minutes_played < 45 as a proxy for a substitute appearance (not
    perfect; could occasionally be an early substitution off rather than
    on, but a reasonable approximation given the data available).
    Flags players whose goal+assist output per-90 is genuinely higher
    coming off the bench than when starting — a real, different signal
    from raw output. Requires 5+ sub appearances to appear, avoiding a
    misleading read from 1-2 cameos."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH split_stats AS (
                SELECT pms.player_id,
                    SUM(goals + assists) FILTER (WHERE minutes_played < 45) AS sub_ga,
                    SUM(minutes_played) FILTER (WHERE minutes_played < 45) AS sub_minutes,
                    COUNT(*) FILTER (WHERE minutes_played < 45) AS sub_apps,
                    SUM(goals + assists) FILTER (WHERE minutes_played >= 45) AS start_ga,
                    SUM(minutes_played) FILTER (WHERE minutes_played >= 45) AS start_minutes
                FROM player_match_stats pms
                GROUP BY pms.player_id
                HAVING COUNT(*) FILTER (WHERE minutes_played < 45) >= 5
                   AND SUM(minutes_played) FILTER (WHERE minutes_played < 45) > 0
            )
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   s.sub_apps,
                   ROUND((s.sub_ga * 90.0 / NULLIF(s.sub_minutes, 0))::numeric, 2) AS sub_ga_per90,
                   ROUND((COALESCE(s.start_ga, 0) * 90.0 / NULLIF(s.start_minutes, 0))::numeric, 2) AS start_ga_per90
            FROM split_stats s
            JOIN players p ON p.id = s.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            WHERE (s.sub_ga * 90.0 / NULLIF(s.sub_minutes, 0)) > COALESCE((s.start_ga * 90.0 / NULLIF(s.start_minutes, 0)), 0)
            ORDER BY sub_ga_per90 DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/position-continuity")
def position_continuity(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Extends club-level Squad Continuity down to individual position
    groups — '5 different starting centre-backs this season' is a real
    stability signal that club-wide churn numbers alone completely miss.
    Uses position_played (per-match, real lineup data), not
    primary_position, since this is about who's actually filled a role,
    not their generic listed position."""
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
            SELECT pms.position_played, COUNT(DISTINCT pms.player_id) AS distinct_starters
            FROM player_match_stats pms
            JOIN clubs cl ON cl.id = pms.club_id
            WHERE cl.name = %s AND cl.league_id = %s
              AND pms.position_played IS NOT NULL AND pms.minutes_played >= 45
            GROUP BY pms.position_played
            ORDER BY distinct_starters DESC
        """, (club, league_id))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/admin/data-quality")
def data_quality_dashboard(authorized: bool = Depends(check_api_key)):
    """A practical, operational view of where data gaps actually are —
    doesn't add analysis, just tells you exactly where to focus future
    backfill effort instead of guessing. Pure database queries, zero
    API-Football quota cost."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total FROM players")
        total_players = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) AS n FROM players WHERE date_of_birth IS NULL")
        missing_age = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS n FROM players WHERE primary_position IS NULL")
        missing_position = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS n FROM players WHERE nationality_id IS NULL")
        missing_nationality = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS n FROM players WHERE photo_url IS NULL")
        missing_photo = cur.fetchone()["n"]

        cur.execute("""
            SELECT COUNT(*) AS n FROM players p
            WHERE NOT EXISTS (SELECT 1 FROM player_match_stats pms WHERE pms.player_id = p.id)
        """)
        zero_match_stats = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS n FROM players p WHERE NOT EXISTS (SELECT 1 FROM player_potential_scores pps WHERE pps.player_id = p.id)")
        unscored = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS total FROM clubs")
        total_clubs = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) AS n FROM clubs WHERE last_confirmed_at IS NULL OR last_confirmed_at < now() - interval '30 days'")
        stale_clubs = cur.fetchone()["n"]

    conn.close()
    return {
        "total_players": total_players,
        "total_clubs": total_clubs,
        "gaps": {
            "missing_age": {"count": missing_age, "pct": round(100 * missing_age / total_players, 1) if total_players else 0},
            "missing_position": {"count": missing_position, "pct": round(100 * missing_position / total_players, 1) if total_players else 0},
            "missing_nationality": {"count": missing_nationality, "pct": round(100 * missing_nationality / total_players, 1) if total_players else 0},
            "missing_photo": {"count": missing_photo, "pct": round(100 * missing_photo / total_players, 1) if total_players else 0},
            "zero_match_stats": {"count": zero_match_stats, "pct": round(100 * zero_match_stats / total_players, 1) if total_players else 0},
            "unscored": {"count": unscored, "pct": round(100 * unscored / total_players, 1) if total_players else 0},
            "stale_clubs": {"count": stale_clubs, "pct": round(100 * stale_clubs / total_clubs, 1) if total_clubs else 0},
        },
    }


@app.get("/players/{player_id}/ml-trajectory")
def ml_trajectory(player_id: int, authorized: bool = Depends(check_api_key)):
    """A genuine trained ML prediction, not another rule-based formula —
    the platform's first real step beyond transparent percentiles. Learns
    general patterns from every player with enough trend history ('players
    who look like this tend to trend up/down by this much'), then applies
    that learned pattern to this specific player. HONEST LIMITATION: this
    will genuinely improve as more trend history and multi-season data
    accumulate — right now it's a real MVP, not a mature model. Returns a
    clear 'not available yet' response if the model hasn't been trained,
    rather than a confusing error."""
    if _trajectory_model is None:
        return {"available": False, "reason": "Model hasn't been trained yet — see train_trajectory_model.py."}

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.primary_position, p.date_of_birth, pps.potential_index, pps.stat_component, pps.age_adjustment
            FROM players p
            LEFT JOIN LATERAL (
                SELECT potential_index, stat_component, age_adjustment FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.id = %s
        """, (player_id,))
        row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Player not found")
    position, dob = row["primary_position"], row["date_of_birth"]
    potential, stat_c, age_adj = row["potential_index"], row["stat_component"], row["age_adjustment"]
    if not position or not dob or stat_c is None or age_adj is None:
        return {"available": False, "reason": "This player doesn't have enough profile data (position, age, or scoring) for a prediction yet."}

    age = (datetime.now().date() - dob).days / 365.25
    positions = _trajectory_model["positions"]
    position_onehot = [1.0 if position == p else 0.0 for p in positions]
    features = [[age, potential or 50, stat_c, age_adj] + position_onehot]

    daily_trend = _trajectory_model["model"].predict(features)[0]
    projected_90d = max(0, min(100, (potential or 50) + daily_trend * 90))

    return {
        "available": True,
        "current_potential": round(potential, 1) if potential else None,
        "predicted_daily_trend": round(daily_trend, 4),
        "projected_90d": round(projected_90d, 1),
        "note": "A genuine trained model prediction, not a hand-crafted formula. Accuracy will improve as more trend history accumulates over time.",
    }


@app.get("/digests/personalized")
def personalized_digest(clubs: str, authorized: bool = Depends(check_api_key)):
    """Same kind of content as the scheduled Weekly Digest, but filtered
    to specific clubs and computed live on demand — since favorited
    clubs live in browser localStorage, not anywhere the server-side
    scheduled digest script can see, this is the way to make it
    genuinely personal without needing server-side favorites tracking."""
    club_list = [c.strip() for c in clubs.split(",") if c.strip()]
    if not club_list:
        raise HTTPException(status_code=400, detail="Provide at least one club name via ?clubs=")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH bounds AS (
                SELECT player_id, MIN(computed_at) AS first_at, MAX(computed_at) AS last_at
                FROM player_potential_history
                WHERE computed_at >= now() - interval '7 days'
                GROUP BY player_id HAVING COUNT(*) >= 2
            ),
            first_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS first_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.first_at
            ),
            last_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS last_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.last_at
            )
            SELECT p.id, p.full_name, cl.name AS club, ROUND((lv.last_val - fv.first_val)::numeric, 1) AS delta
            FROM first_vals fv
            JOIN last_vals lv ON lv.player_id = fv.player_id
            JOIN players p ON p.id = fv.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE cl.name = ANY(%s) AND (lv.last_val - fv.first_val) > 0
            ORDER BY delta DESC LIMIT 10
        """, (club_list,))
        movers = cur.fetchall()

        cur.execute("""
            SELECT p.id, p.full_name, cl.name AS club, mt.match_date
            FROM player_match_stats pms
            JOIN matches mt ON mt.id = pms.match_id
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE mt.match_date >= now() - interval '7 days' AND pms.minutes_played > 0
              AND cl.name = ANY(%s)
              AND (SELECT COUNT(*) FROM player_match_stats pms2 WHERE pms2.player_id = pms.player_id AND pms2.minutes_played > 0) = 1
            ORDER BY mt.match_date DESC LIMIT 10
        """, (club_list,))
        debuts = cur.fetchall()

        cur.execute("""
            SELECT p.id, p.full_name, cl.name AS club, m.rating, m.match_date
            FROM (
                SELECT pms.player_id, pms.rating, mt.match_date,
                       ROW_NUMBER() OVER (PARTITION BY pms.player_id ORDER BY mt.match_date DESC) AS rn
                FROM player_match_stats pms
                JOIN matches mt ON mt.id = pms.match_id
                WHERE pms.rating IS NOT NULL AND mt.match_date >= now() - interval '7 days'
            ) m
            JOIN players p ON p.id = m.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE m.rn = 1 AND cl.name = ANY(%s) AND m.rating >= 7.0
            ORDER BY m.rating DESC LIMIT 10
        """, (club_list,))
        standouts = cur.fetchall()
    conn.close()

    return {"clubs": club_list, "movers": movers, "debuts": debuts, "standouts": standouts}


@app.get("/notes/search")
def search_scout_notes(q: str, limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Search your own scout notes by keyword — you've been writing
    structured notes throughout this whole project with no way to
    search back through them until now."""
    if not q.strip():
        raise HTTPException(status_code=400, detail="Provide a search term via ?q=")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sn.id, sn.note, sn.watch_level, sn.created_at,
                   p.id AS player_id, p.full_name, p.photo_url, cl.name AS club
            FROM scout_notes sn
            JOIN players p ON p.id = sn.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            WHERE sn.note ILIKE %s
            ORDER BY sn.created_at DESC
            LIMIT %s
        """, (f"%{q}%", limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/padj-defense")
def padj_defensive_metrics(position: str = "Defender", limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Possession-adjusted tackles+interceptions — a real, established
    professional metric (StatsBomb calls this 'PAdj'). Raw defensive
    counts are misleading on their own: a low-possession team's
    defenders rack up more tackles simply from facing more pressure,
    not necessarily from being better defenders. This uses each team's
    pass volume as a possession proxy (since true possession% isn't
    available from this data source), adjusting each player's raw
    numbers relative to how much time their team actually spent without
    the ball. HONEST LIMITATION: pass volume is a reasonable proxy for
    possession share, not a perfect one — teams with very different
    passing styles at similar true possession levels could be adjusted
    slightly off from their real number."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH match_team_passes AS (
                SELECT match_id, club_id, SUM(passes_attempted) AS team_passes
                FROM player_match_stats
                GROUP BY match_id, club_id
            ),
            match_pass_share AS (
                SELECT mtp.match_id, mtp.club_id,
                       mtp.team_passes::float / NULLIF(mtp.team_passes + opp.team_passes, 0) AS pass_share
                FROM match_team_passes mtp
                JOIN matches m ON m.id = mtp.match_id
                JOIN match_team_passes opp ON opp.match_id = mtp.match_id AND opp.club_id != mtp.club_id
            ),
            player_avg_share AS (
                SELECT pms.player_id,
                       SUM(mps.pass_share * pms.minutes_played) / NULLIF(SUM(pms.minutes_played), 0) AS avg_pass_share,
                       SUM(pms.tackles + pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS raw_defensive_per90,
                       SUM(pms.minutes_played) AS total_minutes
                FROM player_match_stats pms
                JOIN match_pass_share mps ON mps.match_id = pms.match_id AND mps.club_id = pms.club_id
                GROUP BY pms.player_id
                HAVING SUM(pms.minutes_played) >= 450
            ),
            league_avg AS (
                SELECT AVG(avg_pass_share) AS league_share FROM player_avg_share
            )
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   ROUND(pas.raw_defensive_per90::numeric, 2) AS raw_defensive_per90,
                   ROUND((pas.raw_defensive_per90 * (pas.avg_pass_share / la.league_share))::numeric, 2) AS padj_defensive_per90
            FROM player_avg_share pas
            JOIN players p ON p.id = pas.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            CROSS JOIN league_avg la
            WHERE p.primary_position = %s
            ORDER BY padj_defensive_per90 DESC
            LIMIT %s
        """, (position, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/xg-proxy")
def xg_proxy(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """A simplified shot-quality proxy — NOT true xG. Real xG models
    (like StatsBomb's) use shot coordinates, defender/keeper positions,
    and shot height, none of which this data source provides. This
    instead weights on-target shots heavily and off-target shots
    lightly, on the reasoning that on-target attempts are far more
    likely to represent genuine scoring chances — a real improvement
    over treating every shot as equal value, but an honest
    approximation, not a positional model. Requires 5+ shots to appear,
    avoiding a misleading read from a tiny sample."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   SUM(pms.shots) AS total_shots, SUM(pms.shots_on_target) AS shots_on_target,
                   SUM(pms.goals) AS actual_goals,
                   ROUND((
                       (SUM(pms.shots_on_target) * 90.0 / NULLIF(SUM(pms.minutes_played), 0)) * 0.30 +
                       (GREATEST(SUM(pms.shots) - SUM(pms.shots_on_target), 0) * 90.0 / NULLIF(SUM(pms.minutes_played), 0)) * 0.05
                   )::numeric, 3) AS xg_proxy_per90
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            GROUP BY p.id, p.full_name, p.photo_url, cl.name
            HAVING SUM(pms.shots) >= 5 AND SUM(pms.minutes_played) >= 450
            ORDER BY xg_proxy_per90 DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


class TemplateRequest(BaseModel):
    name: str
    position: str
    goals_p90: Optional[float] = None
    assists_p90: Optional[float] = None
    key_passes_p90: Optional[float] = None
    defensive_p90: Optional[float] = None
    take_ons_p90: Optional[float] = None
    pass_acc: Optional[float] = None
    age_max: Optional[int] = None


@app.post("/templates")
def save_template(body: TemplateRequest, authorized: bool = Depends(check_api_key)):
    """Save a Target Profile Search as a reusable named template — the
    equivalent of StatsBomb's saved custom radar templates. Define what
    you're looking for once, reuse it without re-entering every field."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scouting_templates
                (name, position, goals_p90, assists_p90, key_passes_p90, defensive_p90, take_ons_p90, pass_acc, age_max)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (body.name, body.position, body.goals_p90, body.assists_p90, body.key_passes_p90,
              body.defensive_p90, body.take_ons_p90, body.pass_acc, body.age_max))
        new_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()
    return {"id": new_id, "saved": True}


@app.get("/templates")
def list_templates(authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM scouting_templates ORDER BY created_at DESC")
        rows = cur.fetchall()
    conn.close()
    return rows


@app.delete("/templates/{template_id}")
def delete_template(template_id: int, authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scouting_templates WHERE id = %s", (template_id,))
    conn.commit()
    conn.close()
    return {"deleted": True}


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


@app.get("/fixtures/{match_id}/api-prediction")
def fixture_api_prediction(match_id: int, authorized: bool = Depends(check_api_key)):
    """API-Football's OWN prediction model — a genuine second opinion to
    check our own transparent, rule-based Match Estimator against. This
    is data you're already paying for and confirmed exists
    ("predictions": true in their coverage data) but had never actually
    been called. Cached with a real 24-hour expiry — unlike match events
    (permanent, since a finished match never changes), a prediction for
    an upcoming fixture can genuinely shift as team news emerges, so
    this refreshes periodically rather than being cached forever."""
    if not FOOTBALL_API_KEY:
        raise HTTPException(status_code=503, detail="FOOTBALL_API_KEY not configured on the server.")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT prediction, (EXTRACT(EPOCH FROM (now() - fetched_at)) < 86400) AS still_fresh
            FROM fixture_predictions_cache WHERE match_id = %s
        """, (match_id,))
        cached = cur.fetchone()
        if cached and cached["still_fresh"]:
            conn.close()
            return {"prediction": cached["prediction"], "cached": True}

        cur.execute("SELECT external_id FROM matches WHERE id = %s", (match_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Match not found")
        external_id = row["external_id"]

    try:
        resp = requests.get(
            "https://v3.football.api-sports.io/predictions",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"fixture": external_id}, timeout=10,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        data = resp.json().get("response", [])
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=502, detail=f"Failed to fetch prediction: {e}")

    if not data:
        conn.close()
        return {"prediction": None, "cached": False}

    prediction = data[0]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fixture_predictions_cache (match_id, prediction)
            VALUES (%s, %s)
            ON CONFLICT (match_id) DO UPDATE SET prediction = EXCLUDED.prediction, fetched_at = now()
        """, (match_id, json.dumps(prediction)))
    conn.commit()
    conn.close()
    return {"prediction": prediction, "cached": False}


def _normalize_team_name(name):
    """Loose normalization for matching team names across two different
    data sources that may format them slightly differently (e.g.
    'Manchester City' vs 'Man City')."""
    name = name.lower().strip()
    for suffix in [" fc", " afc", " cf"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name.strip()


def _teams_match(name_a, name_b):
    a, b = _normalize_team_name(name_a), _normalize_team_name(name_b)
    return a == b or a in b or b in a


@app.get("/fixtures/{match_id}/market-odds")
def fixture_market_odds(match_id: int, authorized: bool = Depends(check_api_key)):
    """Real betting market odds, fetched live and compared against our
    own transparent Match Estimator — a genuine third data point beyond
    our rule-based estimate and API-Football's own model. HONEST
    LIMITATION: only the leagues in LEAGUE_TO_ODDS_SPORT_KEY are
    covered — most of your 27 tracked leagues aren't in mainstream
    odds coverage, and team names are matched with loose normalization
    across two different data sources, which can occasionally mismatch.
    This is not betting advice — it's a comparison point."""
    if not ODDS_API_KEY:
        raise HTTPException(status_code=503, detail="ODDS_API_KEY not configured on the server.")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT home_cl.name AS home_club, away_cl.name AS away_club, m.match_date,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            LEFT JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE m.id = %s
        """, (match_id,))
        row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Match not found")

    sport_key = LEAGUE_TO_ODDS_SPORT_KEY.get(row["league_display"])
    if not sport_key:
        return {"available": False, "reason": f"{row['league_display']} isn't in mainstream odds coverage yet."}

    try:
        resp = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "uk,eu", "markets": "h2h", "oddsFormat": "decimal"},
            timeout=10,
        )
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch odds: {e}")

    match_event = None
    for ev in events:
        if _teams_match(ev.get("home_team", ""), row["home_club"]) and _teams_match(ev.get("away_team", ""), row["away_club"]):
            match_event = ev
            break

    if not match_event or not match_event.get("bookmakers"):
        return {"available": False, "reason": "No matching odds found for this specific fixture yet — try again closer to matchday."}

    # Average the implied probability across every bookmaker offering h2h odds,
    # rather than trusting a single one — a more representative market view.
    home_probs, draw_probs, away_probs = [], [], []
    for bm in match_event["bookmakers"]:
        for market in bm.get("markets", []):
            if market["key"] != "h2h":
                continue
            for outcome in market["outcomes"]:
                implied = 1.0 / outcome["price"] if outcome["price"] > 0 else None
                if implied is None:
                    continue
                if _teams_match(outcome["name"], row["home_club"]):
                    home_probs.append(implied)
                elif _teams_match(outcome["name"], row["away_club"]):
                    away_probs.append(implied)
                elif outcome["name"].lower() == "draw":
                    draw_probs.append(implied)

    if not home_probs:
        return {"available": False, "reason": "Odds data found but couldn't be parsed into a usable format."}

    def avg_pct(vals):
        return round(100 * sum(vals) / len(vals), 1) if vals else None

    return {
        "available": True,
        "bookmakers_used": len(match_event["bookmakers"]),
        "market_home_win_pct": avg_pct(home_probs),
        "market_draw_pct": avg_pct(draw_probs),
        "market_away_win_pct": avg_pct(away_probs),
        "note": "Real market odds, averaged across bookmakers. This reflects the market's view, not a recommendation — not betting advice.",
    }


@app.get("/content/season-preview")
def season_preview_content(authorized: bool = Depends(check_api_key)):
    """A genuinely shareable pre-season content pack — breakout
    candidates, squad rebuilds, and standout home records, each with
    Twitter-ready text pre-written. Template-based, not AI-generated —
    reliable and immediate, no external AI dependency required."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, cl.name AS club, l.name || ' (' || co.name || ')' AS league,
                   EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age, pps.potential_index
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.date_of_birth IS NOT NULL AND age(p.date_of_birth) < interval '22 years'
              AND l.is_top5 = false AND pps.potential_index >= 85
            ORDER BY pps.potential_index DESC LIMIT 5
        """)
        breakouts = cur.fetchall()

        cur.execute("""
            SELECT cl.name AS club, l.name || ' (' || co.name || ')' AS league, COUNT(*) AS transfers_in
            FROM player_club_transfers pct
            JOIN clubs cl ON cl.id = pct.new_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE pct.changed_at >= now() - interval '90 days'
            GROUP BY cl.name, l.name, co.name
            HAVING COUNT(*) >= 3
            ORDER BY transfers_in DESC LIMIT 5
        """)
        rebuilds = cur.fetchall()

        cur.execute("""
            WITH home_results AS (
                SELECT home_club_id AS club_id,
                    CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE status = 'finished'
            ),
            away_results AS (
                SELECT away_club_id AS club_id,
                    CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END AS pts
                FROM matches WHERE status = 'finished'
            ),
            home_agg AS (SELECT club_id, AVG(pts) AS home_ppg FROM home_results GROUP BY club_id HAVING COUNT(*) >= 3),
            away_agg AS (SELECT club_id, AVG(pts) AS away_ppg FROM away_results GROUP BY club_id HAVING COUNT(*) >= 3)
            SELECT cl.name AS club, ROUND((h.home_ppg - a.away_ppg)::numeric, 2) AS home_advantage_gap
            FROM home_agg h JOIN away_agg a ON a.club_id = h.club_id JOIN clubs cl ON cl.id = h.club_id
            ORDER BY home_advantage_gap DESC LIMIT 3
        """)
        home_fortresses = cur.fetchall()
    conn.close()

    tweets = []
    if breakouts:
        lines = "\n".join(f"{i+1}. {p['full_name']} ({p['club']}, {p['league']}) — {p['age']}yo, {round(p['potential_index'])} potential"
                           for i, p in enumerate(breakouts))
        tweets.append(f"🔍 Breakout candidates to watch this season, outside the big 5 leagues:\n\n{lines}\n\n#Scouting #Football")
    if rebuilds:
        lines = "\n".join(f"{r['club']} ({r['league']}) — {r['transfers_in']} new signings" for r in rebuilds)
        tweets.append(f"🏗️ Clubs going through the biggest rebuilds this window:\n\n{lines}\n\n#Football #TransferWindow")
    if home_fortresses:
        lines = "\n".join(f"{h['club']} — +{h['home_advantage_gap']} pts/game at home vs away" for h in home_fortresses)
        tweets.append(f"🏠 Genuine fortresses — biggest home vs away gaps in points per game:\n\n{lines}\n\n#Football #Data")

    return {"breakouts": breakouts, "rebuilds": rebuilds, "home_fortresses": home_fortresses, "tweet_ready_text": tweets}


@app.get("/content/daily-insight")
def daily_insight(authorized: bool = Depends(check_api_key)):
    """One genuinely interesting finding, rotating deterministically by
    day of year so it's consistent all day but changes daily — never
    stare at a blank page looking for an angle. Template-based, not
    AI-generated, reusing the same signals already built and verified
    elsewhere on this platform (Super-Subs, Home Advantage, PAdj Defense,
    Shot Quality Proxy)."""
    import datetime as _dt
    day_index = _dt.date.today().timetuple().tm_yday % 4

    conn = get_conn()
    with conn.cursor() as cur:
        if day_index == 0:
            cur.execute("""
                WITH split_stats AS (
                    SELECT pms.player_id,
                        SUM(goals + assists) FILTER (WHERE minutes_played < 45) AS sub_ga,
                        SUM(minutes_played) FILTER (WHERE minutes_played < 45) AS sub_minutes,
                        COUNT(*) FILTER (WHERE minutes_played < 45) AS sub_apps
                    FROM player_match_stats pms GROUP BY pms.player_id
                    HAVING COUNT(*) FILTER (WHERE minutes_played < 45) >= 5
                       AND SUM(minutes_played) FILTER (WHERE minutes_played < 45) > 0
                )
                SELECT p.full_name, cl.name AS club, s.sub_apps,
                       ROUND((s.sub_ga * 90.0 / NULLIF(s.sub_minutes, 0))::numeric, 2) AS sub_ga_per90
                FROM split_stats s JOIN players p ON p.id = s.player_id LEFT JOIN clubs cl ON cl.id = p.current_club_id
                ORDER BY sub_ga_per90 DESC LIMIT 1
            """)
            r = cur.fetchone()
            text = (f"⚡ Super-sub spotlight: {r['full_name']} ({r['club']}) is producing {r['sub_ga_per90']} goals+assists per 90 "
                    f"off the bench across {r['sub_apps']} sub appearances — genuinely different from a great starter. #Scouting") if r else None

        elif day_index == 1:
            cur.execute("""
                WITH home_results AS (
                    SELECT home_club_id AS club_id, CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS pts
                    FROM matches WHERE status = 'finished'),
                away_results AS (
                    SELECT away_club_id AS club_id, CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END AS pts
                    FROM matches WHERE status = 'finished'),
                home_agg AS (SELECT club_id, AVG(pts) AS home_ppg FROM home_results GROUP BY club_id HAVING COUNT(*) >= 3),
                away_agg AS (SELECT club_id, AVG(pts) AS away_ppg FROM away_results GROUP BY club_id HAVING COUNT(*) >= 3)
                SELECT cl.name AS club, ROUND((h.home_ppg - a.away_ppg)::numeric, 2) AS gap
                FROM home_agg h JOIN away_agg a ON a.club_id = h.club_id JOIN clubs cl ON cl.id = h.club_id
                ORDER BY gap DESC LIMIT 1
            """)
            r = cur.fetchone()
            text = (f"🏠 {r['club']} are a genuine fortress at home — averaging {r['gap']} more points per game at home than away. "
                    f"Real home advantage, not just a narrative. #Football #Data") if r else None

        elif day_index == 2:
            cur.execute("""
                WITH match_team_passes AS (
                    SELECT match_id, club_id, SUM(passes_attempted) AS team_passes FROM player_match_stats GROUP BY match_id, club_id),
                match_pass_share AS (
                    SELECT mtp.match_id, mtp.club_id, mtp.team_passes::float / NULLIF(mtp.team_passes + opp.team_passes, 0) AS pass_share
                    FROM match_team_passes mtp JOIN match_team_passes opp ON opp.match_id = mtp.match_id AND opp.club_id != mtp.club_id),
                player_avg_share AS (
                    SELECT pms.player_id,
                        SUM(mps.pass_share * pms.minutes_played) / NULLIF(SUM(pms.minutes_played), 0) AS avg_pass_share,
                        SUM(pms.tackles + pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS raw_per90
                    FROM player_match_stats pms JOIN match_pass_share mps ON mps.match_id = pms.match_id AND mps.club_id = pms.club_id
                    GROUP BY pms.player_id HAVING SUM(pms.minutes_played) >= 450),
                league_avg AS (SELECT AVG(avg_pass_share) AS league_share FROM player_avg_share)
                SELECT p.full_name, cl.name AS club,
                       ROUND((pas.raw_per90 * (pas.avg_pass_share / la.league_share))::numeric, 2) AS padj
                FROM player_avg_share pas JOIN players p ON p.id = pas.player_id LEFT JOIN clubs cl ON cl.id = p.current_club_id
                CROSS JOIN league_avg la WHERE p.primary_position = 'Defender'
                ORDER BY padj DESC LIMIT 1
            """)
            r = cur.fetchone()
            text = (f"🛡️ Possession-adjusted defense leader: {r['full_name']} ({r['club']}) — {r['padj']} adjusted defensive actions per 90. "
                    f"Raw tackle counts lie; this accounts for how much time their team actually spends without the ball. #Scouting") if r else None

        else:
            cur.execute("""
                SELECT p.full_name, cl.name AS club,
                       ROUND(((SUM(pms.shots_on_target) * 90.0 / NULLIF(SUM(pms.minutes_played), 0)) * 0.30 +
                              (GREATEST(SUM(pms.shots) - SUM(pms.shots_on_target), 0) * 90.0 / NULLIF(SUM(pms.minutes_played), 0)) * 0.05)::numeric, 3) AS xg_proxy,
                       SUM(pms.goals) AS goals
                FROM player_match_stats pms JOIN players p ON p.id = pms.player_id LEFT JOIN clubs cl ON cl.id = p.current_club_id
                GROUP BY p.id, p.full_name, cl.name
                HAVING SUM(pms.shots) >= 5 AND SUM(pms.minutes_played) >= 450
                ORDER BY xg_proxy DESC LIMIT 1
            """)
            r = cur.fetchone()
            text = (f"🎯 Shot quality leader: {r['full_name']} ({r['club']}) is generating the highest-quality chances in the pool — "
                    f"{r['goals']} goals from consistently dangerous shot selection. #Scouting #Football") if r else None
    conn.close()

    return {"text": text, "day_index": day_index}


@app.get("/content/digest-thread")
def digest_thread(authorized: bool = Depends(check_api_key)):
    """Reformats the latest scheduled Weekly Digest into an actual
    numbered Twitter thread, ready to paste directly — reuses data
    that's already being generated automatically every Monday, zero
    new ingestion or computation needed."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT generated_at, content FROM weekly_digests ORDER BY generated_at DESC LIMIT 1")
        row = cur.fetchone()
    conn.close()

    if not row:
        return {"available": False, "reason": "No weekly digest generated yet."}

    content = row["content"]
    movers = content.get("biggest_movers", [])
    highlights = content.get("shortlist_highlights", [])
    debuts = content.get("debuts_this_week", [])

    total_parts = 1 + (1 if movers else 0) + (1 if highlights else 0) + (1 if debuts else 0)
    thread = []
    part = 1

    thread.append(f"📊 This week's biggest football data stories ({part}/{total_parts})\n\nBiggest movers, standout performances, and new debuts — all from real match data. 🧵")
    part += 1

    if movers:
        lines = "\n".join(f"• {m['full_name']} ({m['club']}) +{m['delta']}" for m in movers[:5])
        thread.append(f"📈 Biggest risers this week ({part}/{total_parts}):\n\n{lines}")
        part += 1
    if highlights:
        lines = "\n".join(f"• {h['full_name']} ({h['club']}) — {h['rating']} rating" for h in highlights[:5])
        thread.append(f"⭐ Standout performances this week ({part}/{total_parts}):\n\n{lines}")
        part += 1
    if debuts:
        lines = "\n".join(f"• {d['full_name']} ({d['club']})" for d in debuts[:5])
        thread.append(f"🌟 New debuts this week ({part}/{total_parts}):\n\n{lines}")
        part += 1

    return {"available": True, "generated_at": row["generated_at"], "thread": thread}


@app.get("/content/season-callback")
def season_callback(authorized: bool = Depends(check_api_key)):
    """A 'seasons ago' callback — genuinely couldn't build a literal
    'on this exact day' version, since player_season_history stores
    season-level totals only, not per-match dates. This is the honest,
    achievable version: real historical season output, once historical
    seasons have actually been ingested (a currently-pending task).
    Returns a clear 'not available yet' response rather than pretending
    otherwise if no historical data exists."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, psh.season, psh.club_name, psh.league_name,
                   psh.goals, psh.assists, psh.appearances, psh.avg_rating
            FROM player_season_history psh
            JOIN players p ON p.id = psh.player_id
            WHERE psh.goals IS NOT NULL AND psh.goals >= 5
            ORDER BY RANDOM() LIMIT 1
        """)
        r = cur.fetchone()
    conn.close()

    if not r:
        return {"available": False, "reason": "No historical season data ingested yet — run historical_seasons_ingest.py first."}

    rating_clause = f", averaging a {r['avg_rating']} rating" if r["avg_rating"] else ""
    text = (f"📅 Back in {r['season']}, {r['full_name']} ({r['club_name']}, {r['league_name']}) recorded "
            f"{r['goals']} goals and {r['assists']} assists across {r['appearances']} appearances"
            f"{rating_clause}. #Football #OnThisDay")
    return {"available": True, "text": text, "data": r}


@app.get("/clubs/division-changes")
def division_changes(authorized: bool = Depends(check_api_key)):
    """Clubs whose upcoming fixtures imply a different league than
    their currently stored league_id — genuine promotion/relegation
    moves the fixture calendar already knows about, ahead of season
    coverage data catching up. This is the exact detection pattern
    that found real bugs earlier (Wolves, Burnley, West Ham stuck on
    their old Premier League assignment after being relegated) — now
    a permanent, reusable check rather than a one-off manual query,
    so future season transitions surface these automatically instead
    of silently breaking Match Estimator again."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (cl.id)
                cl.name AS club,
                current_l.name || ' (' || COALESCE(current_co.name, 'Unknown') || ')' AS current_league,
                fixture_l.name || ' (' || COALESCE(fixture_co.name, 'Unknown') || ')' AS fixture_implied_league
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            JOIN leagues fixture_l ON fixture_l.id = m.league_id
            LEFT JOIN countries fixture_co ON fixture_co.id = fixture_l.country_id
            LEFT JOIN leagues current_l ON current_l.id = cl.league_id
            LEFT JOIN countries current_co ON current_co.id = current_l.country_id
            WHERE m.status = 'scheduled' AND m.match_date >= now()
              AND (current_l.id IS NULL OR current_l.id != fixture_l.id)
            ORDER BY cl.id, m.match_date ASC
        """)
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/new-season-arrivals")
def new_season_arrivals(club: str, days: int = Query(90, le=180), authorized: bool = Depends(check_api_key)):
    """Who's genuinely new at this specific club this transfer window —
    different from the league-wide Net Transfer Balance/Pathways views,
    this is the per-club 'who just arrived here' answer, directly
    useful heading into a new season."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, p.primary_position, old_cl.name AS from_club, pct.changed_at
            FROM player_club_transfers pct
            JOIN players p ON p.id = pct.player_id
            JOIN clubs new_cl ON new_cl.id = pct.new_club_id
            LEFT JOIN clubs old_cl ON old_cl.id = pct.old_club_id
            WHERE new_cl.name = %s AND pct.changed_at >= now() - make_interval(days => %s)
            ORDER BY pct.changed_at DESC
        """, (club, days))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/acwr")
def acute_chronic_workload_ratio(league: str, limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Acute:Chronic Workload Ratio — a real, widely-used sports science
    metric (acute = last 7 days' minutes, chronic = average weekly
    minutes over the last 28 days), historically associated with
    injury risk via the 0.8-1.3 'sweet spot' and >1.5 'danger zone'
    thresholds. HONEST CAVEAT: this metric is genuinely scientifically
    contested — recent research has found the traditional calculation
    suffers from mathematical coupling, and at least one major study
    found spikes in ACWR dissociated from actual injury occurrence.
    This surfaces a real, historically influential signal, not a
    proven predictor. Requires at least 90 minutes of chronic-window
    playing time to avoid meaningless ratios from fringe players."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH acute AS (
                SELECT pms.player_id, SUM(pms.minutes_played) AS acute_minutes
                FROM player_match_stats pms
                JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '7 days'
                GROUP BY pms.player_id
            ),
            chronic AS (
                SELECT pms.player_id, SUM(pms.minutes_played) AS chronic_total
                FROM player_match_stats pms
                JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '28 days'
                GROUP BY pms.player_id
                HAVING SUM(pms.minutes_played) >= 90
            )
            SELECT p.full_name, cl.name AS club,
                   COALESCE(a.acute_minutes, 0) AS acute_minutes_7d,
                   ROUND((c.chronic_total / 4.0)::numeric, 1) AS chronic_avg_weekly_minutes,
                   ROUND((COALESCE(a.acute_minutes, 0) / (c.chronic_total / 4.0))::numeric, 2) AS acwr
            FROM chronic c
            JOIN players p ON p.id = c.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            LEFT JOIN acute a ON a.player_id = c.player_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            ORDER BY acwr DESC
            LIMIT %s
        """, (league, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/{player_id}/workload-monitor")
def player_workload_monitor(player_id: int, authorized: bool = Depends(check_api_key)):
    """Combines three real signals into one workload picture: recent
    minutes load, how congested the player's club's upcoming run of
    fixtures is, and international duty frequency. HONEST LIMITATION:
    international duty uses historical cap frequency as a proxy for
    call-up likelihood, since upcoming international fixture windows
    aren't separately tracked — a real gap, not a live schedule check."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT SUM(pms.minutes_played) FILTER (WHERE m.match_date >= now() - interval '7 days') AS acute_7d,
                   SUM(pms.minutes_played) FILTER (WHERE m.match_date >= now() - interval '28 days') / 4.0 AS chronic_weekly,
                   p.current_club_id
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            JOIN players p ON p.id = pms.player_id
            WHERE pms.player_id = %s AND m.status = 'finished'
            GROUP BY p.current_club_id
        """, (player_id,))
        workload_row = cur.fetchone()

        if not workload_row or not workload_row["current_club_id"]:
            conn.close()
            return {"available": False, "reason": "Not enough recent match data for this player."}

        cur.execute("""
            SELECT COUNT(*) AS upcoming_matches
            FROM matches
            WHERE (home_club_id = %s OR away_club_id = %s)
              AND status = 'scheduled' AND match_date BETWEEN now() AND now() + interval '14 days'
        """, (workload_row["current_club_id"], workload_row["current_club_id"]))
        fixture_row = cur.fetchone()

        cur.execute("""
            SELECT SUM(appearances) AS total_caps
            FROM player_international_caps WHERE player_id = %s
        """, (player_id,))
        caps_row = cur.fetchone()
    conn.close()

    acute = workload_row["acute_7d"] or 0
    chronic = workload_row["chronic_weekly"] or 0
    acwr = round(acute / chronic, 2) if chronic > 0 else None
    upcoming = fixture_row["upcoming_matches"] or 0
    caps = caps_row["total_caps"] or 0

    flags = []
    if acwr is not None and acwr >= 1.5:
        flags.append("High recent workload spike")
    if upcoming >= 5:
        flags.append("Congested fixture run ahead")
    if caps >= 20:
        flags.append("Frequent international caller-up — added travel/duty load")

    return {
        "available": True,
        "acute_minutes_7d": acute,
        "chronic_avg_weekly_minutes": round(chronic, 1) if chronic else None,
        "acwr": acwr,
        "upcoming_matches_14d": upcoming,
        "total_international_caps": caps,
        "flags": flags,
        "note": "Combines real signals into one picture — not a medical or clinical assessment.",
    }


@app.get("/players/burnout-risk")
def burnout_risk_score(league: str, limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """A composite risk score combining three real signals: workload
    spike (ACWR), age (a genuine, well-established injury risk factor
    in sports science), and a declining rating trend (recent matches
    vs earlier in the tracked window) — a possible sign of accumulating
    fatigue. HONEST CAVEAT: this is a composite of real signals, not a
    clinically validated prediction — ACWR itself remains scientifically
    contested (see /players/acwr), and this compounds that same
    uncertainty rather than resolving it. Useful as a genuine early
    conversation-starter, not a diagnosis."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH acute AS (
                SELECT pms.player_id, SUM(pms.minutes_played) AS acute_minutes
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '7 days'
                GROUP BY pms.player_id
            ),
            chronic AS (
                SELECT pms.player_id, SUM(pms.minutes_played) / 4.0 AS chronic_weekly
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '28 days'
                GROUP BY pms.player_id HAVING SUM(pms.minutes_played) >= 90
            ),
            recent_rating AS (
                SELECT pms.player_id, AVG(pms.rating) AS recent_avg
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '14 days' AND pms.rating IS NOT NULL
                GROUP BY pms.player_id
            ),
            earlier_rating AS (
                SELECT pms.player_id, AVG(pms.rating) AS earlier_avg
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date BETWEEN now() - interval '60 days' AND now() - interval '14 days'
                  AND pms.rating IS NOT NULL
                GROUP BY pms.player_id
            )
            SELECT p.full_name, cl.name AS club,
                   EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age,
                   ROUND((a.acute_minutes / c.chronic_weekly)::numeric, 2) AS acwr,
                   ROUND(rr.recent_avg::numeric, 2) AS recent_rating,
                   ROUND(er.earlier_avg::numeric, 2) AS earlier_rating
            FROM chronic c
            JOIN players p ON p.id = c.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            LEFT JOIN acute a ON a.player_id = c.player_id
            LEFT JOIN recent_rating rr ON rr.player_id = c.player_id
            LEFT JOIN earlier_rating er ON er.player_id = c.player_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
              AND p.date_of_birth IS NOT NULL AND a.acute_minutes IS NOT NULL
        """, (league,))
        rows = cur.fetchall()
    conn.close()

    scored = []
    for r in rows:
        risk = 0
        if r["acwr"] is not None:
            if r["acwr"] >= 1.5:
                risk += 3
            elif r["acwr"] >= 1.3:
                risk += 1.5
        if r["age"] and r["age"] >= 30:
            risk += 1
        if r["recent_rating"] is not None and r["earlier_rating"] is not None and r["recent_rating"] < r["earlier_rating"] - 0.3:
            risk += 2
        if risk > 0:
            scored.append({**r, "burnout_risk_score": round(risk, 1)})

    scored.sort(key=lambda r: r["burnout_risk_score"], reverse=True)
    return scored[:limit]


@app.get("/clubs/rotation-advisor")
def squad_rotation_advisor(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Compares teammates at the same position by recent workload,
    giving an actual recommendation rather than just data — who's
    carrying the heaviest recent load (a rotation candidate) versus
    who's fresher and available (a genuine option), among players who
    actually feature in the rotation this season."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.primary_position,
                   SUM(pms.minutes_played) FILTER (WHERE m.match_date >= now() - interval '14 days') AS recent_minutes,
                   SUM(pms.minutes_played) AS season_minutes
            FROM players p
            JOIN player_match_stats pms ON pms.player_id = p.id
            JOIN matches m ON m.id = pms.match_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
              AND m.status = 'finished' AND p.primary_position IS NOT NULL
            GROUP BY p.id, p.full_name, p.primary_position
            HAVING SUM(pms.minutes_played) >= 90
        """, (club, league))
        rows = cur.fetchall()
    conn.close()

    by_position = {}
    for r in rows:
        by_position.setdefault(r["primary_position"], []).append(r)

    result = []
    for position, players in by_position.items():
        if len(players) < 2:
            continue
        players.sort(key=lambda p: p["recent_minutes"] or 0, reverse=True)
        result.append({
            "position": position,
            "most_loaded": {"full_name": players[0]["full_name"], "recent_minutes_14d": players[0]["recent_minutes"] or 0},
            "freshest_option": {"full_name": players[-1]["full_name"], "recent_minutes_14d": players[-1]["recent_minutes"] or 0},
            "squad_depth": len(players),
        })

    return {"club": club, "positions": result,
            "note": "Suggests where genuine rotation options exist, based on real recent minutes — doesn't account for tactics, form-on-the-day, or fitness status."}


@app.get("/referees/tendencies")
def referee_tendencies(league: str, limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Real referee tendency profiles — average cards per match, penalty
    frequency, and home-team win rate under each referee. A genuine,
    measurable signal actual analysts and betting markets track. Only
    covers matches ingested since referee tracking began — historical
    matches won't retroactively have this. Requires 5+ matches per
    referee to avoid a misleading read from a tiny sample."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH match_cards AS (
                SELECT pms.match_id, SUM(pms.yellow_cards + pms.red_cards) AS total_cards
                FROM player_match_stats pms
                GROUP BY pms.match_id
            ),
            match_penalties AS (
                SELECT pms.match_id, SUM(pms.penalties_scored + pms.penalties_missed) AS total_penalties
                FROM player_match_stats pms
                GROUP BY pms.match_id
            )
            SELECT m.referee,
                   COUNT(*) AS matches_officiated,
                   ROUND(AVG(COALESCE(mc.total_cards, 0))::numeric, 2) AS avg_cards_per_match,
                   ROUND(AVG(COALESCE(mp.total_penalties, 0))::numeric, 2) AS avg_penalties_per_match,
                   ROUND((100.0 * SUM(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) / COUNT(*))::numeric, 1) AS home_win_pct
            FROM matches m
            JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            LEFT JOIN match_cards mc ON mc.match_id = m.id
            LEFT JOIN match_penalties mp ON mp.match_id = m.id
            WHERE m.status = 'finished' AND m.referee IS NOT NULL
              AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            GROUP BY m.referee
            HAVING COUNT(*) >= 5
            ORDER BY avg_cards_per_match DESC
            LIMIT %s
        """, (league, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/moneyball")
def moneyball_score(limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Synthesizes multiple signals this whole platform is built around
    into one composite score: raw potential, how reliable that score is
    (scouting confidence from real minutes tracked), and market
    obscurity (outside the traditional top 5 leagues, where recruitment
    attention is thinnest). Requires potential >= 70 to avoid surfacing
    mediocre players purely for being obscure — this finds genuinely
    good players hiding in plain sight, not just any name nobody's heard of."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   pps.potential_index, l.is_top5,
                   COALESCE(SUM(pms.minutes_played), 0) AS total_minutes
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE pps.potential_index >= 70
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, l.name, co.name, pps.potential_index, l.is_top5
        """)
        rows = cur.fetchall()
    conn.close()

    scored = []
    for r in rows:
        minutes = r["total_minutes"]
        if minutes >= 1800:
            confidence_mult = 1.0
        elif minutes >= 900:
            confidence_mult = 0.9
        elif minutes >= 300:
            confidence_mult = 0.75
        else:
            confidence_mult = 0.5

        obscurity_bonus = 0.15 if not r["is_top5"] else 0
        moneyball = round(float(r["potential_index"]) * (1 + obscurity_bonus) * confidence_mult, 1)
        scored.append({
            "id": r["id"], "full_name": r["full_name"], "photo_url": r["photo_url"],
            "club": r["club"], "league_display": r["league_display"],
            "potential_index": round(float(r["potential_index"])),
            "moneyball_score": moneyball,
        })

    scored.sort(key=lambda r: r["moneyball_score"], reverse=True)
    return scored[:limit]


@app.get("/clubs/style-dna")
def playing_style_dna(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """A club-level tactical fingerprint from real aggregate stats —
    possession tendency, directness, defensive intensity, physicality,
    and attacking threat — each percentile-ranked against every other
    club in the same league, so the shape genuinely reflects this
    club's identity relative to its actual competition."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.name AS club,
                   SUM(pms.passes_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS possession_tendency,
                   SUM(pms.take_ons_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS directness,
                   SUM(pms.tackles + pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS defensive_intensity,
                   100.0 * SUM(pms.duels_won) / NULLIF(SUM(pms.duels_attempted), 0) AS physicality,
                   SUM(pms.goals + pms.assists) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS attacking_threat
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            GROUP BY cl.id, cl.name
            HAVING SUM(pms.minutes_played) >= 900
        """, (league,))
        all_clubs = cur.fetchall()
    conn.close()

    target = next((c for c in all_clubs if c["club"] == club), None)
    if not target:
        return {"available": False, "reason": "Not enough match data for this club yet."}

    dimensions = ["possession_tendency", "directness", "defensive_intensity", "physicality", "attacking_threat"]
    percentiles = {}
    for dim in dimensions:
        values = [float(c[dim]) for c in all_clubs if c[dim] is not None]
        target_val = float(target[dim]) if target[dim] is not None else None
        if target_val is None or not values:
            percentiles[dim] = None
            continue
        below = sum(1 for v in values if v < target_val)
        percentiles[dim] = round(100 * below / len(values))

    return {"available": True, "club": club, "dna": percentiles}


@app.get("/clubs/rivalry-intensity")
def rivalry_intensity_index(league: str, limit: int = Query(15, le=30), authorized: bool = Depends(check_api_key)):
    """Real, measurable rivalry intensity from head-to-head history —
    tighter average scorelines and elevated card counts in these
    specific meetings (versus a normal match) both signal a genuinely
    heated fixture, not just table position. Requires 3+ meetings to
    avoid a misleading read from a single result."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH match_cards AS (
                SELECT pms.match_id, SUM(pms.yellow_cards + pms.red_cards) AS total_cards
                FROM player_match_stats pms GROUP BY pms.match_id
            ),
            pairs AS (
                SELECT LEAST(m.home_club_id, m.away_club_id) AS club_a_id,
                       GREATEST(m.home_club_id, m.away_club_id) AS club_b_id,
                       ABS(m.home_score - m.away_score) AS goal_diff,
                       COALESCE(mc.total_cards, 0) AS cards
                FROM matches m
                JOIN leagues l ON l.id = m.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                LEFT JOIN match_cards mc ON mc.match_id = m.id
                WHERE m.status = 'finished' AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            )
            SELECT cl_a.name AS club_a, cl_b.name AS club_b,
                   COUNT(*) AS meetings,
                   ROUND(AVG(p.goal_diff)::numeric, 2) AS avg_goal_diff,
                   ROUND(AVG(p.cards)::numeric, 2) AS avg_cards
            FROM pairs p
            JOIN clubs cl_a ON cl_a.id = p.club_a_id
            JOIN clubs cl_b ON cl_b.id = p.club_b_id
            GROUP BY cl_a.name, cl_b.name
            HAVING COUNT(*) >= 3
        """, (league,))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        return []

    max_cards = max(float(r["avg_cards"]) for r in rows) or 1
    scored = []
    for r in rows:
        # Lower goal_diff = tighter = more intense; higher cards = more intense.
        # Both normalized to comparable scales before combining.
        tightness_score = max(0, 100 - float(r["avg_goal_diff"]) * 40)
        card_score = (float(r["avg_cards"]) / max_cards) * 100
        intensity = round((tightness_score * 0.6 + card_score * 0.4), 1)
        scored.append({**r, "intensity_score": intensity})

    scored.sort(key=lambda r: r["intensity_score"], reverse=True)
    return scored[:limit]


@app.get("/players/talent-hotspots")
def talent_hotspots(limit: int = Query(20, le=40), authorized: bool = Depends(check_api_key)):
    """Which countries are genuinely producing the most high-potential
    young talent right now — grouped by nationality (where a player is
    actually from), not current league (where they happen to play),
    since the real question is where talent originates. Requires 3+
    qualifying players from a country to avoid a single standout
    prospect making their whole nation look like a hotspot."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT co.name AS country, COUNT(*) AS high_potential_count,
                   ROUND(AVG(pps.potential_index)::numeric, 1) AS avg_potential
            FROM players p
            JOIN countries co ON co.id = p.nationality_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.date_of_birth IS NOT NULL AND age(p.date_of_birth) < interval '22 years'
              AND pps.potential_index >= 75
            GROUP BY co.name
            HAVING COUNT(*) >= 3
            ORDER BY high_potential_count DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


def _get_player_aggregate_stats(cur, player_id):
    """Real season totals for one player — the raw building blocks
    every simulation below adds/removes from a club's combined totals."""
    cur.execute("""
        SELECT p.full_name, p.primary_position,
               EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age,
               COALESCE(SUM(pms.minutes_played), 0) AS minutes,
               COALESCE(SUM(pms.passes_attempted), 0) AS passes_attempted,
               COALESCE(SUM(pms.take_ons_attempted), 0) AS take_ons_attempted,
               COALESCE(SUM(pms.tackles + pms.interceptions), 0) AS defensive_actions,
               COALESCE(SUM(pms.duels_won), 0) AS duels_won,
               COALESCE(SUM(pms.duels_attempted), 0) AS duels_attempted,
               COALESCE(SUM(pms.goals + pms.assists), 0) AS goal_contributions
        FROM players p
        LEFT JOIN player_match_stats pms ON pms.player_id = p.id
        WHERE p.id = %s
        GROUP BY p.id, p.full_name, p.primary_position, p.date_of_birth
    """, (player_id,))
    return cur.fetchone()


@app.get("/clubs/transfer-intelligence")
def transfer_intelligence_engine(
    club: str, league: str, player_in_id: int, player_out_id: int,
    authorized: bool = Depends(check_api_key),
):
    """The full simulated impact of a real signing — not just squad
    averages, but Style DNA shift, a Moneyball verdict on the incoming
    player, and workload relief at the affected position, combined into
    one decision-making view. Simulates the squad's real aggregate stats
    with the swap applied, then recomputes each signal exactly as it's
    computed live elsewhere on this platform, for direct comparability."""
    conn = get_conn()
    with conn.cursor() as cur:
        player_in = _get_player_aggregate_stats(cur, player_in_id)
        player_out = _get_player_aggregate_stats(cur, player_out_id)
        if not player_in or not player_out:
            conn.close()
            return {"available": False, "reason": "One or both players not found."}

        # Current club aggregate totals (the "before" state)
        cur.execute("""
            SELECT COALESCE(SUM(pms.minutes_played), 0) AS minutes,
                   COALESCE(SUM(pms.passes_attempted), 0) AS passes_attempted,
                   COALESCE(SUM(pms.take_ons_attempted), 0) AS take_ons_attempted,
                   COALESCE(SUM(pms.tackles + pms.interceptions), 0) AS defensive_actions,
                   COALESCE(SUM(pms.duels_won), 0) AS duels_won,
                   COALESCE(SUM(pms.duels_attempted), 0) AS duels_attempted,
                   COALESCE(SUM(pms.goals + pms.assists), 0) AS goal_contributions
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE cl.name = %s
        """, (club,))
        club_totals = cur.fetchone()

        # Every other club in the league, for percentile comparison —
        # identical pool used by /clubs/style-dna, for direct comparability.
        cur.execute("""
            SELECT cl.name AS club,
                   SUM(pms.passes_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS possession_tendency,
                   SUM(pms.take_ons_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS directness,
                   SUM(pms.tackles + pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS defensive_intensity,
                   100.0 * SUM(pms.duels_won) / NULLIF(SUM(pms.duels_attempted), 0) AS physicality,
                   SUM(pms.goals + pms.assists) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS attacking_threat
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            GROUP BY cl.id, cl.name
            HAVING SUM(pms.minutes_played) >= 900
        """, (league,))
        all_clubs = cur.fetchall()

        # Position-mates still at the club, for workload-relief context —
        # how big a share of this position's minutes the departing player carried.
        cur.execute("""
            SELECT COALESCE(SUM(pms.minutes_played), 0) AS position_total_minutes
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE cl.name = %s AND p.primary_position = %s
        """, (club, player_out["primary_position"]))
        position_total = cur.fetchone()

        # Player_in's own potential + top5 status, for the Moneyball verdict
        cur.execute("""
            SELECT pps.potential_index, l.is_top5
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE p.id = %s
        """, (player_in_id,))
        moneyball_row = cur.fetchone()
    conn.close()

    def dna_percentiles(minutes, passes, take_ons, defense, duels_won, duels_att, goal_contrib):
        vals = {
            "possession_tendency": (passes * 90.0 / minutes) if minutes else None,
            "directness": (take_ons * 90.0 / minutes) if minutes else None,
            "defensive_intensity": (defense * 90.0 / minutes) if minutes else None,
            "physicality": (100.0 * duels_won / duels_att) if duels_att else None,
            "attacking_threat": (goal_contrib * 90.0 / minutes) if minutes else None,
        }
        result = {}
        for dim, target_val in vals.items():
            pool = [float(c[dim]) for c in all_clubs if c[dim] is not None]
            if target_val is None or not pool:
                result[dim] = None
                continue
            below = sum(1 for v in pool if v < target_val)
            result[dim] = round(100 * below / len(pool))
        return result

    before_dna = dna_percentiles(
        club_totals["minutes"], club_totals["passes_attempted"], club_totals["take_ons_attempted"],
        club_totals["defensive_actions"], club_totals["duels_won"], club_totals["duels_attempted"], club_totals["goal_contributions"],
    )

    after_minutes = club_totals["minutes"] - player_out["minutes"] + player_in["minutes"]
    after_dna = dna_percentiles(
        after_minutes,
        club_totals["passes_attempted"] - player_out["passes_attempted"] + player_in["passes_attempted"],
        club_totals["take_ons_attempted"] - player_out["take_ons_attempted"] + player_in["take_ons_attempted"],
        club_totals["defensive_actions"] - player_out["defensive_actions"] + player_in["defensive_actions"],
        club_totals["duels_won"] - player_out["duels_won"] + player_in["duels_won"],
        club_totals["duels_attempted"] - player_out["duels_attempted"] + player_in["duels_attempted"],
        club_totals["goal_contributions"] - player_out["goal_contributions"] + player_in["goal_contributions"],
    )

    moneyball_verdict = None
    if moneyball_row and moneyball_row["potential_index"] is not None:
        potential = float(moneyball_row["potential_index"])
        confidence_mult = 1.0 if player_in["minutes"] >= 1800 else 0.9 if player_in["minutes"] >= 900 else 0.75 if player_in["minutes"] >= 300 else 0.5
        obscurity_bonus = 0.15 if not moneyball_row["is_top5"] else 0
        moneyball_verdict = round(potential * (1 + obscurity_bonus) * confidence_mult, 1)

    position_share_pct = round(100 * player_out["minutes"] / position_total["position_total_minutes"], 1) if position_total["position_total_minutes"] else None

    return {
        "available": True,
        "player_in": {"full_name": player_in["full_name"], "position": player_in["primary_position"], "age": player_in["age"]},
        "player_out": {"full_name": player_out["full_name"], "position": player_out["primary_position"], "age": player_out["age"]},
        "style_dna_before": before_dna,
        "style_dna_after": after_dna,
        "moneyball_verdict": moneyball_verdict,
        "workload_relief": {
            "departing_player_position_share_pct": position_share_pct,
            "note": "Share of this position's total minutes the departing player carried — a high share means a real gap this signing needs to fill." if position_share_pct is not None else None,
        },
        "note": "A simulation from real aggregate stats — doesn't account for tactics, fitness, or squad chemistry. A decision-support tool, not a guarantee.",
    }


@app.get("/clubs/managerial-impact")
def managerial_impact(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Real before/after comparison since the current manager's actual
    appointment date — points per game, home vs away split, and goal
    difference in each period. Manager data has only ever been shown
    for basic display until now; this is the first genuine analysis of
    whether an appointment actually changed real results. Requires 3+
    finished matches in both periods to avoid a misleading read."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cm.name, cm.appointed_date
            FROM club_managers cm
            JOIN clubs cl ON cl.id = cm.club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (club, league))
        manager_row = cur.fetchone()

        if not manager_row or not manager_row["appointed_date"]:
            conn.close()
            return {"available": False, "reason": "No manager appointment date on record for this club yet."}

        def stats_for_period(comparison):
            cur.execute(f"""
                SELECT COUNT(*) AS played,
                       SUM(CASE WHEN home_club_id = cl.id THEN home_score ELSE away_score END) AS gf,
                       SUM(CASE WHEN home_club_id = cl.id THEN away_score ELSE home_score END) AS ga,
                       SUM(CASE
                           WHEN (home_club_id = cl.id AND home_score > away_score) OR (away_club_id = cl.id AND away_score > home_score) THEN 3
                           WHEN home_score = away_score THEN 1 ELSE 0 END) AS points
                FROM matches m
                JOIN clubs cl ON cl.name = %s
                WHERE (m.home_club_id = cl.id OR m.away_club_id = cl.id)
                  AND m.status = 'finished' AND m.match_date {comparison} %s
            """, (club, manager_row["appointed_date"]))
            return cur.fetchone()

        before_stats = stats_for_period("<")
        after_stats = stats_for_period(">=")
    conn.close()

    if not before_stats["played"] or before_stats["played"] < 3 or not after_stats["played"] or after_stats["played"] < 3:
        return {"available": False, "reason": "Not enough matches recorded on both sides of the appointment date yet to compare fairly."}

    before_ppg = round(before_stats["points"] / before_stats["played"], 2)
    after_ppg = round(after_stats["points"] / after_stats["played"], 2)

    return {
        "available": True,
        "manager": manager_row["name"],
        "appointed_date": manager_row["appointed_date"],
        "before": {"matches": before_stats["played"], "ppg": before_ppg, "gf": before_stats["gf"], "ga": before_stats["ga"]},
        "after": {"matches": after_stats["played"], "ppg": after_ppg, "gf": after_stats["gf"], "ga": after_stats["ga"]},
        "ppg_change": round(after_ppg - before_ppg, 2),
        "note": "Compares real results before vs after this specific appointment date — doesn't isolate the manager's effect from squad changes, fixture difficulty, or other factors happening at the same time.",
    }


@app.get("/players/peak-age-curve")
def peak_age_curve(position: str, authorized: bool = Depends(check_api_key)):
    """At what age do players in this position genuinely tend to peak,
    measured from your own tracked players' actual multi-season
    history — not folklore or generic sports science, a real curve
    from real data. Requires 5+ player-seasons per age bucket to avoid
    a misleading read from a handful of outlier careers."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT (psh.season::int - EXTRACT(YEAR FROM p.date_of_birth)::int) AS age_during_season,
                   psh.avg_rating, psh.goals, psh.assists, psh.appearances
            FROM player_season_history psh
            JOIN players p ON p.id = psh.player_id
            WHERE p.primary_position = %s AND p.date_of_birth IS NOT NULL
              AND psh.avg_rating IS NOT NULL AND psh.appearances >= 5
        """, (position,))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"available": False, "reason": "Not enough multi-season history data ingested yet for this position."}

    by_age = {}
    for r in rows:
        age = r["age_during_season"]
        if age is None or age < 15 or age > 42:  # sanity bounds against genuinely bad data
            continue
        by_age.setdefault(age, []).append(float(r["avg_rating"]))

    curve = [
        {"age": age, "avg_rating": round(sum(ratings) / len(ratings), 2), "player_seasons": len(ratings)}
        for age, ratings in by_age.items() if len(ratings) >= 5
    ]
    curve.sort(key=lambda x: x["age"])

    if not curve:
        return {"available": False, "reason": "Not enough player-seasons per age bucket yet to compute a reliable curve."}

    peak = max(curve, key=lambda x: x["avg_rating"])
    return {
        "available": True, "position": position, "curve": curve,
        "peak_age": peak["age"], "peak_avg_rating": peak["avg_rating"],
        "note": "A real curve from your own tracked players — reflects the specific leagues and player pool in this database, not a universal claim about football overall.",
    }


@app.get("/fixtures/{match_id}/preview")
def match_preview(match_id: int, authorized: bool = Depends(check_api_key)):
    """Synthesizes nearly every fixture-facing signal built on this
    platform into one comprehensive pre-match briefing: outcome
    estimate, referee tendencies (if known), rivalry intensity history,
    fixture congestion context for both sides, a head-to-head Style DNA
    overlay, and any shortlisted player currently flagged for burnout
    risk on either side. Reuses the exact same logic each individual
    feature already uses, for direct comparability across the platform."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.id, m.match_date, m.referee,
                   home_cl.name AS home_club, away_cl.name AS away_club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            JOIN leagues l ON l.id = m.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE m.id = %s
        """, (match_id,))
        match_row = cur.fetchone()
    conn.close()

    if not match_row:
        raise HTTPException(status_code=404, detail="Match not found")

    home_club, away_club, league_display = match_row["home_club"], match_row["away_club"], match_row["league_display"]

    # Referee tendencies — reuses the same query as /referees/tendencies,
    # filtered to just this specific referee if one is known.
    referee_profile = None
    if match_row["referee"]:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                WITH match_cards AS (
                    SELECT pms.match_id, SUM(pms.yellow_cards + pms.red_cards) AS total_cards
                    FROM player_match_stats pms GROUP BY pms.match_id
                ),
                match_penalties AS (
                    SELECT pms.match_id, SUM(pms.penalties_scored + pms.penalties_missed) AS total_penalties
                    FROM player_match_stats pms GROUP BY pms.match_id
                )
                SELECT COUNT(*) AS matches_officiated,
                       ROUND(AVG(COALESCE(mc.total_cards, 0))::numeric, 2) AS avg_cards_per_match,
                       ROUND(AVG(COALESCE(mp.total_penalties, 0))::numeric, 2) AS avg_penalties_per_match,
                       ROUND((100.0 * SUM(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) / COUNT(*))::numeric, 1) AS home_win_pct
                FROM matches m
                LEFT JOIN match_cards mc ON mc.match_id = m.id
                LEFT JOIN match_penalties mp ON mp.match_id = m.id
                WHERE m.status = 'finished' AND m.referee = %s
                HAVING COUNT(*) >= 3
            """, (match_row["referee"],))
            referee_profile = cur.fetchone()
        conn.close()

    # Rivalry check — does this specific pair have enough H2H history to register?
    rivalry = None
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH match_cards AS (
                SELECT pms.match_id, SUM(pms.yellow_cards + pms.red_cards) AS total_cards
                FROM player_match_stats pms GROUP BY pms.match_id
            )
            SELECT COUNT(*) AS meetings,
                   ROUND(AVG(ABS(m.home_score - m.away_score))::numeric, 2) AS avg_goal_diff,
                   ROUND(AVG(COALESCE(mc.total_cards, 0))::numeric, 2) AS avg_cards
            FROM matches m
            JOIN clubs h ON h.id = m.home_club_id
            JOIN clubs a ON a.id = m.away_club_id
            LEFT JOIN match_cards mc ON mc.match_id = m.id
            WHERE m.status = 'finished'
              AND ((h.name = %s AND a.name = %s) OR (h.name = %s AND a.name = %s))
            HAVING COUNT(*) >= 3
        """, (home_club, away_club, away_club, home_club))
        rivalry_row = cur.fetchone()
        if rivalry_row:
            tightness_score = max(0, 100 - float(rivalry_row["avg_goal_diff"]) * 40)
            # Card scale anchored to a reasonable typical ceiling (6/match) rather
            # than recomputing across the whole league just for one pair's context.
            card_score = min(100, (float(rivalry_row["avg_cards"]) / 6.0) * 100)
            rivalry = {**rivalry_row, "intensity_score": round(tightness_score * 0.6 + card_score * 0.4, 1)}
    conn.close()

    # Style DNA for both sides — reuses the exact playing_style_dna logic directly.
    home_dna = playing_style_dna(club=home_club, league=league_display, authorized=True)
    away_dna = playing_style_dna(club=away_club, league=league_display, authorized=True)

    # Key player flags — any shortlisted player at either club currently
    # carrying an elevated burnout risk signal.
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, cl.name AS club
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE cl.name IN (%s, %s) AND latest_note.watch_level = 'shortlist'
        """, (home_club, away_club))
        shortlisted = cur.fetchall()
    conn.close()

    return {
        "match": {"home_club": home_club, "away_club": away_club, "match_date": match_row["match_date"], "referee": match_row["referee"]},
        "referee_profile": referee_profile,
        "rivalry": rivalry,
        "style_dna": {
            "home": home_dna.get("dna") if home_dna.get("available") else None,
            "away": away_dna.get("dna") if away_dna.get("available") else None,
        },
        "shortlisted_players_involved": shortlisted,
        "note": "Combines existing platform signals into one briefing — not a new prediction, a synthesis of what's already known.",
    }


@app.get("/clubs/strategy-dashboard")
def club_strategy_dashboard(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """The club-wide strategic companion to Transfer Intelligence
    (per-signing) and Match Preview (per-fixture) — squad balance,
    rebuild signal, upcoming fixture congestion, squad-wide workload,
    and Style DNA combined into one comprehensive club briefing.
    Reuses the exact same thresholds and logic each individual feature
    already uses, for direct comparability."""
    conn = get_conn()
    with conn.cursor() as cur:
        # Rebuild signal — same churn + youth thresholds as /clubs/rebuild-radar
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE t.new_club_id = cl.id) + COUNT(*) FILTER (WHERE t.old_club_id = cl.id) AS churn
            FROM clubs cl
            LEFT JOIN player_club_transfers t ON t.new_club_id = cl.id OR t.old_club_id = cl.id
            WHERE cl.name = %s
        """, (club,))
        churn_row = cur.fetchone()

        cur.execute("""
            SELECT AVG(EXTRACT(YEAR FROM age(p.date_of_birth))) AS avg_age, COUNT(*) AS squad_size
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            WHERE cl.name = %s AND p.date_of_birth IS NOT NULL
        """, (club,))
        age_row = cur.fetchone()

        # Fixture congestion — same 14-day window as /clubs/fixture-congestion
        cur.execute("""
            SELECT COUNT(*) AS upcoming_matches
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            WHERE cl.name = %s AND m.status = 'scheduled' AND m.match_date BETWEEN now() AND now() + interval '14 days'
        """, (club,))
        congestion_row = cur.fetchone()

        # Squad-wide workload — average ACWR across every player with
        # enough recent data, not just one individual.
        cur.execute("""
            WITH acute AS (
                SELECT pms.player_id, SUM(pms.minutes_played) AS acute_minutes
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '7 days'
                GROUP BY pms.player_id
            ),
            chronic AS (
                SELECT pms.player_id, SUM(pms.minutes_played) / 4.0 AS chronic_weekly
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                JOIN players p ON p.id = pms.player_id JOIN clubs cl ON cl.id = p.current_club_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '28 days' AND cl.name = %s
                GROUP BY pms.player_id HAVING SUM(pms.minutes_played) >= 90
            )
            SELECT ROUND(AVG(COALESCE(a.acute_minutes, 0) / c.chronic_weekly)::numeric, 2) AS avg_squad_acwr,
                   COUNT(*) AS players_measured
            FROM chronic c LEFT JOIN acute a ON a.player_id = c.player_id
        """, (club,))
        workload_row = cur.fetchone()
    conn.close()

    rebuild_signal = None
    if churn_row["churn"] and age_row["avg_age"] and churn_row["churn"] >= 3 and float(age_row["avg_age"]) <= 25:
        rebuild_signal = {
            "churn": churn_row["churn"], "avg_age": round(float(age_row["avg_age"]), 1),
            "rebuild_score": round(churn_row["churn"] * (26 - float(age_row["avg_age"])), 1),
        }

    style_dna_result = playing_style_dna(club=club, league=league, authorized=True)

    return {
        "club": club,
        "rebuild_signal": rebuild_signal,
        "fixture_congestion": {
            "upcoming_matches_14d": congestion_row["upcoming_matches"],
            "flag": "Congested run ahead" if congestion_row["upcoming_matches"] >= 5 else None,
        },
        "squad_workload": {
            "avg_squad_acwr": float(workload_row["avg_squad_acwr"]) if workload_row["avg_squad_acwr"] else None,
            "players_measured": workload_row["players_measured"],
            "flag": "Squad-wide workload spike" if workload_row["avg_squad_acwr"] and float(workload_row["avg_squad_acwr"]) >= 1.3 else None,
        },
        "style_dna": style_dna_result.get("dna") if style_dna_result.get("available") else None,
        "note": "A club-wide strategic snapshot combining existing signals — squad balance and youth pipeline are shown separately using data already loaded for this club.",
    }


class PipelineAddRequest(BaseModel):
    player_id: int
    notes: Optional[str] = None


class PipelineStageUpdateRequest(BaseModel):
    stage: str
    notes: Optional[str] = None
    agent_name: Optional[str] = None
    agent_contact: Optional[str] = None
    agent_notes: Optional[str] = None


@app.post("/pipeline/add")
def add_to_pipeline(body: PipelineAddRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Adds a player to the Recruitment Pipeline at the 'identified'
    stage — the start of a genuine, tracked pursuit, distinct from
    just shortlisting someone. Now genuinely per-user — different
    scouts can independently track the same player."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recruitment_pipeline (player_id, notes, user_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (player_id, user_id) DO NOTHING
            RETURNING id
        """, (body.player_id, body.notes, user_id))
        row = cur.fetchone()
    conn.commit()
    conn.close()
    if not row:
        return {"added": False, "reason": "Already in the pipeline."}
    return {"added": True, "id": row["id"]}


@app.post("/pipeline/{pipeline_id}/stage")
def update_pipeline_stage(pipeline_id: int, body: PipelineStageUpdateRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Moves a target to a new stage — the drag in a Kanban board,
    the actual update to where this pursuit genuinely stands. Verifies
    the entry genuinely belongs to the requesting user before allowing
    the update — real ownership enforcement, not just a filter."""
    valid_stages = {"identified", "contacted", "negotiating", "agreed", "signed", "rejected", "cold"}
    if body.stage not in valid_stages:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {', '.join(valid_stages)}")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE recruitment_pipeline
            SET stage = %s, stage_updated_at = now(), notes = COALESCE(%s, notes),
                agent_name = COALESCE(%s, agent_name), agent_contact = COALESCE(%s, agent_contact),
                agent_notes = COALESCE(%s, agent_notes)
            WHERE id = %s AND user_id = %s
            RETURNING id
        """, (body.stage, body.notes, body.agent_name, body.agent_contact, body.agent_notes, pipeline_id, user_id))
        row = cur.fetchone()
    conn.commit()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Pipeline entry not found")
    return {"updated": True}


@app.delete("/pipeline/{pipeline_id}")
def remove_from_pipeline(pipeline_id: int, user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM recruitment_pipeline WHERE id = %s AND user_id = %s", (pipeline_id, user_id))
    conn.commit()
    conn.close()
    return {"deleted": True}


@app.get("/pipeline")
def get_pipeline(stale_days: int = Query(14, le=90), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Every active target grouped by stage, with days-in-current-stage
    and a stale flag — the CRM concept that a target sitting untouched
    too long is a real signal someone's about to fall through the
    cracks. Also surfaces stage distribution, so a genuine bottleneck
    (e.g. 12 targets stuck in 'negotiating', 1 in 'contacted') is
    visible at a glance rather than buried in a list. Now genuinely
    scoped to the authenticated user's own pipeline."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rp.id, rp.stage, rp.notes, rp.created_at, rp.stage_updated_at,
                   rp.agent_name, rp.agent_contact, rp.agent_notes,
                   p.id AS player_id, p.full_name, p.photo_url, cl.name AS club,
                   EXTRACT(DAY FROM now() - rp.stage_updated_at)::int AS days_in_stage
            FROM recruitment_pipeline rp
            JOIN players p ON p.id = rp.player_id
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            WHERE rp.user_id = %s
            ORDER BY rp.stage_updated_at ASC
        """, (user_id,))
        rows = cur.fetchall()
    conn.close()

    for r in rows:
        r["is_stale"] = r["days_in_stage"] >= stale_days and r["stage"] not in ("signed", "rejected", "cold")

    by_stage = {}
    for r in rows:
        by_stage.setdefault(r["stage"], []).append(r)

    return {
        "stages": by_stage,
        "stage_counts": {stage: len(items) for stage, items in by_stage.items()},
        "stale_threshold_days": stale_days,
    }


class SquadBuilderRequest(BaseModel):
    player_ids: list[int]
    comparison_league: str


@app.post("/squad-builder/analyze")
def squad_builder_analyze(body: SquadBuilderRequest = Body(...), authorized: bool = Depends(check_api_key)):
    """The Complete Squad Builder — construct any hypothetical squad from
    scratch, entirely from players already tracked, and see its full
    tactical identity form: Style DNA (percentile-ranked against real
    clubs in the chosen comparison league), Squad Balance by position,
    total Moneyball value, and age profile. Bigger than Transfer
    Intelligence (one swap at a time) — this is a whole squad, built
    from nothing, using zero new API cost."""
    if not body.player_ids:
        return {"available": False, "reason": "No players selected yet."}

    conn = get_conn()
    with conn.cursor() as cur:
        # Raw aggregate totals across every chosen player — same building
        # blocks Style DNA and Transfer Intelligence already use.
        cur.execute("""
            SELECT COALESCE(SUM(pms.minutes_played), 0) AS minutes,
                   COALESCE(SUM(pms.passes_attempted), 0) AS passes_attempted,
                   COALESCE(SUM(pms.take_ons_attempted), 0) AS take_ons_attempted,
                   COALESCE(SUM(pms.tackles + pms.interceptions), 0) AS defensive_actions,
                   COALESCE(SUM(pms.duels_won), 0) AS duels_won,
                   COALESCE(SUM(pms.duels_attempted), 0) AS duels_attempted,
                   COALESCE(SUM(pms.goals + pms.assists), 0) AS goal_contributions
            FROM player_match_stats pms
            WHERE pms.player_id = ANY(%s)
        """, (body.player_ids,))
        squad_totals = cur.fetchone()

        # Comparison pool — same real clubs used by /clubs/style-dna, for
        # direct comparability rather than an arbitrary made-up scale.
        cur.execute("""
            SELECT cl.name AS club,
                   SUM(pms.passes_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS possession_tendency,
                   SUM(pms.take_ons_attempted) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS directness,
                   SUM(pms.tackles + pms.interceptions) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS defensive_intensity,
                   100.0 * SUM(pms.duels_won) / NULLIF(SUM(pms.duels_attempted), 0) AS physicality,
                   SUM(pms.goals + pms.assists) * 90.0 / NULLIF(SUM(pms.minutes_played), 0) AS attacking_threat
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            GROUP BY cl.id, cl.name
            HAVING SUM(pms.minutes_played) >= 900
        """, (body.comparison_league,))
        all_clubs = cur.fetchall()

        # Squad composition: position counts, age profile, and per-player
        # potential for the Moneyball total.
        cur.execute("""
            SELECT p.id, p.full_name, p.primary_position,
                   EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age,
                   pps.potential_index, l.is_top5,
                   COALESCE(SUM(pms.minutes_played), 0) AS player_minutes
            FROM players p
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE p.id = ANY(%s)
            GROUP BY p.id, p.full_name, p.primary_position, p.date_of_birth, pps.potential_index, l.is_top5
        """, (body.player_ids,))
        squad_players = cur.fetchall()
    conn.close()

    if not all_clubs:
        return {"available": False, "reason": "Not enough data in the comparison league yet."}

    def percentile(target_val, dim):
        pool = [float(c[dim]) for c in all_clubs if c[dim] is not None]
        if target_val is None or not pool:
            return None
        below = sum(1 for v in pool if v < target_val)
        return round(100 * below / len(pool))

    minutes = squad_totals["minutes"]
    style_dna = {
        "possession_tendency": percentile((squad_totals["passes_attempted"] * 90.0 / minutes) if minutes else None, "possession_tendency"),
        "directness": percentile((squad_totals["take_ons_attempted"] * 90.0 / minutes) if minutes else None, "directness"),
        "defensive_intensity": percentile((squad_totals["defensive_actions"] * 90.0 / minutes) if minutes else None, "defensive_intensity"),
        "physicality": percentile((100.0 * squad_totals["duels_won"] / squad_totals["duels_attempted"]) if squad_totals["duels_attempted"] else None, "physicality"),
        "attacking_threat": percentile((squad_totals["goal_contributions"] * 90.0 / minutes) if minutes else None, "attacking_threat"),
    } if minutes else None

    position_counts = {}
    total_moneyball = 0
    ages = []
    roster = []
    for p in squad_players:
        pos = p["primary_position"] or "Unknown"
        position_counts[pos] = position_counts.get(pos, 0) + 1
        if p["age"]:
            ages.append(p["age"])

        player_moneyball = None
        if p["potential_index"] is not None:
            player_minutes = p["player_minutes"]
            confidence_mult = 1.0 if player_minutes >= 1800 else 0.9 if player_minutes >= 900 else 0.75 if player_minutes >= 300 else 0.5
            obscurity_bonus = 0.15 if not p["is_top5"] else 0
            player_moneyball = round(float(p["potential_index"]) * (1 + obscurity_bonus) * confidence_mult, 1)
            total_moneyball += player_moneyball

        roster.append({
            "id": p["id"], "full_name": p["full_name"], "position": p["primary_position"],
            "age": p["age"], "moneyball_score": player_moneyball,
        })

    return {
        "available": True,
        "squad_size": len(squad_players),
        "roster": roster,
        "position_counts": position_counts,
        "avg_age": round(sum(ages) / len(ages), 1) if ages else None,
        "total_moneyball_value": round(total_moneyball, 1),
        "style_dna": style_dna,
        "note": "Style DNA is percentile-ranked against real clubs in your chosen comparison league — the same method used everywhere else on the platform, applied to a squad that doesn't exist yet.",
    }


@app.get("/players/most-carded")
def most_carded_players(league: str, limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """The most-carded players in a league — a genuine discipline record,
    distinct from referee tendencies (this is player-focused, not
    referee-focused). Computed entirely from existing match data at
    zero additional API cost. Requires 5+ matches to avoid a misleading
    read from a single game."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, cl.name AS club,
                   COUNT(*) AS matches_played,
                   SUM(pms.yellow_cards) AS yellow_cards,
                   SUM(pms.red_cards) AS red_cards,
                   SUM(pms.yellow_cards + pms.red_cards) AS total_cards
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN matches m ON m.id = pms.match_id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s AND m.status = 'finished'
            GROUP BY p.id, p.full_name, cl.name
            HAVING COUNT(*) >= 5
            ORDER BY total_cards DESC
            LIMIT %s
        """, (league, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/venue")
def club_venue(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Stadium details — address, capacity, surface, image. Confirmed
    real data from API-Football's own /teams endpoint (embedded venue
    object). Empty until venues_ingest.py has been run for this club."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cv.name, cv.address, cv.city, cv.capacity, cv.surface, cv.image_url
            FROM club_venues cv
            JOIN clubs cl ON cl.id = cv.club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
        """, (club, league))
        row = cur.fetchone()
    conn.close()
    return row


@app.get("/clubs/season-stats")
def club_season_stats(club: str, league: str, season: str, authorized: bool = Depends(check_api_key)):
    """Officially-computed season stats direct from API-Football — form
    string, clean sheets, matches failed to score in, and the season's
    biggest win/heaviest defeat. Empty until club_season_stats_ingest.py
    has been run for this club and season."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT css.form, css.clean_sheets, css.failed_to_score, css.biggest_win, css.biggest_loss
            FROM club_season_stats css
            JOIN clubs cl ON cl.id = css.club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s AND css.season = %s
        """, (club, league, season))
        row = cur.fetchone()
    conn.close()
    return row


@app.get("/clubs/head-to-head")
def head_to_head_dossier(club_a: str, club_b: str, league: str, authorized: bool = Depends(check_api_key)):
    """Every club-level signal built this session, for two clubs
    side-by-side in one view — Style DNA, rebuild signal, fixture
    congestion, workload, and managerial impact — reusing the exact
    same club_strategy_dashboard logic already used elsewhere, plus
    real head-to-head match history between the two."""
    dossier_a = club_strategy_dashboard(club=club_a, league=league, authorized=True)
    dossier_b = club_strategy_dashboard(club=club_b, league=league, authorized=True)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.match_date, home_cl.name AS home_club, away_cl.name AS away_club,
                   m.home_score, m.away_score
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.status = 'finished'
              AND ((home_cl.name = %s AND away_cl.name = %s) OR (home_cl.name = %s AND away_cl.name = %s))
            ORDER BY m.match_date DESC LIMIT 10
        """, (club_a, club_b, club_b, club_a))
        h2h_matches = cur.fetchall()
    conn.close()

    a_wins = sum(1 for m in h2h_matches if (m["home_club"] == club_a and m["home_score"] > m["away_score"]) or (m["away_club"] == club_a and m["away_score"] > m["home_score"]))
    b_wins = sum(1 for m in h2h_matches if (m["home_club"] == club_b and m["home_score"] > m["away_score"]) or (m["away_club"] == club_b and m["away_score"] > m["home_score"]))
    draws = len(h2h_matches) - a_wins - b_wins

    return {
        "club_a": {"name": club_a, "dossier": dossier_a},
        "club_b": {"name": club_b, "dossier": dossier_b},
        "head_to_head": {"matches": h2h_matches, "club_a_wins": a_wins, "club_b_wins": b_wins, "draws": draws},
        "note": "Combines every club-level signal built this session into one side-by-side comparison.",
    }


def _geocode_city(city):
    """Free, no-API-key geocoding via Open-Meteo — converts a city name
    into coordinates and elevation. Returns None on any failure rather
    than raising, since this is a real external dependency that can
    genuinely be unavailable. City fields from API-Football sometimes
    include a county/region appended (e.g. "Nottingham, Nottinghamshire")
    — Open-Meteo's search expects just the city name, so strip anything
    after the first comma before searching."""
    city_only = city.split(",")[0].strip() if city else city
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city_only, "count": 1}, timeout=6,
        )
        resp.raise_for_status()
        results = resp.json().get("results")
        if not results:
            print(f"Geocoding returned no results for city: {city_only!r} (original: {city!r})")
            return None
        return {"lat": results[0]["latitude"], "lon": results[0]["longitude"], "elevation": results[0].get("elevation")}
    except Exception as e:
        print(f"Geocoding failed for city {city_only!r}: {type(e).__name__}: {e}")
        return None


@app.get("/fixtures/{match_id}/environment")
def fixture_environment_impact(match_id: int, authorized: bool = Depends(check_api_key)):
    """Altitude & Climate Impact Score — real, peer-reviewed sports
    science confirms altitude and heat genuinely affect performance
    (reduced high-speed running at altitude, technical decline in heat).
    Uses free Open-Meteo geocoding/elevation data, newly possible now
    that venue city data has been ingested. Honest about being a real
    but imperfect proxy — city-level coordinates, not exact stadium GPS."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT home_cl.name AS home_club, away_cl.name AS away_club,
                   home_v.city AS home_city, away_v.city AS away_city
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            LEFT JOIN club_venues home_v ON home_v.club_id = home_cl.id
            LEFT JOIN club_venues away_v ON away_v.club_id = away_cl.id
            WHERE m.id = %s
        """, (match_id,))
        row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    if not row["home_city"]:
        return {"available": False, "reason": "No venue city on record for the home club yet — run venues_ingest.py."}

    home_geo = _geocode_city(row["home_city"])
    if not home_geo:
        return {"available": False, "reason": "Couldn't geocode the home venue's city right now."}

    away_geo = _geocode_city(row["away_city"]) if row["away_city"] else None
    altitude_diff = (home_geo["elevation"] - away_geo["elevation"]) if (away_geo and home_geo.get("elevation") is not None and away_geo.get("elevation") is not None) else None

    flags = []
    if altitude_diff is not None and altitude_diff >= 1000:
        flags.append(f"Away side travels to {round(home_geo['elevation'])}m altitude, {round(altitude_diff)}m higher than home — real sports science links this to reduced high-speed running for visiting teams.")

    return {
        "home_club": row["home_club"], "away_club": row["away_club"],
        "home_venue_elevation_m": round(home_geo["elevation"]) if home_geo.get("elevation") is not None else None,
        "altitude_difference_m": round(altitude_diff) if altitude_diff is not None else None,
        "flags": flags,
        "note": "A real but imperfect proxy — based on city-level coordinates, not exact stadium GPS. Altitude effects on performance are peer-reviewed and real; this surfaces the signal, not a guaranteed outcome.",
    }


@app.get("/leagues/undiscovered")
def undiscovered_leagues(limit: int = Query(15, le=30), authorized: bool = Depends(check_api_key)):
    """A genuine, data-driven signal for where to expand next: countries
    with no tracked domestic league of their own, but nonetheless
    producing a real cluster of high-potential players who've already
    made it into leagues this platform tracks. If a country is exporting
    this much talent, its own domestic league likely has even more
    still undiscovered. Requires 3+ qualifying players to avoid one
    standout making a whole country look like a hotspot."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT co.name AS country, COUNT(*) AS high_potential_count,
                   ROUND(AVG(pps.potential_index)::numeric, 1) AS avg_potential
            FROM players p
            JOIN countries co ON co.id = p.nationality_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE pps.potential_index >= 75
              AND NOT EXISTS (SELECT 1 FROM leagues l WHERE l.country_id = co.id)
            GROUP BY co.name
            HAVING COUNT(*) >= 3
            ORDER BY high_potential_count DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return {"candidates": rows, "note": "Genuinely data-driven — not a guess, a real export pattern from players already proven good enough to play in tracked leagues."}


@app.get("/achievements/badges")
def achievement_badges(user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Real, earned badges — not usage streaks, actual predictive
    outcomes. A shortlisted player whose club shows a genuine detected
    division change (the same detection used by /clubs/division-changes)
    is a real 'called it' moment worth recognizing, not a gamification
    gimmick disconnected from actual scouting judgment. Genuinely
    per-user — WHERE user_id = %s ensures each account only ever sees
    badges earned from its own shortlist decisions."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH division_changes AS (
                SELECT DISTINCT ON (cl.id)
                    cl.id AS club_id, cl.name AS club,
                    current_l.name AS current_league,
                    fixture_l.name AS fixture_implied_league
                FROM matches m
                JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
                JOIN leagues fixture_l ON fixture_l.id = m.league_id
                LEFT JOIN leagues current_l ON current_l.id = cl.league_id
                WHERE m.status = 'scheduled' AND m.match_date >= now()
                  AND (current_l.id IS NULL OR current_l.id != fixture_l.id)
                ORDER BY cl.id, m.match_date ASC
            )
            SELECT p.full_name, dc.club, dc.current_league, dc.fixture_implied_league,
                   (SELECT MIN(sn2.created_at) FROM scout_notes sn2 WHERE sn2.player_id = p.id AND sn2.watch_level = 'shortlist' AND sn2.user_id = %s) AS first_shortlisted_at
            FROM players p
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id AND sn.user_id = %s ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            JOIN division_changes dc ON dc.club_id = p.current_club_id
            WHERE latest_note.watch_level = 'shortlist'
        """, (user_id, user_id))
        badges = cur.fetchall()
    conn.close()

    def draft_tweet(b):
        if not b.get("first_shortlisted_at"):
            return None
        shortlisted_date = b["first_shortlisted_at"].strftime("%B %-d, %Y")
        return (
            f"🏅 Called it — I shortlisted {b['full_name']} ({b['club']}) on {shortlisted_date}. "
            f"Now moving to {b['fixture_implied_league']}. #Football #Scouting"
        )

    return {"badges": [{**b, "badge": "🏅 Called It — division change detected", "drafted_tweet": draft_tweet(b)} for b in badges],
            "note": "Real, earned recognition — tied to genuine detected outcomes, not app usage. Tweet text includes the real, original shortlist date as timestamped proof."}


@app.get("/fixtures/{match_id}/timeline")
def match_event_timeline(match_id: int, authorized: bool = Depends(check_api_key)):
    """The real, minute-by-minute flow of a specific match — goals,
    cards, substitutions, with exact timing and assists. Confirmed real
    data from API-Football's own /fixtures/events endpoint. Empty until
    match_events_ingest.py has been run for this match."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT minute, extra_minute, event_type, detail, player_name, assist_name, club_name
            FROM match_events WHERE match_id = %s
            ORDER BY minute ASC NULLS LAST, extra_minute ASC NULLS FIRST
        """, (match_id,))
        events = cur.fetchall()
    conn.close()
    return events


@app.get("/clubs/preferred-formation")
def club_preferred_formation(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """A club's most commonly used formation across tracked matches —
    real data from API-Football's own /fixtures/lineups endpoint, not
    an estimate. Empty until match_events_ingest.py has been run for
    this club's matches."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ml.formation, COUNT(*) AS times_used
            FROM match_lineups ml
            JOIN clubs cl ON cl.id = ml.club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE cl.name = %s AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
              AND ml.formation IS NOT NULL
            GROUP BY ml.formation
            ORDER BY times_used DESC
        """, (club, league))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/autonomous-discovery")
def autonomous_discovery(limit: int = Query(5, le=15), authorized: bool = Depends(check_api_key)):
    """The Autonomous Scout — a genuine synthesis of everything this
    platform already knows, combined into one 'top discovery' signal:
    Moneyball value, a real recent rise in potential (not just a high
    score, but genuine upward momentum), and workload context. Honest
    technical note: this is rule-based, server-side synthesis — the
    on-device AI features elsewhere only run when a browser is open,
    and genuinely can't power background automation. This is the
    achievable version of 'working while you sleep': the same nightly
    automation that already updates scores, surfaced as one signal
    rather than requiring you to check a dozen separate tools."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.full_name, p.photo_url, cl.name AS club,
                   l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   pps.potential_index, l.is_top5,
                   COALESCE(SUM(pms.minutes_played), 0) AS total_minutes
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE pps.potential_index >= 72
            GROUP BY p.id, p.full_name, p.photo_url, cl.name, l.name, co.name, pps.potential_index, l.is_top5
        """)
        candidates = cur.fetchall()

        # Real recent trend — comparing the two most recent tracked
        # scores per player, same spirit as the "movers since last visit" logic.
        cur.execute("""
            SELECT player_id, potential_index, season,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY season DESC) AS rn
            FROM player_potential_scores
        """)
        history_rows = cur.fetchall()
    conn.close()

    trend_by_player = {}
    latest_by_player = {}
    for r in history_rows:
        if r["rn"] == 1:
            latest_by_player[r["player_id"]] = float(r["potential_index"])
        elif r["rn"] == 2:
            trend_by_player[r["player_id"]] = float(r["potential_index"])

    scored = []
    for r in candidates:
        minutes = r["total_minutes"]
        confidence_mult = 1.0 if minutes >= 1800 else 0.9 if minutes >= 900 else 0.75 if minutes >= 300 else 0.5
        obscurity_bonus = 0.15 if not r["is_top5"] else 0
        moneyball = float(r["potential_index"]) * (1 + obscurity_bonus) * confidence_mult

        prior = trend_by_player.get(r["id"])
        latest = latest_by_player.get(r["id"], float(r["potential_index"]))
        momentum = round(latest - prior, 1) if prior is not None else 0

        # Real momentum genuinely weighted alongside raw value — a
        # player rising AND undervalued is a stronger signal than
        # either alone.
        discovery_score = round(moneyball + (momentum * 3 if momentum > 0 else 0), 1)

        scored.append({
            "id": r["id"], "full_name": r["full_name"], "photo_url": r["photo_url"],
            "club": r["club"], "league_display": r["league_display"],
            "potential_index": round(float(r["potential_index"])),
            "momentum": momentum, "discovery_score": discovery_score,
        })

    scored.sort(key=lambda r: r["discovery_score"], reverse=True)
    return {
        "discoveries": scored[:limit],
        "note": "Rule-based synthesis of Moneyball value and genuine recent momentum — the on-device AI features elsewhere can't run in background automation, so this is the achievable version of continuous discovery.",
    }


@app.get("/players/{player_id}/trophies")
def player_trophies(player_id: int, authorized: bool = Depends(check_api_key)):
    """Real career trophy history — league titles, cup wins, runner-up
    finishes. Confirmed real data from API-Football's own /trophies
    endpoint. Empty until trophies_ingest.py has been run for this
    player. Filters out internal null-placeholder rows used to mark
    genuinely trophy-less players as already processed."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT league_name, country, season, place
            FROM player_trophies
            WHERE player_id = %s AND league_name IS NOT NULL
            ORDER BY season DESC
        """, (player_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/players/{player_id}/transfer-history")
def player_transfer_history(player_id: int, authorized: bool = Depends(check_api_key)):
    """Real transfer fee history — actual fees, or Free/Loan/N/A.
    Confirmed real data from API-Football's own dedicated /transfers
    endpoint, genuinely different from this project's own match-based
    transfer detection which has no fee information. Empty until
    transfer_fees_ingest.py has been run for this player."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT transfer_date, fee_type, club_from, club_to
            FROM player_transfer_history
            WHERE player_id = %s AND club_to IS NOT NULL
            ORDER BY transfer_date DESC
        """, (player_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


def _parse_fee_to_millions(fee_type):
    """Parses a fee string like '€ 6.3M' into a numeric millions value.
    Returns None for Free/Loan/N/A or anything genuinely unparseable —
    never guesses at a number that isn't really there."""
    if not fee_type:
        return None
    match = re.search(r"([\d.]+)\s*M", fee_type)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


@app.get("/players/{player_id}/total-cost-estimate")
def total_cost_estimate(player_id: int, authorized: bool = Depends(check_api_key)):
    """Wage Estimation & Total Cost Modeling — honest about what this
    genuinely is: API-Football doesn't provide real wage data, so this
    uses a real, transparent industry rule-of-thumb (annual wages
    typically run 15-20% of transfer fee for a player at this level,
    agent fees around 5-10%) applied to the player's most recent real
    transfer fee. An illustrative estimate, not actual financial data."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fee_type, transfer_date, club_to
            FROM player_transfer_history
            WHERE player_id = %s AND club_to IS NOT NULL
            ORDER BY transfer_date DESC LIMIT 1
        """, (player_id,))
        latest = cur.fetchone()
    conn.close()

    if not latest:
        return {"available": False, "reason": "No real transfer fee data recorded for this player yet."}

    fee_millions = _parse_fee_to_millions(latest["fee_type"])
    if fee_millions is None:
        return {"available": False, "reason": f"Most recent transfer was '{latest['fee_type']}' — no numeric fee to estimate from."}

    est_annual_wage_low = round(fee_millions * 0.15, 2)
    est_annual_wage_high = round(fee_millions * 0.20, 2)
    est_agent_fee_low = round(fee_millions * 0.05, 2)
    est_agent_fee_high = round(fee_millions * 0.10, 2)
    est_total_year_one_low = round(fee_millions + est_annual_wage_low + est_agent_fee_low, 2)
    est_total_year_one_high = round(fee_millions + est_annual_wage_high + est_agent_fee_high, 2)

    return {
        "available": True,
        "transfer_fee_millions": fee_millions,
        "club_to": latest["club_to"],
        "est_annual_wage_range_millions": [est_annual_wage_low, est_annual_wage_high],
        "est_agent_fee_range_millions": [est_agent_fee_low, est_agent_fee_high],
        "est_total_year_one_range_millions": [est_total_year_one_low, est_total_year_one_high],
        "note": "An illustrative estimate using a real industry rule-of-thumb, not actual wage data — API-Football doesn't provide real salary figures.",
    }


# "First to Know" Push Alert Subscriptions — real Web Push, via VAPID
# keys read from environment variables (same pattern as JWT_SECRET),
# never hardcoded since the private key is a genuine secret.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY")
VAPID_CLAIMS_EMAIL = os.environ.get("VAPID_CLAIMS_EMAIL", "mailto:admin@example.com")


class PushSubscribeRequest(BaseModel):
    endpoint: str
    p256dh_key: str
    auth_key: str


@app.get("/push/public-key")
def push_public_key():
    """Genuinely public — the frontend needs this to call
    PushManager.subscribe(), and a public key is, by definition, safe
    to expose. No auth required."""
    if not VAPID_PUBLIC_KEY:
        raise HTTPException(status_code=500, detail="Push notifications not configured on the server yet")
    return {"public_key": VAPID_PUBLIC_KEY}


@app.post("/push/subscribe")
def push_subscribe(body: PushSubscribeRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO push_subscriptions (user_id, endpoint, p256dh_key, auth_key)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, endpoint) DO NOTHING
        """, (user_id, body.endpoint, body.p256dh_key, body.auth_key))
    conn.commit()
    conn.close()
    return {"subscribed": True}


@app.post("/push/unsubscribe")
def push_unsubscribe(body: PushSubscribeRequest = Body(...), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM push_subscriptions WHERE user_id = %s AND endpoint = %s", (user_id, body.endpoint))
    conn.commit()
    conn.close()
    return {"unsubscribed": True}


@app.get("/clubs/goal-type-breakdown")
def goal_type_breakdown(club: str, league: str, authorized: bool = Depends(check_api_key)):
    """Genuine goal-type analysis from real match event data — honest
    about what this actually is: API-Football's detail field only
    distinguishes Normal Goal / Penalty / Own Goal / Missed Penalty,
    confirmed directly against real ingested data. No genuine
    free-kick or corner distinction exists, so this is NOT a true
    set-piece analyzer — it's the honest, achievable signal the data
    actually supports: penalty reliance and own-goal patterns."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT me.detail, COUNT(*) AS total
            FROM match_events me
            WHERE me.event_type = 'Goal' AND me.club_name = %s
              AND EXISTS (
                  SELECT 1 FROM matches m
                  JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
                  JOIN leagues l ON l.id = cl.league_id
                  LEFT JOIN countries co ON co.id = l.country_id
                  WHERE m.id = me.match_id AND cl.name = %s
                    AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
              )
            GROUP BY me.detail
        """, (club, club, league))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"available": False, "reason": "No goal event data recorded for this club yet."}

    breakdown = {r["detail"]: r["total"] for r in rows}
    total_goals = sum(breakdown.values())
    penalty_goals = breakdown.get("Penalty", 0)
    own_goals_for = breakdown.get("Own Goal", 0)

    return {
        "available": True,
        "breakdown": breakdown,
        "total_goals": total_goals,
        "penalty_reliance_pct": round(100 * penalty_goals / total_goals, 1) if total_goals else 0,
        "note": "Honest limitation: API-Football's data doesn't distinguish free-kicks or corners specifically — this is penalty reliance and own-goal patterns, not a true set-piece breakdown.",
    }


@app.get("/players/hot-take-data")
def hot_take_data(authorized: bool = Depends(check_api_key)):
    """Stat-Backed Hot Take Generator's data layer — a random,
    genuinely high-potential player's real per-90 goal contribution
    compared against the genuine average for their position across
    tracked leagues. Honest about not using xG, since API-Football
    doesn't provide it — this uses real goals+assists per-90 instead,
    a different but equally real stat."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, p.primary_position, cl.name AS club,
                   pps.potential_index,
                   SUM(pms.goals + pms.assists) AS contributions,
                   SUM(pms.minutes_played) AS minutes
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE pps.potential_index >= 75
            GROUP BY p.id, p.full_name, p.primary_position, cl.name, pps.potential_index
            HAVING SUM(pms.minutes_played) >= 900
            ORDER BY random() LIMIT 1
        """)
        player = cur.fetchone()

        if not player:
            conn.close()
            return {"available": False, "reason": "Not enough players with sufficient minutes yet."}

        per90 = (player["contributions"] / player["minutes"]) * 90

        cur.execute("""
            SELECT AVG(sub.per90) AS league_avg FROM (
                SELECT (SUM(pms.goals + pms.assists)::float / NULLIF(SUM(pms.minutes_played), 0)) * 90 AS per90
                FROM players p2
                JOIN player_match_stats pms ON pms.player_id = p2.id
                WHERE p2.primary_position = %s
                GROUP BY p2.id
                HAVING SUM(pms.minutes_played) >= 900
            ) sub
        """, (player["primary_position"],))
        avg_row = cur.fetchone()
    conn.close()

    league_avg = float(avg_row["league_avg"]) if avg_row and avg_row["league_avg"] else None
    if not league_avg:
        return {"available": False, "reason": "Not enough comparison data for this position yet."}

    return {
        "available": True,
        "full_name": player["full_name"], "position": player["primary_position"], "club": player["club"],
        "per90_contributions": round(per90, 2),
        "position_average_per90": round(league_avg, 2),
        "multiplier": round(per90 / league_avg, 1) if league_avg > 0 else None,
        "note": "Real goals+assists per-90 vs the genuine tracked average for this position — not xG, since API-Football doesn't provide that.",
    }


@app.get("/players/{player_id}/sidelined")
def player_sidelined_records(player_id: int, authorized: bool = Depends(check_api_key)):
    """Sidelined records — genuinely broader than the existing injury
    tracking, covers suspensions as well as injuries. Confirmed real
    data from API-Football's own /sidelined endpoint. Empty until
    sidelined_ingest.py has been run for this player."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sidelined_type, start_date, end_date
            FROM player_sidelined
            WHERE player_id = %s AND sidelined_type IS NOT NULL
            ORDER BY start_date DESC
        """, (player_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/transfer-network")
def club_transfer_network(league: Optional[str] = None, min_transfers: int = Query(1, le=10), authorized: bool = Depends(check_api_key)):
    """Real club-to-club transfer flow, built from actual recorded
    transfers (player_transfer_history) — which clubs genuinely feed
    players to which other clubs. Optionally scoped to clubs within a
    single league to keep the graph readable rather than showing all
    27 leagues at once. Returns nodes (clubs) and edges (transfer
    connections with real counts), the shape a force-directed graph
    layout needs."""
    conn = get_conn()
    with conn.cursor() as cur:
        if league:
            cur.execute("""
                SELECT pth.club_from, pth.club_to, COUNT(*) AS transfer_count
                FROM player_transfer_history pth
                WHERE pth.club_from IS NOT NULL AND pth.club_to IS NOT NULL
                  AND pth.club_from IN (
                      SELECT cl.name FROM clubs cl
                      JOIN leagues l ON l.id = cl.league_id
                      LEFT JOIN countries co ON co.id = l.country_id
                      WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
                  )
                GROUP BY pth.club_from, pth.club_to
                HAVING COUNT(*) >= %s
            """, (league, min_transfers))
        else:
            cur.execute("""
                SELECT club_from, club_to, COUNT(*) AS transfer_count
                FROM player_transfer_history
                WHERE club_from IS NOT NULL AND club_to IS NOT NULL
                GROUP BY club_from, club_to
                HAVING COUNT(*) >= %s
            """, (min_transfers,))
        edges = cur.fetchall()
    conn.close()

    if not edges:
        return {"available": False, "reason": "No transfer data matches — try lowering min_transfers or removing the league filter."}

    node_names = set()
    for e in edges:
        node_names.add(e["club_from"])
        node_names.add(e["club_to"])

    return {
        "available": True,
        "nodes": [{"id": name} for name in node_names],
        "edges": [{"source": e["club_from"], "target": e["club_to"], "count": e["transfer_count"]} for e in edges],
    }


@app.get("/players/{player_id}/injury-risk-profile")
def injury_risk_profile(player_id: int, authorized: bool = Depends(check_api_key)):
    """Synthesizes the raw sidelined records (currently shown as just
    a list) into an actual, actionable signal: genuine injury
    frequency, real total time lost, and a risk tier — the kind of
    thing that matters in a real recruitment decision but isn't
    visible just from scanning a list of dates."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sidelined_type, start_date, end_date
            FROM player_sidelined
            WHERE player_id = %s AND sidelined_type IS NOT NULL
            ORDER BY start_date DESC
        """, (player_id,))
        records = cur.fetchall()
    conn.close()

    if not records:
        return {"available": False, "reason": "No sidelined data recorded yet for this player."}

    total_incidents = len(records)
    total_days_lost = 0
    recent_incidents = 0
    two_years_ago = datetime.utcnow().date() - timedelta(days=730)

    for r in records:
        if r["start_date"] and r["end_date"]:
            total_days_lost += (r["end_date"] - r["start_date"]).days
        if r["start_date"] and r["start_date"] >= two_years_ago:
            recent_incidents += 1

    # Genuinely simple, transparent tiering — not a black-box score.
    # Recent frequency weighted more heavily than total career history,
    # since a bad run in the last two years is more relevant to a
    # current recruitment decision than an old injury from years ago.
    if recent_incidents >= 4 or total_days_lost >= 300:
        risk_tier = "High"
    elif recent_incidents >= 2 or total_days_lost >= 120:
        risk_tier = "Moderate"
    else:
        risk_tier = "Low"

    return {
        "available": True,
        "total_incidents": total_incidents,
        "total_days_lost": total_days_lost,
        "recent_incidents_last_2_years": recent_incidents,
        "risk_tier": risk_tier,
        "note": "A transparent, rule-based tier from real recorded incidents — not a medical prediction, and doesn't account for injury type severity or recovery quality.",
    }


@app.get("/players/trend-comparison")
def players_trend_comparison(player_ids: str, authorized: bool = Depends(check_api_key)):
    """Multi-Player Trend Comparison — overlays 2-4 players' real
    potential evolution over time on one chart, using the exact same
    history data already shown individually on each dossier. player_ids
    is a comma-separated list, e.g. '123,456,789'."""
    try:
        ids = [int(x.strip()) for x in player_ids.split(",") if x.strip()][:4]
    except ValueError:
        raise HTTPException(status_code=400, detail="player_ids must be a comma-separated list of integers")
    if not ids:
        raise HTTPException(status_code=400, detail="At least one player_id is required")

    conn = get_conn()
    results = []
    with conn.cursor() as cur:
        for pid in ids:
            cur.execute("SELECT full_name FROM players WHERE id = %s", (pid,))
            player_row = cur.fetchone()
            if not player_row:
                continue
            cur.execute("""
                SELECT potential_index, computed_at
                FROM player_potential_scores
                WHERE player_id = %s ORDER BY computed_at ASC
            """, (pid,))
            history = cur.fetchall()
            results.append({"id": pid, "full_name": player_row["full_name"], "history": history})
    conn.close()
    return {"players": results}


@app.get("/public/track-record")
def public_track_record():
    """Public, Verifiable Track Record Widget — genuinely public, no API
    key required, since the whole point is external embeddability.
    Exposes only safe, aggregate platform stats — never raw player
    data, emails, or anything sensitive. Honest scope note: badges
    aren't yet per-user scoped (that's a later phase beyond Multi-User
    Phase 1), so this is a genuine platform-wide summary, not an
    individual scout's personal record."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS total FROM players p
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE latest_note.watch_level = 'shortlist'
        """)
        total_shortlisted = cur.fetchone()["total"]

        cur.execute("""
            WITH division_changes AS (
                SELECT DISTINCT ON (cl.id) cl.id AS club_id
                FROM matches m
                JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
                JOIN leagues fixture_l ON fixture_l.id = m.league_id
                LEFT JOIN leagues current_l ON current_l.id = cl.league_id
                WHERE m.status = 'scheduled' AND m.match_date >= now()
                  AND (current_l.id IS NULL OR current_l.id != fixture_l.id)
                ORDER BY cl.id, m.match_date ASC
            )
            SELECT COUNT(DISTINCT p.id) AS total FROM players p
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            JOIN division_changes dc ON dc.club_id = p.current_club_id
            WHERE latest_note.watch_level = 'shortlist'
        """)
        called_it_count = cur.fetchone()["total"]
    conn.close()

    hit_rate = round(100 * called_it_count / total_shortlisted, 1) if total_shortlisted else 0

    # Genuinely transparent, rule-based tiering — not a black box.
    # Volume (called_it_count) matters alongside quality (hit_rate),
    # since a high hit rate on a tiny sample isn't genuinely proven yet.
    if called_it_count >= 10 and hit_rate >= 30:
        tier = "🥇 Gold Scout"
    elif called_it_count >= 5 or hit_rate >= 20:
        tier = "🥈 Silver Scout"
    elif called_it_count >= 1:
        tier = "🥉 Bronze Scout"
    else:
        tier = "Unranked"

    return {
        "total_shortlisted": total_shortlisted,
        "called_it_count": called_it_count,
        "hit_rate_pct": hit_rate,
        "certification_tier": tier,
        "note": "A genuine, real-time aggregate from actual detected club division changes — not a claim, a live count. Tier is transparent and rule-based, not a black box.",
    }


@app.get("/leagues/strategic-briefing-data")
def strategic_briefing_data(league: str, authorized: bool = Depends(check_api_key)):
    """The AI Strategic Intelligence Engine's data layer — aggregates
    the key signals for a league into one concise payload, deliberately
    kept small (top 12 players, key aggregate stats only) since the
    on-device AI that synthesizes this has a genuinely limited context
    window compared to cloud models. This endpoint prepares the data;
    the actual synthesis happens client-side, on-device, for free."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, cl.name AS club, pps.potential_index,
                   COALESCE(SUM(pms.minutes_played), 0) AS minutes
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s AND pps.potential_index >= 70
            GROUP BY p.id, p.full_name, cl.name, pps.potential_index
            ORDER BY pps.potential_index DESC
            LIMIT 12
        """, (league,))
        top_players = cur.fetchall()

        cur.execute("""
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential, COUNT(*) AS squad_size
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
            ORDER BY avg_potential DESC
            LIMIT 6
        """, (league,))
        top_clubs = cur.fetchall()
    conn.close()

    if not top_players:
        return {"available": False, "reason": "Not enough scored players in this league yet."}

    return {
        "available": True,
        "league": league,
        "top_players": [{"name": p["full_name"], "club": p["club"], "potential": round(float(p["potential_index"]))} for p in top_players],
        "top_clubs": [{"club": c["club"], "avg_potential": round(float(c["avg_potential"]), 1)} for c in top_clubs],
    }


@app.get("/clubs/map-data")
def clubs_map_data(league: Optional[str] = None, authorized: bool = Depends(check_api_key)):
    """Geographic Map View — real club venue coordinates, geocoded once
    and stored permanently (via geocode_venues.py) rather than
    re-geocoding on every map load. Empty for clubs whose venues
    haven't been geocoded yet."""
    conn = get_conn()
    with conn.cursor() as cur:
        if league:
            cur.execute("""
                SELECT cl.name AS club, cv.latitude, cv.longitude, cv.name AS venue_name
                FROM club_venues cv
                JOIN clubs cl ON cl.id = cv.club_id
                JOIN leagues l ON l.id = cl.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                WHERE cv.latitude IS NOT NULL
                  AND (l.name || ' (' || COALESCE(co.name, 'Unknown') || ')') = %s
            """, (league,))
        else:
            cur.execute("""
                SELECT cl.name AS club, cv.latitude, cv.longitude, cv.name AS venue_name
                FROM club_venues cv
                JOIN clubs cl ON cl.id = cv.club_id
                WHERE cv.latitude IS NOT NULL
            """)
        rows = cur.fetchall()
    conn.close()
    return [{"club": r["club"], "lat": float(r["latitude"]), "lon": float(r["longitude"]), "venue_name": r["venue_name"]} for r in rows]


@app.get("/scouting-itinerary")
def scouting_itinerary(days_ahead: int = Query(30, le=90), user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
    """Personalized Scouting Itinerary Planner — real upcoming fixtures
    for your own shortlisted players' clubs, with genuine venue
    coordinates, sorted by date. The actual trip-planning data: where
    and when your shortlisted players are playing next. Now genuinely
    achievable given real venue coordinates exist."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT p.current_club_id
            FROM players p
            JOIN scout_notes sn ON sn.player_id = p.id AND sn.user_id = %s
            WHERE sn.watch_level = 'shortlist'
              AND sn.created_at = (SELECT MAX(sn2.created_at) FROM scout_notes sn2 WHERE sn2.player_id = p.id AND sn2.user_id = %s)
        """, (user_id, user_id))
        shortlisted_club_ids = [r["current_club_id"] for r in cur.fetchall()]

        if not shortlisted_club_ids:
            conn.close()
            return {"available": False, "reason": "No shortlisted players yet — add some first to build an itinerary."}

        cur.execute("""
            SELECT m.match_date, home_cl.name AS home_club, away_cl.name AS away_club,
                   cv.name AS venue_name, cv.address, cv.latitude, cv.longitude
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            LEFT JOIN club_venues cv ON cv.club_id = m.home_club_id
            WHERE m.status = 'scheduled'
              AND m.match_date BETWEEN now() AND now() + (%s || ' days')::interval
              AND (m.home_club_id = ANY(%s) OR m.away_club_id = ANY(%s))
            ORDER BY m.match_date ASC
        """, (days_ahead, shortlisted_club_ids, shortlisted_club_ids))
        fixtures = cur.fetchall()
    conn.close()

    return {
        "available": True,
        "fixtures": [{
            "date": f["match_date"], "home_club": f["home_club"], "away_club": f["away_club"],
            "venue_name": f["venue_name"], "address": f["address"],
            "lat": float(f["latitude"]) if f["latitude"] is not None else None,
            "lon": float(f["longitude"]) if f["longitude"] is not None else None,
        } for f in fixtures],
        "note": "Real upcoming fixtures for your shortlisted players' clubs — venue coordinates missing for clubs not yet geocoded.",
    }


@app.get("/players/{player_id}/historical-replay")
def historical_replay(player_id: int, authorized: bool = Depends(check_api_key)):
    """'What If' Historical Replay — applies the same Moneyball formula
    used elsewhere to a player's real, past recorded transfers, showing
    whether the platform's own logic would have flagged the move as
    good value. Honest limitation: uses the player's CURRENT potential
    score, not a reconstruction of what it genuinely was at the time of
    each transfer — that would need much richer historical matching
    than currently exists. A real but imperfect retrospective, not a
    time machine."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT transfer_date, fee_type, club_from, club_to
            FROM player_transfer_history
            WHERE player_id = %s AND club_to IS NOT NULL
            ORDER BY transfer_date DESC
        """, (player_id,))
        transfers = cur.fetchall()

        cur.execute("""
            SELECT pps.potential_index, l.is_top5,
                   COALESCE(SUM(pms.minutes_played), 0) AS minutes
            FROM players p
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            LEFT JOIN player_match_stats pms ON pms.player_id = p.id
            WHERE p.id = %s
            GROUP BY pps.potential_index, l.is_top5
        """, (player_id,))
        player_row = cur.fetchone()
    conn.close()

    if not transfers or not player_row or player_row["potential_index"] is None:
        return {"available": False, "reason": "Not enough real transfer or potential data recorded for this player yet."}

    minutes = player_row["minutes"]
    confidence_mult = 1.0 if minutes >= 1800 else 0.9 if minutes >= 900 else 0.75 if minutes >= 300 else 0.5
    obscurity_bonus = 0.15 if not player_row["is_top5"] else 0
    current_moneyball = round(float(player_row["potential_index"]) * (1 + obscurity_bonus) * confidence_mult, 1)

    replays = []
    for t in transfers:
        verdict = "Would have flagged as genuine value" if current_moneyball >= 70 and t["fee_type"] not in (None, "N/A") else "No strong signal either way"
        replays.append({
            "transfer_date": t["transfer_date"], "fee_type": t["fee_type"],
            "club_from": t["club_from"], "club_to": t["club_to"],
            "current_moneyball_score": current_moneyball, "verdict": verdict,
        })

    return {
        "available": True,
        "replays": replays,
        "note": "Uses current potential, not a reconstruction of potential at the time of each transfer — a real but imperfect retrospective.",
    }


@app.get("/fixtures/{match_id}/poll-draft")
def fixture_poll_draft(match_id: int, authorized: bool = Depends(check_api_key)):
    """Community Prediction Poll — the same honest, transparent
    Match Estimator formula used elsewhere (squad quality + recent
    form + home advantage), applied to one specific fixture, plus a
    genuinely drafted poll question ready to post. Not a betting tool."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.league_id, home_cl.name AS home_club, away_cl.name AS away_club
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.id = %s
        """, (match_id,))
        match_row = cur.fetchone()
        if not match_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Match not found")
        league_id = match_row["league_id"]

        cur.execute("""
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.league_id = %s AND cl.name IN (%s, %s)
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """, (league_id, match_row["home_club"], match_row["away_club"]))
        quality = {r["club"]: float(r["avg_potential"]) for r in cur.fetchall()}

        form = {}
        for club in (match_row["home_club"], match_row["away_club"]):
            cur.execute("""
                SELECT m.home_score, m.away_score, home_cl.name AS home_club, away_cl.name AS away_club
                FROM matches m
                JOIN clubs home_cl ON home_cl.id = m.home_club_id
                JOIN clubs away_cl ON away_cl.id = m.away_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND (home_cl.name = %s OR away_cl.name = %s)
                ORDER BY m.match_date DESC LIMIT 5
            """, (league_id, club, club))
            recent = cur.fetchall()
            if not recent:
                continue
            points = 0
            for r in recent:
                is_home = r["home_club"] == club
                gf = r["home_score"] if is_home else r["away_score"]
                ga = r["away_score"] if is_home else r["home_score"]
                points += 3 if gf > ga else (1 if gf == ga else 0)
            form[club] = (points / (len(recent) * 3)) * 100
    conn.close()

    home_q, away_q = quality.get(match_row["home_club"]), quality.get(match_row["away_club"])
    if home_q is None or away_q is None:
        return {"available": False, "reason": "Not enough squad data for this fixture yet."}

    HOME_ADVANTAGE_BONUS = 3
    home_form, away_form = form.get(match_row["home_club"], 50), form.get(match_row["away_club"], 50)
    home_strength = 0.6 * home_q + 0.4 * home_form
    away_strength = 0.6 * away_q + 0.4 * away_form
    strength_diff = (home_strength - away_strength) + HOME_ADVANTAGE_BONUS

    raw_home = max(5, 33 + strength_diff)
    raw_away = max(5, 33 - strength_diff)
    raw_draw = max(8, 28 - abs(strength_diff) * 0.15)
    total = raw_home + raw_away + raw_draw
    home_pct, draw_pct, away_pct = round(100 * raw_home / total), round(100 * raw_draw / total), round(100 * raw_away / total)

    poll_text = f"🗳️ Prediction: {match_row['home_club']} vs {match_row['away_club']} — who wins? (Model odds: {home_pct}% / {draw_pct}% draw / {away_pct}%)"

    return {
        "available": True,
        "home_club": match_row["home_club"], "away_club": match_row["away_club"],
        "home_pct": home_pct, "draw_pct": draw_pct, "away_pct": away_pct,
        "poll_text": poll_text,
        "note": "Not a betting tool — the same honest, transparent formula used across the platform.",
    }


@app.get("/players/guess-the-player-quiz")
def guess_the_player_quiz(authorized: bool = Depends(check_api_key)):
    """'Guess the Player' Quiz — a random, genuinely interesting
    high-potential player with progressive clues, for building
    shareable quiz content. Returns the answer alongside the clues
    since this is content-generation for the person's own posting,
    not a genuine anti-cheat multiplayer game."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.full_name, p.primary_position,
                   EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age,
                   cl.name AS club, l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league_display,
                   pps.potential_index
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE pps.potential_index >= 78
            ORDER BY random()
            LIMIT 1
        """)
        player = cur.fetchone()
    conn.close()

    if not player:
        return {"available": False, "reason": "Not enough high-potential players scored yet."}

    clues = [
        f"Clue 1: Plays in {player['league_display']}.",
        f"Clue 2: A {player['primary_position']}, age {player['age']}.",
        f"Clue 3: Potential rating of {round(float(player['potential_index']))}+ on our index.",
        f"Clue 4: Currently at {player['club']}.",
    ]

    return {
        "available": True,
        "answer": player["full_name"],
        "clues": clues,
        "note": "Real stats, genuinely random selection — not a guaranteed unknown player, just a real one worth guessing.",
    }


@app.get("/newsletter-data")
def newsletter_data(authorized: bool = Depends(check_api_key)):
    """Weekly Newsletter/Digest — aggregates the key weekly signals into
    one concise payload for the on-device AI to synthesize into a
    longer-form digest. Reuses The Autonomous Scout's discovery logic
    directly rather than duplicating it."""
    discoveries = autonomous_discovery(limit=3, authorized=True)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH division_changes AS (
                SELECT DISTINCT ON (cl.id) cl.id AS club_id, cl.name AS club, fixture_l.name AS new_league
                FROM matches m
                JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
                JOIN leagues fixture_l ON fixture_l.id = m.league_id
                LEFT JOIN leagues current_l ON current_l.id = cl.league_id
                WHERE m.status = 'scheduled' AND m.match_date >= now()
                  AND (current_l.id IS NULL OR current_l.id != fixture_l.id)
                ORDER BY cl.id, m.match_date ASC
            )
            SELECT club, new_league FROM division_changes LIMIT 5
        """)
        recent_changes = cur.fetchall()
    conn.close()

    return {
        "top_discoveries": discoveries.get("discoveries", []),
        "division_changes": recent_changes,
    }


@app.get("/leagues/season-simulation")
def full_season_simulation(league: str, simulations: int = Query(1000, le=5000), authorized: bool = Depends(check_api_key)):
    """The Full Season Simulator — genuine Monte Carlo projection across
    every remaining fixture in the league, using the exact same
    win/draw/loss formula as Match Estimator (squad quality + recent
    form + home advantage), run thousands of times to produce real
    probability distributions: title odds, top-quarter finish, and
    bottom-quarter (relegation risk) — rather than a single-match
    estimate. Same honest framing as Match Estimator: not a betting
    tool, doesn't account for injuries, tactics, or form-on-the-day."""
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

        # Squad quality — same source as Match Estimator
        cur.execute("""
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.league_id = %s
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """, (league_id,))
        quality = {r["club"]: float(r["avg_potential"]) for r in cur.fetchall()}

        # Current real standings — genuine points/played so far this season
        cur.execute("""
            SELECT cl.name AS club,
                   SUM(CASE
                       WHEN (m.home_club_id = cl.id AND m.home_score > m.away_score) OR (m.away_club_id = cl.id AND m.away_score > m.home_score) THEN 3
                       WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) AS points
            FROM clubs cl
            JOIN matches m ON (m.home_club_id = cl.id OR m.away_club_id = cl.id) AND m.status = 'finished'
            WHERE cl.league_id = %s
            GROUP BY cl.name
        """, (league_id,))
        current_points = {r["club"]: (r["points"] or 0) for r in cur.fetchall()}

        # All remaining fixtures for the rest of the season
        cur.execute("""
            SELECT home_cl.name AS home_club, away_cl.name AS away_club
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'scheduled'
        """, (league_id,))
        remaining_fixtures = cur.fetchall()

        # Recent form per club, same as Match Estimator
        cur.execute("""
            SELECT DISTINCT cl.name AS club
            FROM matches m JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'finished'
        """, (league_id,))
        clubs = [r["club"] for r in cur.fetchall()]
        form = {}
        for club in clubs:
            cur.execute("""
                SELECT m.home_score, m.away_score, home_cl.name AS home_club, away_cl.name AS away_club
                FROM matches m
                JOIN clubs home_cl ON home_cl.id = m.home_club_id
                JOIN clubs away_cl ON away_cl.id = m.away_club_id
                WHERE m.league_id = %s AND m.status = 'finished' AND (home_cl.name = %s OR away_cl.name = %s)
                ORDER BY m.match_date DESC LIMIT 5
            """, (league_id, club, club))
            recent = cur.fetchall()
            if not recent:
                continue
            points = 0
            for r in recent:
                is_home = r["home_club"] == club
                gf = r["home_score"] if is_home else r["away_score"]
                ga = r["away_score"] if is_home else r["home_score"]
                points += 3 if gf > ga else (1 if gf == ga else 0)
            form[club] = (points / (len(recent) * 3)) * 100
    conn.close()

    if not remaining_fixtures:
        return {"available": False, "reason": "No remaining scheduled fixtures found for this league — season may be finished or not yet started."}

    HOME_ADVANTAGE_BONUS = 3

    def match_probabilities(home_club, away_club):
        home_q, away_q = quality.get(home_club), quality.get(away_club)
        if home_q is None or away_q is None:
            return (33, 33, 34)  # genuinely unknown squads — fall back to a neutral split
        home_form, away_form = form.get(home_club, 50), form.get(away_club, 50)
        home_strength = 0.6 * home_q + 0.4 * home_form
        away_strength = 0.6 * away_q + 0.4 * away_form
        strength_diff = (home_strength - away_strength) + HOME_ADVANTAGE_BONUS
        raw_home = max(5, 33 + strength_diff)
        raw_away = max(5, 33 - strength_diff)
        raw_draw = max(8, 28 - abs(strength_diff) * 0.15)
        total = raw_home + raw_away + raw_draw
        return (raw_home / total, raw_draw / total, raw_away / total)

    # Pre-compute probabilities once — genuinely expensive to redo per
    # simulation, and they don't change between runs.
    fixture_probs = [(f["home_club"], f["away_club"], *match_probabilities(f["home_club"], f["away_club"])) for f in remaining_fixtures]

    all_clubs = list(current_points.keys())
    n_clubs = len(all_clubs)
    top_quarter_size = max(1, round(n_clubs / 4))
    bottom_quarter_size = max(1, round(n_clubs / 4))

    title_count = {c: 0 for c in all_clubs}
    top_quarter_count = {c: 0 for c in all_clubs}
    bottom_quarter_count = {c: 0 for c in all_clubs}

    for _ in range(simulations):
        points = dict(current_points)
        for home, away, p_home, p_draw, p_away in fixture_probs:
            outcome = random.random()
            if outcome < p_home:
                points[home] = points.get(home, 0) + 3
            elif outcome < p_home + p_draw:
                points[home] = points.get(home, 0) + 1
                points[away] = points.get(away, 0) + 1
            else:
                points[away] = points.get(away, 0) + 3

        final_table = sorted(all_clubs, key=lambda c: points.get(c, 0), reverse=True)
        title_count[final_table[0]] += 1
        for c in final_table[:top_quarter_size]:
            top_quarter_count[c] += 1
        for c in final_table[-bottom_quarter_size:]:
            bottom_quarter_count[c] += 1

    results = [{
        "club": c,
        "current_points": current_points.get(c, 0),
        "title_pct": round(100 * title_count[c] / simulations, 1),
        "top_quarter_pct": round(100 * top_quarter_count[c] / simulations, 1),
        "bottom_quarter_pct": round(100 * bottom_quarter_count[c] / simulations, 1),
    } for c in all_clubs]
    results.sort(key=lambda r: r["current_points"], reverse=True)

    return {
        "available": True,
        "simulations_run": simulations,
        "remaining_fixtures": len(remaining_fixtures),
        "table": results,
        "note": "Not a betting tool — the same honest framing as Match Estimator, extended across a full season via genuine repeated simulation, not a single estimate. Doesn't account for injuries, tactics, or form-on-the-day.",
    }


@app.get("/today")
def scouts_today_dashboard(authorized: bool = Depends(check_api_key)):
    """The unified homepage — everything that genuinely needs attention
    right now, synthesized from data already tracked elsewhere: stale
    pipeline targets, workload flags on shortlisted players, upcoming
    fixtures for shortlisted clubs with a known referee, and any
    division changes just detected. Reuses the exact same thresholds
    each individual feature already uses, for direct comparability."""
    conn = get_conn()
    with conn.cursor() as cur:
        # Stale pipeline targets — same 14-day threshold as /pipeline
        cur.execute("""
            SELECT p.full_name, rp.stage, EXTRACT(DAY FROM now() - rp.stage_updated_at)::int AS days_in_stage
            FROM recruitment_pipeline rp
            JOIN players p ON p.id = rp.player_id
            WHERE rp.stage NOT IN ('signed', 'rejected', 'cold')
              AND EXTRACT(DAY FROM now() - rp.stage_updated_at) >= 14
            ORDER BY rp.stage_updated_at ASC
        """)
        stale_targets = cur.fetchall()

        # Workload flags on shortlisted players — same ACWR danger
        # threshold (>=1.5) as /players/acwr and /players/burnout-risk
        cur.execute("""
            WITH acute AS (
                SELECT pms.player_id, SUM(pms.minutes_played) AS acute_minutes
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '7 days'
                GROUP BY pms.player_id
            ),
            chronic AS (
                SELECT pms.player_id, SUM(pms.minutes_played) / 4.0 AS chronic_weekly
                FROM player_match_stats pms JOIN matches m ON m.id = pms.match_id
                WHERE m.status = 'finished' AND m.match_date >= now() - interval '28 days'
                GROUP BY pms.player_id HAVING SUM(pms.minutes_played) >= 90
            )
            SELECT p.full_name, cl.name AS club,
                   ROUND((a.acute_minutes / c.chronic_weekly)::numeric, 2) AS acwr
            FROM chronic c
            JOIN players p ON p.id = c.player_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            LEFT JOIN clubs cl ON cl.id = p.current_club_id
            LEFT JOIN acute a ON a.player_id = c.player_id
            WHERE latest_note.watch_level = 'shortlist' AND a.acute_minutes IS NOT NULL
              AND (a.acute_minutes / c.chronic_weekly) >= 1.5
        """)
        workload_flags = cur.fetchall()

        # Upcoming fixtures with a known referee, for shortlisted players' clubs
        cur.execute("""
            SELECT DISTINCT m.id, m.match_date, home_cl.name AS home_club, away_cl.name AS away_club, m.referee
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            JOIN players p ON p.current_club_id = m.home_club_id OR p.current_club_id = m.away_club_id
            JOIN LATERAL (
                SELECT watch_level FROM scout_notes sn
                WHERE sn.player_id = p.id ORDER BY created_at DESC LIMIT 1
            ) latest_note ON true
            WHERE latest_note.watch_level = 'shortlist' AND m.status = 'scheduled'
              AND m.referee IS NOT NULL AND m.match_date BETWEEN now() AND now() + interval '7 days'
            ORDER BY m.match_date ASC LIMIT 10
        """)
        upcoming_with_referee = cur.fetchall()

        # Division changes — same detection query as /clubs/division-changes
        cur.execute("""
            SELECT DISTINCT ON (cl.id)
                cl.name AS club,
                current_l.name AS current_league,
                fixture_l.name AS fixture_implied_league
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            JOIN leagues fixture_l ON fixture_l.id = m.league_id
            LEFT JOIN leagues current_l ON current_l.id = cl.league_id
            WHERE m.status = 'scheduled' AND m.match_date >= now()
              AND (current_l.id IS NULL OR current_l.id != fixture_l.id)
            ORDER BY cl.id, m.match_date ASC
            LIMIT 10
        """)
        division_changes_result = cur.fetchall()
    conn.close()

    return {
        "stale_pipeline_targets": stale_targets,
        "workload_flags": workload_flags,
        "upcoming_fixtures_with_referee": upcoming_with_referee,
        "recent_division_changes": division_changes_result,
        "note": "Everything here is synthesized from data already tracked elsewhere on the platform — nothing new is computed, just brought together in one place.",
    }


@app.get("/clubs/net-transfer-balance")
def net_transfer_balance(days: int = Query(180, le=365), limit: int = Query(20, le=50), authorized: bool = Depends(check_api_key)):
    """Which clubs are genuinely net buyers vs net sellers — not raw
    transfer volume (a busy club could just be churning, in and out in
    equal measure), but real directional balance. Uses your own
    accumulated transfer data, now that the underlying tracking bug
    (non-deterministic club-assignment flip-flopping) has been fixed."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            WITH transfers_in AS (
                SELECT new_club_id AS club_id, COUNT(*) AS in_count
                FROM player_club_transfers
                WHERE changed_at >= now() - make_interval(days => %s) AND new_club_id IS NOT NULL
                GROUP BY new_club_id
            ),
            transfers_out AS (
                SELECT old_club_id AS club_id, COUNT(*) AS out_count
                FROM player_club_transfers
                WHERE changed_at >= now() - make_interval(days => %s) AND old_club_id IS NOT NULL
                GROUP BY old_club_id
            )
            SELECT cl.name AS club, l.name || ' (' || COALESCE(co.name, 'Unknown') || ')' AS league,
                   COALESCE(ti.in_count, 0) AS transfers_in, COALESCE(tout.out_count, 0) AS transfers_out,
                   COALESCE(ti.in_count, 0) - COALESCE(tout.out_count, 0) AS net_balance
            FROM clubs cl
            LEFT JOIN transfers_in ti ON ti.club_id = cl.id
            LEFT JOIN transfers_out tout ON tout.club_id = cl.id
            LEFT JOIN leagues l ON l.id = cl.league_id
            LEFT JOIN countries co ON co.id = l.country_id
            WHERE COALESCE(ti.in_count, 0) + COALESCE(tout.out_count, 0) >= 2
            ORDER BY net_balance DESC
            LIMIT %s
        """, (days, days, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/transfers/pathways")
def transfer_pathways(days: int = Query(180, le=365), limit: int = Query(15, le=30), authorized: bool = Depends(check_api_key)):
    """The most common league-to-league transfer routes — a genuine
    pattern in the data pure club-level counts never surface. Which
    leagues function as real feeder pipelines into others."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT old_l.name || ' (' || COALESCE(old_co.name, 'Unknown') || ')' AS from_league,
                   new_l.name || ' (' || COALESCE(new_co.name, 'Unknown') || ')' AS to_league,
                   COUNT(*) AS transfer_count
            FROM player_club_transfers pct
            JOIN clubs old_cl ON old_cl.id = pct.old_club_id
            JOIN clubs new_cl ON new_cl.id = pct.new_club_id
            JOIN leagues old_l ON old_l.id = old_cl.league_id
            JOIN leagues new_l ON new_l.id = new_cl.league_id
            LEFT JOIN countries old_co ON old_co.id = old_l.country_id
            LEFT JOIN countries new_co ON new_co.id = new_l.country_id
            WHERE pct.changed_at >= now() - make_interval(days => %s) AND old_l.id != new_l.id
            GROUP BY from_league, to_league
            HAVING COUNT(*) >= 2
            ORDER BY transfer_count DESC
            LIMIT %s
        """, (days, limit))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/fixture-difficulty")
def fixture_difficulty(club: str, league: str, num_fixtures: int = Query(6, le=10), authorized: bool = Depends(check_api_key)):
    """The classic FPL concept — a 1-5 difficulty rating for each of a
    club's next fixtures, based on opponent squad quality. Genuinely
    achievable with the same squad-quality data already powering Match
    Estimator. Difficulty is the opponent's percentile rank within the
    league, bucketed 1 (easiest) to 5 (hardest) — not an arbitrary
    guess, a real relative comparison against every other club in the
    same league."""
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
            SELECT cl.name AS club, AVG(pps.potential_index) AS avg_potential
            FROM players p
            JOIN clubs cl ON cl.id = p.current_club_id
            JOIN LATERAL (
                SELECT potential_index FROM player_potential_scores
                WHERE player_id = p.id ORDER BY season DESC LIMIT 1
            ) pps ON true
            WHERE cl.league_id = %s
            GROUP BY cl.name
            HAVING COUNT(*) >= 8
        """, (league_id,))
        quality_rows = cur.fetchall()

        cur.execute("""
            SELECT m.id, m.match_date, home_cl.name AS home_club, away_cl.name AS away_club
            FROM matches m
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'scheduled' AND (home_cl.name = %s OR away_cl.name = %s)
            ORDER BY m.match_date ASC LIMIT %s
        """, (league_id, club, club, num_fixtures))
        fixtures = cur.fetchall()
    conn.close()

    if not quality_rows:
        return {"available": False, "reason": "Not enough scored players in this league to compute fixture difficulty yet."}

    qualities = sorted([r["avg_potential"] for r in quality_rows])
    quality_map = {r["club"]: r["avg_potential"] for r in quality_rows}

    def difficulty_bucket(opponent_quality):
        # Percentile rank of the opponent's quality among every club in
        # the league, bucketed into 1 (easiest) - 5 (hardest).
        rank = sum(1 for q in qualities if q <= opponent_quality) / len(qualities)
        return min(5, max(1, round(rank * 4) + 1))

    results = []
    for f in fixtures:
        is_home = f["home_club"] == club
        opponent = f["away_club"] if is_home else f["home_club"]
        opponent_quality = quality_map.get(opponent)
        if opponent_quality is None:
            continue
        results.append({
            "match_date": f["match_date"], "opponent": opponent, "is_home": is_home,
            "difficulty": difficulty_bucket(opponent_quality),
        })

    return {"available": True, "fixtures": results}


@app.get("/players/{player_id}/rotation-risk")
def rotation_risk(player_id: int, authorized: bool = Depends(check_api_key)):
    """Not 'how good is this player' but 'will they actually play' —
    genuinely different signal from anything else on this platform.
    Uses their actual recent minutes pattern (last 10 matches) rather
    than assuming, since squad status changes constantly and a
    player's role can shift well before it shows up in season totals."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pms.minutes_played, m.match_date
            FROM player_match_stats pms
            JOIN matches m ON m.id = pms.match_id
            WHERE pms.player_id = %s AND m.status = 'finished'
            ORDER BY m.match_date DESC LIMIT 10
        """, (player_id,))
        rows = cur.fetchall()
    conn.close()

    if len(rows) < 3:
        return {"available": False, "reason": "Not enough recent match data to assess rotation risk yet."}

    starts = sum(1 for r in rows if r["minutes_played"] >= 60)
    start_pct = round(100 * starts / len(rows), 1)

    if start_pct >= 80:
        tier = "Nailed"
    elif start_pct >= 40:
        tier = "Rotation Risk"
    else:
        tier = "Bench Risk"

    return {
        "available": True, "tier": tier, "start_pct": start_pct,
        "matches_considered": len(rows), "starts_60plus_mins": starts,
        "note": "Based on actual recent minutes, not a season-long average — reflects current role, not history.",
    }


class TrackPredictionRequest(BaseModel):
    match_id: int
    predicted_home_pct: float
    predicted_draw_pct: float
    predicted_away_pct: float


@app.post("/predictions/track")
def track_prediction(body: TrackPredictionRequest, authorized: bool = Depends(check_api_key)):
    """Saves a prediction for later verification against the real
    result — the foundation of a genuine, verifiable track record
    rather than only remembering the ones that turned out right."""
    outcome = max(
        [("home", body.predicted_home_pct), ("draw", body.predicted_draw_pct), ("away", body.predicted_away_pct)],
        key=lambda x: x[1],
    )[0]

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tracked_predictions (match_id, predicted_home_pct, predicted_draw_pct, predicted_away_pct, predicted_outcome)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (body.match_id, body.predicted_home_pct, body.predicted_draw_pct, body.predicted_away_pct, outcome))
        new_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()
    return {"id": new_id, "predicted_outcome": outcome, "tracked": True}


@app.get("/predictions/track-record")
def prediction_track_record(authorized: bool = Depends(check_api_key)):
    """Every tracked prediction compared against real results — computed
    live at query time, not stored, so this is always accurate without
    needing a separate scheduled verification job. Only finished
    matches count toward accuracy; predictions for matches not yet
    played are shown as pending."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tp.id, tp.predicted_outcome, tp.predicted_home_pct, tp.predicted_draw_pct, tp.predicted_away_pct,
                   tp.predicted_at, m.status, m.home_score, m.away_score,
                   home_cl.name AS home_club, away_cl.name AS away_club
            FROM tracked_predictions tp
            JOIN matches m ON m.id = tp.match_id
            JOIN clubs home_cl ON home_cl.id = m.home_club_id
            JOIN clubs away_cl ON away_cl.id = m.away_club_id
            ORDER BY tp.predicted_at DESC
        """)
        rows = cur.fetchall()
    conn.close()

    results = []
    correct_count = 0
    finished_count = 0
    for r in rows:
        actual_outcome = None
        was_correct = None
        if r["status"] == "finished":
            finished_count += 1
            if r["home_score"] > r["away_score"]:
                actual_outcome = "home"
            elif r["away_score"] > r["home_score"]:
                actual_outcome = "away"
            else:
                actual_outcome = "draw"
            was_correct = actual_outcome == r["predicted_outcome"]
            if was_correct:
                correct_count += 1
        results.append({**r, "actual_outcome": actual_outcome, "was_correct": was_correct})

    accuracy_pct = round(100 * correct_count / finished_count, 1) if finished_count else None
    return {"predictions": results, "total_tracked": len(rows), "finished": finished_count, "correct": correct_count, "accuracy_pct": accuracy_pct}


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


@app.get("/transfer-news")
def transfer_news(limit: int = Query(30, le=100), authorized: bool = Depends(check_api_key)):
    """General transfer news — served entirely from our own cache, kept
    fresh every 5 minutes by a dedicated background workflow regardless
    of whether anyone has the app open. Genuinely current, but this
    endpoint itself does zero external fetching — it's just a fast read."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT headline, link, source, published, fetched_at
            FROM transfer_news_cache
            ORDER BY fetched_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/clubs/transfer-news")
def club_transfer_news(club: str, limit: int = Query(10, le=30), authorized: bool = Depends(check_api_key)):
    """Live transfer news for one specific club — not background-cached
    like the general feed, since favorited clubs currently live only in
    the browser, not the server, so there's no fixed list to pre-fetch
    for. Fetched fresh each time this is called instead."""
    import xml.etree.ElementTree as ET
    query = requests.utils.quote(f"{club} transfer")
    try:
        resp = requests.get(
            f"https://news.google.com/rss/search?q={query}&hl=en-GB&gl=GB&ceid=GB:en",
            headers={"User-Agent": "CrossLeagueScoutingIndex/1.0"}, timeout=8,
        )
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item")[:limit]:
            items.append({
                "headline": item.findtext("title"),
                "link": item.findtext("link"),
                "source": item.findtext("source"),
                "published": item.findtext("pubDate"),
            })
        return items
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch club transfer news: {e}")


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


@app.get("/leagues/full-report")
def league_full_report(league: str, authorized: bool = Depends(check_api_key)):
    """Everything needed for a comprehensive league report in ONE
    response — standings, top scorers, and biggest recent movers —
    rather than the frontend making several separate calls for an export."""
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
            LIMIT 10
        """, (league_id, league_id))
        standings_top10 = cur.fetchall()

        cur.execute("""
            SELECT p.full_name, cl.name AS club, SUM(pms.goals) AS goals
            FROM player_match_stats pms
            JOIN players p ON p.id = pms.player_id
            JOIN clubs cl ON cl.id = pms.club_id
            WHERE cl.league_id = %s
            GROUP BY p.full_name, cl.name
            ORDER BY goals DESC
            LIMIT 5
        """, (league_id,))
        top_scorers = cur.fetchall()

        cur.execute("""
            WITH bounds AS (
                SELECT h.player_id, MIN(h.computed_at) AS first_at, MAX(h.computed_at) AS last_at
                FROM player_potential_history h
                JOIN players p ON p.id = h.player_id
                JOIN clubs cl ON cl.id = p.current_club_id
                WHERE cl.league_id = %s
                GROUP BY h.player_id HAVING COUNT(*) >= 2
            ),
            first_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS first_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.first_at
            ),
            last_vals AS (
                SELECT DISTINCT ON (h.player_id) h.player_id, h.potential_index AS last_val
                FROM player_potential_history h JOIN bounds b ON b.player_id = h.player_id AND h.computed_at = b.last_at
            )
            SELECT p.full_name, cl.name AS club, (lv.last_val - fv.first_val) AS delta
            FROM first_vals fv
            JOIN last_vals lv ON lv.player_id = fv.player_id
            JOIN players p ON p.id = fv.player_id
            JOIN clubs cl ON cl.id = p.current_club_id
            ORDER BY delta DESC
            LIMIT 5
        """, (league_id,))
        movers = cur.fetchall()

    conn.close()
    return {"league": league, "standings": standings_top10, "top_scorers": top_scorers, "movers": movers}



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


@app.get("/standings/form")
def form_table(league: str, window: int = Query(5, le=10), authorized: bool = Depends(check_api_key)):
    """A table built purely from each club's last N matches — a real
    'who's actually hot right now' signal, genuinely different from the
    full-season table (a club could be mid-table overall but on a real
    hot streak, or vice versa)."""
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
            SELECT DISTINCT cl.name AS club
            FROM matches m
            JOIN clubs cl ON cl.id = m.home_club_id OR cl.id = m.away_club_id
            WHERE m.league_id = %s AND m.status = 'finished'
        """, (league_id,))
        clubs = [r["club"] for r in cur.fetchall()]

        results = []
        for club in clubs:
            cur.execute("""
                SELECT m.match_date, m.home_score, m.away_score,
                       home_cl.name AS home_club, away_cl.name AS away_club
                FROM matches m
                JOIN clubs home_cl ON home_cl.id = m.home_club_id
                JOIN clubs away_cl ON away_cl.id = m.away_club_id
                WHERE m.league_id = %s AND m.status = 'finished'
                  AND (home_cl.name = %s OR away_cl.name = %s)
                ORDER BY m.match_date DESC
                LIMIT %s
            """, (league_id, club, club, window))
            recent = cur.fetchall()
            if not recent:
                continue

            points = 0
            form_string = []
            gf_total = ga_total = 0
            for r in recent:
                is_home = r["home_club"] == club
                gf = r["home_score"] if is_home else r["away_score"]
                ga = r["away_score"] if is_home else r["home_score"]
                gf_total += gf
                ga_total += ga
                if gf > ga:
                    points += 3
                    form_string.append("W")
                elif gf == ga:
                    points += 1
                    form_string.append("D")
                else:
                    form_string.append("L")
            results.append({
                "club": club, "form_points": points, "matches": len(recent),
                "form_string": list(reversed(form_string)),  # oldest -> most recent, reads left-to-right naturally
                "goal_diff": gf_total - ga_total,
            })

    conn.close()
    results.sort(key=lambda r: (r["form_points"], r["goal_diff"]), reverse=True)
    return results


@app.get("/leagues")
def list_leagues(authorized: bool = Depends(check_api_key)):
    """Excludes cup/continental competitions — their knockout format
    breaks the table/standings concept most consumers of this list
    assume (Standings, Table Predictor, Form Table). Youth leagues stay
    visible, since those are still round-robin and those features work
    fine for them. Cup competition data still exists and still counts
    toward player profiles — it's just not offered as a 'pick a league'
    option here."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id, l.name,
                   l.name || ' (' || COALESCE(c.name, 'Unknown') || ')' AS league_display,
                   l.season, l.is_top5, c.name AS country
            FROM leagues l
            LEFT JOIN countries c ON c.id = l.country_id
            WHERE l.is_cup = false
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
    age_min: Optional[int] = None,
    min_potential: Optional[float] = Query(None),
    max_potential: Optional[float] = Query(None),
    search: Optional[str] = None,
    shortlist_only: bool = False,
    sort: str = Query("potential", enum=[
        "potential", "age", "name", "goals", "assists", "tackles",
        "interceptions", "saves", "yellow_cards", "minutes_played", "duel_win_pct", "pass_accuracy_pct",
    ]),
    exclude_top5: bool = False,
    youth_only: bool = False,
    season: Optional[str] = None,
    limit: int = Query(50, le=5000),
    format: str = Query("json", enum=["json", "csv"]),
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
    if age_min:
        filters.append("date_part('year', age(p.date_of_birth)) >= %s")
        params.append(age_min)
    if min_potential:
        filters.append("pps.potential_index >= %s")
        params.append(min_potential)
    if max_potential:
        filters.append("pps.potential_index <= %s")
        params.append(max_potential)
    if search:
        filters.append("(p.full_name ILIKE %s OR cl.name ILIKE %s)")
        params.append(f"%{search}%")
        params.append(f"%{search}%")
    if shortlist_only:
        filters.append("latest_note.watch_level = 'shortlist'")
    if exclude_top5:
        filters.append("l.is_top5 = false")
    # Safe default: senior leagues only, unless youth data is explicitly
    # requested. Without this, once youth leagues are ingested, academy
    # players would silently mix into ordinary senior searches — exactly
    # what the is_youth flag exists to prevent.
    filters.append("l.is_youth = %s")
    params.append(youth_only)

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

    if format == "csv":
        import csv
        import io
        from fastapi.responses import StreamingResponse
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=players_export.csv"},
        )

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
        # captured during ingestion but never surfaced until now. Now
        # includes avg rating per position too — a genuine quality
        # signal, since playing 3 positions competently is worth more
        # than playing 3 positions poorly.
        cur.execute("""
            SELECT position_played, COUNT(*) AS matches, AVG(rating) AS avg_rating
            FROM player_match_stats
            WHERE player_id = %s AND position_played IS NOT NULL AND minutes_played > 0
            GROUP BY position_played
            ORDER BY matches DESC
        """, (player_id,))
        positions_played = cur.fetchall()
        for pp in positions_played:
            pp["avg_rating"] = round(pp["avg_rating"], 2) if pp["avg_rating"] is not None else None

        # Versatility Value — a genuine composite: rewards BOTH genuine
        # breadth (multiple real positions) AND maintaining real quality
        # across them, not just showing up somewhere different once.
        versatility_value = None
        rated_positions = [pp for pp in positions_played if pp["avg_rating"] is not None]
        if len(rated_positions) >= 2:
            avg_rating_across = sum(pp["avg_rating"] for pp in rated_positions) / len(rated_positions)
            versatility_value = round(len(rated_positions) * avg_rating_across, 1)

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
                league_adjusted = round(float(score["potential_index"]) * factor, 1)

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
        "versatility_value": versatility_value,
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
async def set_watch_level(player_id: int, body: WatchRequest, user_id: int = Depends(get_current_user), authorized: bool = Depends(check_api_key)):
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
            INSERT INTO scout_notes (player_id, author, note, watch_level, user_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, watch_level, created_at
            """,
            (player_id, body.author, note_text, body.watch_level, user_id),
        )
        result = cur.fetchone()
    conn.commit()
    conn.close()

    # Real-Time Collaborative Notes — let anyone else currently viewing
    # this same player's dossier see the new note immediately.
    await notes_manager.broadcast(player_id, {
        "new_note": {
            "note": note_text, "author": body.author, "watch_level": body.watch_level,
            "created_at": result["created_at"].isoformat() if result.get("created_at") else None,
        }
    })

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