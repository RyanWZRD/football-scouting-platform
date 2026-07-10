"""
Ingestion pipeline: API-Football -> Postgres (schema.sql)

Pulls players PER CLUB, not per league. This matters: /players?league=X
returns every player API-Football has ever tracked for that league —
years of history, reserves, old loanees — which took one league over 3
HOURS to paginate through on Pro tier. /teams?league=X gives the authoritative
current-season club list, and /players?team=X&season=Y for each club returns
just that club's actual current squad (naturally 1-2 pages, no historical
bloat) — reliably complete, not dependent on guessing pagination order.

Usage:
    export FOOTBALL_API_KEY=...
    export DATABASE_URL=postgresql://user:pass@host/dbname
    python ingest.py --league 88 --season 2025          # Eredivisie example
    python ingest.py --all-leagues --season 2025         # every tracked league

Requires: pip install requests psycopg2-binary python-dotenv
"""

import os
import time
import argparse
import requests
import psycopg2

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.environ.get("FOOTBALL_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

HEADERS = {"x-apisports-key": API_KEY}

# Pro plan: 7,500/day, 300 requests/minute. 0.25s between calls keeps us
# comfortably under that (~4 req/sec = 240/min) with headroom to spare.
REQUEST_DELAY_SECONDS = 0.25


def fix_mojibake(s):
    """Repairs a specific, common source-side corruption in API-Football's
    'name' field: some player names are stored there as UTF-8 bytes that
    were mistakenly decoded as Latin-1 and re-encoded (e.g. "OulaÃ¯" instead
    of "Oulaï"). Confirmed via a raw API check — their own 'lastname' field
    has the SAME name correctly encoded, proving this is corrupted at their
    source, not something we're introducing by decoding their response
    wrong. Since this exact corruption is a reversible round-trip, we can
    recover the real characters: re-encode as Latin-1 to get back the
    original UTF-8 bytes, then decode those correctly as UTF-8.
    Only applies the fix if it succeeds cleanly (a genuinely correct
    string won't usually round-trip this way, so this is safe)."""
    if not s:
        return s
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return s


def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS, params=params or {})
    if resp.status_code == 429:
        raise RateLimitError("Daily or per-minute request limit hit (HTTP 429).")
    resp.raise_for_status()
    # Force UTF-8 explicitly rather than trusting requests' auto-detected
    # encoding — API-Football's responses don't declare a charset, and
    # requests' fallback guessing can misidentify genuinely UTF-8 content
    # (e.g. accented names) as a different encoding, corrupting characters
    # like "ï" into "Ã¯" (classic double-encoding/mojibake).
    resp.encoding = "utf-8"
    body = resp.json()
    errors = body.get("errors")
    if errors:
        error_text = str(errors).lower()
        if "page parameter" in error_text:
            raise PageLimitReached()
        if isinstance(errors, dict) and any("request" in k.lower() for k in errors.keys()):
            raise RateLimitError(f"Rate limit reported in response body: {errors}")
        print(f"    API returned errors (non-fatal, treating as no data): {errors}")
    time.sleep(REQUEST_DELAY_SECONDS)
    return body.get("response", [])


class RateLimitError(Exception):
    pass


class PageLimitReached(Exception):
    pass


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def upsert_league(conn, league_id, season):
    data = api_get("leagues", {"id": league_id, "season": season})
    if not data:
        return None
    entry = data[0]
    league = entry["league"]
    country = entry["country"]
    is_top5 = league["name"] in {
        "Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO countries (name, fifa_code) VALUES (%s, %s)
            ON CONFLICT DO NOTHING RETURNING id
            """,
            (country["name"], country.get("code")),
        )
        cur.execute("SELECT id FROM countries WHERE name = %s", (country["name"],))
        country_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO leagues (external_id, name, country_id, tier, is_top5, season)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE SET season = EXCLUDED.season
            RETURNING id
            """,
            (str(league["id"]), fix_mojibake(league["name"]), country_id, 1, is_top5, str(season)),
        )
        conn.commit()
        return cur.fetchone()[0]


def fetch_current_clubs(league_external_id, season):
    """The authoritative list of clubs actually in this league for this
    season — via /teams, not incidentally discovered through player data."""
    return api_get("teams", {"league": league_external_id, "season": season})


def upsert_club(conn, team, db_league_id):
    if not team or not team.get("id"):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO clubs (external_id, name, league_id, logo_url, last_confirmed_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (external_id) DO UPDATE SET
                name = EXCLUDED.name,
                league_id = EXCLUDED.league_id,
                logo_url = EXCLUDED.logo_url,
                last_confirmed_at = now()
            RETURNING id
            """,
            (str(team["id"]), fix_mojibake(team["name"]), db_league_id, team.get("logo")),
        )
        conn.commit()
        return cur.fetchone()[0]


def upsert_players_for_club(conn, club_external_id, season, db_club_id, max_pages=5):
    """Team-scoped player pull — naturally bounded to that club's actual
    current squad (typically 1-2 pages of 20), so max_pages=5 (~100
    players) is a generous ceiling that should never realistically bind,
    unlike the old league-wide pull."""
    page = 1
    total_players = 0
    while True:
        try:
            data = api_get("players", {"team": club_external_id, "season": season, "page": page})
        except PageLimitReached:
            print(f"      reached free-tier page limit (page {page})")
            break
        if not data:
            break
        for entry in data:
            p = entry["player"]
            stats = entry["statistics"][0] if entry["statistics"] else {}
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO players
                        (external_id, full_name, date_of_birth, primary_position, current_club_id, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        current_club_id = EXCLUDED.current_club_id,
                        photo_url = EXCLUDED.photo_url,
                        updated_at = now()
                    """,
                    (str(p["id"]), fix_mojibake(p["name"]), p.get("birth", {}).get("date"),
                     stats.get("games", {}).get("position"), db_club_id, p.get("photo")),
                )
            conn.commit()
            total_players += 1
        if page >= data.__len__() and len(data) < 20:
            break
        page += 1
        if page > max_pages:
            print(f"      reached page cap ({max_pages} pages) for this club — unusually large squad")
            break
    return total_players


def run(league_ids, season, max_pages=5):
    conn = get_conn()
    completed = []
    for league_id in league_ids:
        print(f"Syncing league {league_id} / season {season} ...")
        try:
            db_league_id = upsert_league(conn, league_id, season)
            if db_league_id is None:
                print(f"  no data for league {league_id}, skipping")
                continue

            teams = fetch_current_clubs(league_id, season)
            if not teams:
                print(f"  no clubs found for league {league_id} / season {season}, skipping")
                continue
            print(f"  {len(teams)} clubs found, pulling current squads...")

            total_players = 0
            for team_entry in teams:
                team = team_entry["team"]
                db_club_id = upsert_club(conn, team, db_league_id)
                if db_club_id is None:
                    continue
                n = upsert_players_for_club(conn, team["id"], season, db_club_id, max_pages)
                total_players += n
                print(f"    {team['name']}: {n} players")

            completed.append(league_id)
            print(f"  done: league {league_id} ({total_players} players across {len(teams)} clubs)")
        except RateLimitError as e:
            print(f"\nHit the API rate limit while on league {league_id}.")
            print(f"Actual error detail: {e}")
            print(f"Leagues completed this run: {completed}")
            print(f"Remaining, not yet done: {[l for l in league_ids if l not in completed]}")
            print("Wait for your daily quota to reset, then re-run with --all-leagues —")
            print("already-synced players will just be updated, not duplicated.")
            break
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=int, action="append", help="API-Football league id, repeatable")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--all-leagues", action="store_true",
                         help="Use the curated league list in LEAGUE_IDS below (top 5 + talent-pipeline leagues)")
    parser.add_argument("--max-pages", type=int, default=5,
                         help="Max player-list pages PER CLUB (20 players/page). Default 5 (~100 players) "
                              "is generous headroom above any real squad size.")
    args = parser.parse_args()

    LEAGUE_IDS = [
        39, 140, 78, 135, 61,        # Top 5
        88, 94, 203, 71, 98, 253, 179, 62,   # Original non-top5 set
        40, 144, 262, 128,           # Additional talent-pipeline leagues
    ]

    ids = args.league if args.league else (LEAGUE_IDS if args.all_leagues else [])
    if not ids:
        parser.error("Provide --league <id> (repeatable) or --all-leagues")

    run(ids, args.season, args.max_pages)