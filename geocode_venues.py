"""
Geocodes club venues into real lat/long coordinates, stored permanently
so the Geographic Map View doesn't need to re-geocode on every load.
Reuses the exact same Open-Meteo geocoding approach already proven
working for the Altitude & Climate Impact Score feature — including
the fix for city fields that include a county (e.g. "Nottingham,
Nottinghamshire").

IMPORTANT FIX: the first version of this script took only the single
top geocoding result, which genuinely produced wrong matches for
ambiguous names — e.g. "Wien" (Vienna) matched a small town in
Missouri, USA instead of Vienna, Austria. This version requests
multiple candidate results and matches against the club's actual known
country (via its league), falling back to the top result only if no
genuine country match is found among the candidates.

Usage:
    export DATABASE_URL=...
    python geocode_venues.py --limit 500 --refix-wrong
"""

import os
import time
import argparse
import requests
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
REQUEST_DELAY_SECONDS = 0.3

# Maps this project's country naming to the ISO country codes
# Open-Meteo's geocoding API returns, for genuine disambiguation.
# Verified directly against the actual distinct country names present
# in this project's own database, not assumed — e.g. the real value is
# "Czech-Republic" (hyphenated), not "Czech Republic".
COUNTRY_NAME_TO_ISO = {
    "england": "GB", "scotland": "GB", "wales": "GB", "united kingdom": "GB",
    "netherlands": "NL", "belgium": "BE", "germany": "DE", "france": "FR",
    "spain": "ES", "italy": "IT", "portugal": "PT", "austria": "AT",
    "switzerland": "CH", "turkey": "TR", "poland": "PL",
    "czech-republic": "CZ", "czech republic": "CZ", "croatia": "HR",
    "denmark": "DK", "norway": "NO", "sweden": "SE", "greece": "GR",
    "brazil": "BR", "argentina": "AR", "mexico": "MX", "colombia": "CO",
    "united states": "US", "usa": "US", "japan": "JP", "south korea": "KR",
    "australia": "AU", "saudi arabia": "SA", "qatar": "QA", "uae": "AE",
}


# A small, targeted dictionary for known city-name mismatches between
# API-Football (which returns local-language names like "Wien") and
# Open-Meteo's geocoding index (which indexes some major cities under
# their English name instead). Not a comprehensive translation system —
# just the specific cases genuinely found to cause wrong matches.
CITY_NAME_OVERRIDES = {
    "wien": "vienna",
    "münchen": "munich",
    "köln": "cologne",
    "moskva": "moscow",
    "praha": "prague",
    "warszawa": "warsaw",
    "lisboa": "lisbon",
    "roma": "rome",
    "milano": "milan",
    "torino": "turin",
    "genova": "genoa",
    "firenze": "florence",
    "napoli": "naples",
    "sevilla": "seville",
    "athina": "athens",
}


def geocode_city(city, expected_country=None):
    city_only = city.split(",")[0].strip() if city else city
    search_term = CITY_NAME_OVERRIDES.get(city_only.lower(), city_only)
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": search_term, "count": 8}, timeout=6,
        )
        resp.raise_for_status()
        results = resp.json().get("results")
        if not results:
            return None

        if expected_country:
            expected_iso = COUNTRY_NAME_TO_ISO.get(expected_country.strip().lower())
            if expected_iso:
                country_matches = [r for r in results if r.get("country_code") == expected_iso]
                if country_matches:
                    # Prefer the highest-population match among
                    # country-matched candidates, not just the first —
                    # a genuine safeguard against a tiny, obscure
                    # village outranking the actual major city.
                    best = max(country_matches, key=lambda r: r.get("population", 0))
                    return best["latitude"], best["longitude"]

        # No genuine country match found among candidates — fall back
        # to the top result rather than failing outright, but this is
        # honestly a lower-confidence match worth being aware of.
        return results[0]["latitude"], results[0]["longitude"]
    except Exception as e:
        print(f"  Geocoding failed for {city_only!r}: {type(e).__name__}: {e}")
        return None


def run(limit, refix_wrong):
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        if refix_wrong:
            # Re-process everything, including venues already geocoded
            # by the earlier, less accurate version of this script.
            cur.execute("""
                SELECT cv.id, cv.city, co.name AS country
                FROM club_venues cv
                JOIN clubs cl ON cl.id = cv.club_id
                LEFT JOIN leagues l ON l.id = cl.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                WHERE cv.city IS NOT NULL
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT cv.id, cv.city, co.name AS country
                FROM club_venues cv
                JOIN clubs cl ON cl.id = cv.club_id
                LEFT JOIN leagues l ON l.id = cl.league_id
                LEFT JOIN countries co ON co.id = l.country_id
                WHERE cv.city IS NOT NULL AND cv.latitude IS NULL
                LIMIT %s
            """, (limit,))
        venues = cur.fetchall()

    print(f"Geocoding {len(venues)} venues (country-aware disambiguation).")
    updated = 0
    for i, (venue_id, city, country) in enumerate(venues, 1):
        coords = geocode_city(city, country)
        time.sleep(REQUEST_DELAY_SECONDS)
        if not coords:
            continue
        lat, lon = coords
        with conn.cursor() as cur:
            cur.execute("UPDATE club_venues SET latitude = %s, longitude = %s WHERE id = %s", (lat, lon, venue_id))
        conn.commit()
        updated += 1
        if i % 25 == 0:
            print(f"  ...{i}/{len(venues)} processed ({updated} geocoded)")

    conn.close()
    print(f"Done. {updated} venues geocoded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--refix-wrong", action="store_true", help="Re-process all venues, including already-geocoded ones, using the improved country-aware matching")
    args = parser.parse_args()
    run(args.limit, args.refix_wrong)