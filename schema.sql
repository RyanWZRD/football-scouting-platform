-- ============================================================
-- Football Scouting & Analytics Database — Core Schema
-- Target: PostgreSQL 14+
-- Designed to be filled by API-Football / Wyscout / StatsBomb
-- style feeds. Every stat table is keyed to (player, match)
-- so per-90, percentile, and rolling-window metrics are all
-- derivable rather than stored redundantly.
-- ============================================================

CREATE TABLE countries (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    fifa_code   TEXT
);

CREATE TABLE leagues (
    id              SERIAL PRIMARY KEY,
    external_id     TEXT UNIQUE,          -- id from source API, for sync
    name            TEXT NOT NULL,
    country_id      INT REFERENCES countries(id),
    tier            INT,                  -- 1 = top division, 2 = second, etc.
    is_top5         BOOLEAN DEFAULT FALSE,
    season          TEXT NOT NULL         -- e.g. '2025-26'
);

CREATE TABLE clubs (
    id              SERIAL PRIMARY KEY,
    external_id     TEXT UNIQUE,
    name            TEXT NOT NULL,
    league_id       INT REFERENCES leagues(id),
    country_id      INT REFERENCES countries(id),
    founded         INT,
    logo_url        TEXT
);

CREATE TABLE players (
    id              SERIAL PRIMARY KEY,
    external_id     TEXT UNIQUE,
    full_name       TEXT NOT NULL,
    date_of_birth   DATE,
    nationality_id  INT REFERENCES countries(id),
    height_cm       INT,
    preferred_foot  TEXT CHECK (preferred_foot IN ('left','right','both')),
    primary_position TEXT,                -- GK, CB, RB, LB, DM, CM, AM, RW, LW, ST
    secondary_positions TEXT[],
    current_club_id INT REFERENCES clubs(id),
    photo_url       TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE player_market_values (
    id              SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(id),
    value_eur        NUMERIC,
    source          TEXT,                  -- e.g. 'transfermarkt'
    as_of_date      DATE NOT NULL
);

CREATE TABLE matches (
    id              SERIAL PRIMARY KEY,
    external_id     TEXT UNIQUE,
    league_id       INT REFERENCES leagues(id),
    home_club_id    INT REFERENCES clubs(id),
    away_club_id    INT REFERENCES clubs(id),
    match_date      TIMESTAMPTZ NOT NULL,
    home_score      INT,
    away_score      INT,
    status          TEXT CHECK (status IN ('scheduled','live','finished','postponed'))
);

-- One row per player per match: the atomic unit everything else aggregates from.
CREATE TABLE player_match_stats (
    id                  SERIAL PRIMARY KEY,
    player_id           INT REFERENCES players(id),
    match_id            INT REFERENCES matches(id),
    club_id             INT REFERENCES clubs(id),
    minutes_played      INT DEFAULT 0,
    position_played     TEXT,
    goals               INT DEFAULT 0,
    assists             INT DEFAULT 0,
    shots               INT DEFAULT 0,
    shots_on_target     INT DEFAULT 0,
    key_passes          INT DEFAULT 0,
    passes_completed    INT DEFAULT 0,
    passes_attempted    INT DEFAULT 0,
    progressive_passes  INT DEFAULT 0,
    progressive_carries  INT DEFAULT 0,
    take_ons_attempted  INT DEFAULT 0,
    take_ons_completed  INT DEFAULT 0,
    tackles             INT DEFAULT 0,
    interceptions       INT DEFAULT 0,
    duels_won           INT DEFAULT 0,
    duels_attempted     INT DEFAULT 0,
    xg                  NUMERIC DEFAULT 0,
    xa                  NUMERIC DEFAULT 0,
    rating              NUMERIC,             -- source-provided match rating, if available
    UNIQUE (player_id, match_id)
);

-- Qualitative scouting layer — this is what keeps the system from being stats-only.
CREATE TABLE scout_notes (
    id              SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(id),
    author          TEXT,                   -- scout name or 'system'
    note            TEXT NOT NULL,
    tags            TEXT[],                  -- e.g. {'ball-playing','high-ceiling','injury-prone'}
    watch_level     TEXT CHECK (watch_level IN ('monitor','shortlist','priority')),
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Precomputed potential/scouting scores, refreshed on a schedule (see scoring_model.py).
CREATE TABLE player_potential_scores (
    id                  SERIAL PRIMARY KEY,
    player_id           INT REFERENCES players(id),
    season              TEXT NOT NULL,
    potential_index     NUMERIC NOT NULL,     -- 0-100 composite
    percentile_vs_position NUMERIC,           -- within same position + age band across leagues
    stat_component      NUMERIC,
    age_adjustment      NUMERIC,
    qualitative_component NUMERIC,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (player_id, season)
);

-- Indexes for the queries that actually get run: filter by league/position/age, sort by potential.
CREATE INDEX idx_players_position ON players(primary_position);
CREATE INDEX idx_players_club ON players(current_club_id);
CREATE INDEX idx_pms_player ON player_match_stats(player_id);
CREATE INDEX idx_pms_match ON player_match_stats(match_id);
CREATE INDEX idx_pps_potential ON player_potential_scores(potential_index DESC);
CREATE INDEX idx_leagues_tier ON leagues(tier);
