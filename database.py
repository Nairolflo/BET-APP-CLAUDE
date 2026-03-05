"""
database.py - PostgreSQL database layer

Utilise DATABASE_URL (fourni automatiquement par Railway PostgreSQL).
Fallback SQLite si DATABASE_URL absent (dev local).
"""

import os
import logging

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────

def get_connection():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        # Fallback SQLite pour dev local
        import sqlite3
        db_path = os.getenv("DB_PATH", "valuebet.db")
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn


def is_postgres():
    return bool(DATABASE_URL)


def placeholder(n=1):
    """Retourne %s pour PostgreSQL, ? pour SQLite."""
    if is_postgres():
        return ", ".join(["%s"] * n)
    return ", ".join(["?"] * n)


def ph():
    """Single placeholder."""
    return "%s" if is_postgres() else "?"


# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────

def init_db():
    conn = get_connection()
    try:
        cur = conn.cursor()

        if is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id          SERIAL PRIMARY KEY,
                    match_date  TEXT,
                    league      TEXT,
                    home_team   TEXT,
                    away_team   TEXT,
                    market      TEXT,
                    bookmaker   TEXT,
                    bk_odds     REAL,
                    model_odds  REAL,
                    probability REAL,
                    value       REAL,
                    success     INTEGER DEFAULT -1,
                    notified    INTEGER DEFAULT 0,
                    created_at  TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_stats (
                    id                  SERIAL PRIMARY KEY,
                    league_id           INTEGER,
                    season              INTEGER,
                    team_id             INTEGER,
                    team_name           TEXT,
                    home_goals_scored   INTEGER DEFAULT 0,
                    home_goals_conceded INTEGER DEFAULT 0,
                    away_goals_scored   INTEGER DEFAULT 0,
                    away_goals_conceded INTEGER DEFAULT 0,
                    home_games          INTEGER DEFAULT 0,
                    away_games          INTEGER DEFAULT 0,
                    updated_at          TIMESTAMP DEFAULT NOW(),
                    UNIQUE(league_id, season, team_id)
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_date  TEXT,
                    league      TEXT,
                    home_team   TEXT,
                    away_team   TEXT,
                    market      TEXT,
                    bookmaker   TEXT,
                    bk_odds     REAL,
                    model_odds  REAL,
                    probability REAL,
                    value       REAL,
                    success     INTEGER DEFAULT -1,
                    notified    INTEGER DEFAULT 0,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_stats (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id           INTEGER,
                    season              INTEGER,
                    team_id             INTEGER,
                    team_name           TEXT,
                    home_goals_scored   INTEGER DEFAULT 0,
                    home_goals_conceded INTEGER DEFAULT 0,
                    away_goals_scored   INTEGER DEFAULT 0,
                    away_goals_conceded INTEGER DEFAULT 0,
                    home_games          INTEGER DEFAULT 0,
                    away_games          INTEGER DEFAULT 0,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(league_id, season, team_id)
                )
            """)

        conn.commit()
        log.info("[DB] Database initialized.")
        print("[DB] Database initialized.")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# BETS
# ─────────────────────────────────────────────

def save_bet(bet: dict) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                INSERT INTO bets
                    (match_date, league, home_team, away_team, market,
                     bookmaker, bk_odds, model_odds, probability, value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                bet.get("match_date"), bet.get("league"),
                bet.get("home_team"), bet.get("away_team"),
                bet.get("market"), bet.get("bookmaker"),
                bet.get("bk_odds"), bet.get("model_odds"),
                bet.get("probability"), bet.get("value"),
            ))
            bet_id = cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO bets
                    (match_date, league, home_team, away_team, market,
                     bookmaker, bk_odds, model_odds, probability, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bet.get("match_date"), bet.get("league"),
                bet.get("home_team"), bet.get("away_team"),
                bet.get("market"), bet.get("bookmaker"),
                bet.get("bk_odds"), bet.get("model_odds"),
                bet.get("probability"), bet.get("value"),
            ))
            bet_id = cur.lastrowid

        conn.commit()
        return bet_id
    finally:
        conn.close()


def get_all_bets(limit: int = 100) -> list:
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        cur.execute(f"""
            SELECT id, match_date, league, home_team, away_team,
                   market, bookmaker, bk_odds, model_odds, probability,
                   value, success, notified, created_at
            FROM bets
            ORDER BY created_at DESC
            LIMIT {p}
        """, (limit,))
        rows = cur.fetchall()
        if is_postgres():
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in rows]
        return [dict(row) for row in rows]
    finally:
        conn.close()


def update_bet_result(bet_id: int, success: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        cur.execute(f"UPDATE bets SET success = {p} WHERE id = {p}", (success, bet_id))
        conn.commit()
    finally:
        conn.close()


def get_pending_bets() -> list:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, match_date, league, home_team, away_team,
                   market, bookmaker, bk_odds, probability, value
            FROM bets WHERE success = -1
            ORDER BY match_date ASC
        """)
        rows = cur.fetchall()
        if is_postgres():
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in rows]
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────────
# TEAM STATS
# ─────────────────────────────────────────────

def save_team_stats(team: dict):
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                INSERT INTO team_stats
                    (league_id, season, team_id, team_name,
                     home_goals_scored, home_goals_conceded,
                     away_goals_scored, away_goals_conceded,
                     home_games, away_games, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (league_id, season, team_id)
                DO UPDATE SET
                    team_name           = EXCLUDED.team_name,
                    home_goals_scored   = EXCLUDED.home_goals_scored,
                    home_goals_conceded = EXCLUDED.home_goals_conceded,
                    away_goals_scored   = EXCLUDED.away_goals_scored,
                    away_goals_conceded = EXCLUDED.away_goals_conceded,
                    home_games          = EXCLUDED.home_games,
                    away_games          = EXCLUDED.away_games,
                    updated_at          = NOW()
            """, (
                team["league_id"], team["season"], team["team_id"], team["team_name"],
                team["home_goals_scored"], team["home_goals_conceded"],
                team["away_goals_scored"], team["away_goals_conceded"],
                team["home_games"], team["away_games"],
            ))
        else:
            cur.execute("""
                INSERT OR REPLACE INTO team_stats
                    (league_id, season, team_id, team_name,
                     home_goals_scored, home_goals_conceded,
                     away_goals_scored, away_goals_conceded,
                     home_games, away_games)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                team["league_id"], team["season"], team["team_id"], team["team_name"],
                team["home_goals_scored"], team["home_goals_conceded"],
                team["away_goals_scored"], team["away_goals_conceded"],
                team["home_games"], team["away_games"],
            ))
        conn.commit()
    finally:
        conn.close()


def get_team_stats(league_id: int, season: int) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                SELECT team_id, team_name,
                       home_goals_scored, home_goals_conceded,
                       away_goals_scored, away_goals_conceded,
                       home_games, away_games
                FROM team_stats
                WHERE league_id = %s AND season = %s
            """, (league_id, season))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        else:
            cur.execute("""
                SELECT team_id, team_name,
                       home_goals_scored, home_goals_conceded,
                       away_goals_scored, away_goals_conceded,
                       home_games, away_games
                FROM team_stats
                WHERE league_id = ? AND season = ?
            """, (league_id, season))
            rows = [dict(row) for row in cur.fetchall()]

        return {row["team_id"]: row for row in rows}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# STATS ROI / WIN RATE
# ─────────────────────────────────────────────

def get_stats() -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Stats globales
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN success = -1 THEN 1 ELSE 0 END) as pending
            FROM bets
        """)
        row = cur.fetchone()
        if is_postgres():
            cols = [d[0] for d in cur.description]
            overall_raw = dict(zip(cols, row))
        else:
            overall_raw = dict(row)

        total   = overall_raw.get("total") or 0
        wins    = overall_raw.get("wins") or 0
        losses  = overall_raw.get("losses") or 0
        pending = overall_raw.get("pending") or 0

        win_rate = round(wins / max(total - pending, 1) * 100, 1)
        roi      = round((wins - losses) / max(total - pending, 1) * 100, 1)

        # Stats par ligue
        cur.execute("""
            SELECT league,
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value
            FROM bets
            GROUP BY league
        """)
        if is_postgres():
            cols = [d[0] for d in cur.description]
            by_league = [dict(zip(cols, r)) for r in cur.fetchall()]
        else:
            by_league = [dict(r) for r in cur.fetchall()]

        # Valeur moyenne globale
        cur.execute("SELECT ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value FROM bets")
        avg_row = cur.fetchone()
        avg_value_pct = (avg_row[0] if is_postgres() else dict(avg_row).get("avg_value")) or 0

        return {
            "overall": {
                "total":         total,
                "wins":          wins,
                "losses":        losses,
                "pending":       pending,
                "win_rate":      win_rate,
                "roi":           roi,
                "avg_value_pct": avg_value_pct,
            },
            "by_league": by_league,
        }
    finally:
        conn.close()