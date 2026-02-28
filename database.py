"""
database.py - SQLite database management for ValueBet Bot
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "valuebet.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize database schema."""
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now')),
            match_date TEXT NOT NULL,
            league TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            market TEXT NOT NULL,          -- e.g. "Home Win", "Draw", "Away Win"
            bookmaker TEXT NOT NULL,        -- e.g. "Winamax", "Betclic"
            bk_odds REAL NOT NULL,          -- Bookmaker odds
            model_odds REAL NOT NULL,       -- Our calculated fair odds (1/prob)
            probability REAL NOT NULL,      -- Our probability (0-1)
            value REAL NOT NULL,            -- Value = (bk_odds * probability) - 1
            result TEXT,                    -- Final score, e.g. "2-1"
            success INTEGER,               -- 1 = won, 0 = lost, NULL = pending
            notified INTEGER DEFAULT 0     -- 1 if sent via Telegram
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS team_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT DEFAULT (datetime('now')),
            league_id INTEGER NOT NULL,
            season INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            home_goals_scored REAL DEFAULT 0,
            home_goals_conceded REAL DEFAULT 0,
            away_goals_scored REAL DEFAULT 0,
            away_goals_conceded REAL DEFAULT 0,
            home_games INTEGER DEFAULT 0,
            away_games INTEGER DEFAULT 0,
            UNIQUE(league_id, season, team_id)
        )
    """)

    conn.commit()
    conn.close()
    print("[DB] Database initialized.")


def save_bet(bet: dict) -> int:
    """Insert a bet record and return its ID."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO bets (
            match_date, league, home_team, away_team, market,
            bookmaker, bk_odds, model_odds, probability, value
        ) VALUES (
            :match_date, :league, :home_team, :away_team, :market,
            :bookmaker, :bk_odds, :model_odds, :probability, :value
        )
    """, bet)
    bet_id = c.lastrowid
    conn.commit()
    conn.close()
    return bet_id


def update_bet_result(bet_id: int, result: str, success: int):
    """Update a bet with its real-world result."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        UPDATE bets SET result = ?, success = ? WHERE id = ?
    """, (result, success, bet_id))
    conn.commit()
    conn.close()


def get_all_bets(limit: int = 200):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM bets ORDER BY match_date DESC, created_at DESC LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_stats():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN success IS NULL THEN 1 ELSE 0 END) as pending,
            ROUND(AVG(value) * 100, 2) as avg_value_pct,
            ROUND(AVG(probability) * 100, 2) as avg_probability_pct
        FROM bets
    """)
    overall = dict(c.fetchone())

    # ROI: (sum of returns - total staked) / total staked * 100
    c.execute("""
        SELECT
            SUM(CASE WHEN success = 1 THEN bk_odds - 1 ELSE -1 END) as net_units,
            COUNT(CASE WHEN success IS NOT NULL THEN 1 END) as resolved
        FROM bets
    """)
    roi_row = dict(c.fetchone())
    if roi_row["resolved"] and roi_row["resolved"] > 0:
        overall["roi"] = round(roi_row["net_units"] / roi_row["resolved"] * 100, 2)
    else:
        overall["roi"] = 0.0

    # Win rate
    if overall["wins"] is not None and (overall["wins"] + (overall["losses"] or 0)) > 0:
        overall["win_rate"] = round(overall["wins"] / (overall["wins"] + overall["losses"]) * 100, 2)
    else:
        overall["win_rate"] = 0.0

    # By league
    c.execute("""
        SELECT league,
            COUNT(*) as total,
            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
            ROUND(AVG(value)*100,2) as avg_value
        FROM bets GROUP BY league
    """)
    by_league = [dict(r) for r in c.fetchall()]

    conn.close()
    return {"overall": overall, "by_league": by_league}


def save_team_stats(stats: dict):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO team_stats (
            league_id, season, team_id, team_name,
            home_goals_scored, home_goals_conceded,
            away_goals_scored, away_goals_conceded,
            home_games, away_games
        ) VALUES (
            :league_id, :season, :team_id, :team_name,
            :home_goals_scored, :home_goals_conceded,
            :away_goals_scored, :away_goals_conceded,
            :home_games, :away_games
        )
        ON CONFLICT(league_id, season, team_id) DO UPDATE SET
            team_name = excluded.team_name,
            home_goals_scored = excluded.home_goals_scored,
            home_goals_conceded = excluded.home_goals_conceded,
            away_goals_scored = excluded.away_goals_scored,
            away_goals_conceded = excluded.away_goals_conceded,
            home_games = excluded.home_games,
            away_games = excluded.away_games,
            updated_at = datetime('now')
    """, stats)
    conn.commit()
    conn.close()


def get_team_stats(league_id: int, season: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM team_stats WHERE league_id = ? AND season = ?
    """, (league_id, season))
    rows = {r["team_id"]: dict(r) for r in c.fetchall()}
    conn.close()
    return rows
