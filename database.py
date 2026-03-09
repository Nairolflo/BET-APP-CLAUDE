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
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
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


def ph():
    return "%s" if is_postgres() else "?"


def row_to_dict(cur, row):
    if is_postgres():
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    return dict(row)


def rows_to_dicts(cur, rows):
    if is_postgres():
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    return [dict(row) for row in rows]


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
                    success          INTEGER DEFAULT -1,
                    notified         INTEGER DEFAULT 0,
                    bete_noire       INTEGER DEFAULT 0,
                    bete_noire_rate  REAL DEFAULT 0,
                    created_at       TIMESTAMP DEFAULT NOW()
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
                    success          INTEGER DEFAULT -1,
                    notified         INTEGER DEFAULT 0,
                    bete_noire       INTEGER DEFAULT 0,
                    bete_noire_rate  REAL DEFAULT 0,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

        # ── Migration : colonnes bete_noire (ajoutées après création initiale)
        try:
            if is_postgres():
                cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS bete_noire INTEGER DEFAULT 0")
                cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS bete_noire_rate REAL DEFAULT 0")
            else:
                # SQLite ne supporte pas IF NOT EXISTS sur ALTER TABLE
                cur.execute("PRAGMA table_info(bets)")
                cols = [r[1] for r in cur.fetchall()]
                if "bete_noire" not in cols:
                    cur.execute("ALTER TABLE bets ADD COLUMN bete_noire INTEGER DEFAULT 0")
                if "bete_noire_rate" not in cols:
                    cur.execute("ALTER TABLE bets ADD COLUMN bete_noire_rate REAL DEFAULT 0")
            conn.commit()
        except Exception as e:
            log.warning(f"[DB] Migration bete_noire ignorée : {e}")

        log.info("[DB] Database initialized.")
        print("[DB] Database initialized.")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# BETS
# ─────────────────────────────────────────────

def save_bet(bet: dict) -> int:
    """
    Insère un bet en DB.
    Si un bet identique existe déjà (même match + marché + bookmaker),
    retourne l'ID existant sans créer de doublon.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()

        # Vérification doublon
        cur.execute(f"""
            SELECT id FROM bets
            WHERE match_date = {p} AND home_team = {p}
              AND away_team = {p} AND market = {p} AND bookmaker = {p}
        """, (
            bet.get("match_date"), bet.get("home_team"),
            bet.get("away_team"), bet.get("market"), bet.get("bookmaker"),
        ))
        existing = cur.fetchone()
        if existing:
            return existing[0]

        # Insertion
        if is_postgres():
            cur.execute("""
                INSERT INTO bets
                    (match_date, league, home_team, away_team, market,
                     bookmaker, bk_odds, model_odds, probability, value,
                     bete_noire, bete_noire_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                bet.get("match_date"), bet.get("league"),
                bet.get("home_team"), bet.get("away_team"),
                bet.get("market"), bet.get("bookmaker"),
                bet.get("bk_odds"), bet.get("model_odds"),
                bet.get("probability"), bet.get("value"),
                1 if bet.get("bete_noire") else 0,
                bet.get("bete_noire_rate", 0),
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
        return rows_to_dicts(cur, cur.fetchall())
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


def is_bet_notified(bet_id: int) -> bool:
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        cur.execute(f"SELECT notified FROM bets WHERE id = {p}", (bet_id,))
        row = cur.fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def mark_bet_notified(bet_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        cur.execute(f"UPDATE bets SET notified = 1 WHERE id = {p}", (bet_id,))
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
        return rows_to_dicts(cur, cur.fetchall())
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
        p = ph()
        cur.execute(f"""
            SELECT team_id, team_name,
                   home_goals_scored, home_goals_conceded,
                   away_goals_scored, away_goals_conceded,
                   home_games, away_games
            FROM team_stats
            WHERE league_id = {p} AND season = {p}
        """, (league_id, season))
        rows = rows_to_dicts(cur, cur.fetchall())
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

        # Sous-requête dédoublonnée : un seul bet par home+away+market (le plus récent)
        dedup = """
            SELECT id, league, value, success
            FROM bets
            WHERE id IN (
                SELECT MAX(id) FROM bets
                GROUP BY home_team, away_team, market
            )
        """

        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN success = -1 THEN 1 ELSE 0 END) as pending
            FROM ({dedup}) u
        """)
        overall_raw = row_to_dict(cur, cur.fetchone())

        total   = overall_raw.get("total") or 0
        wins    = overall_raw.get("wins") or 0
        losses  = overall_raw.get("losses") or 0
        pending = overall_raw.get("pending") or 0

        settled  = max(total - pending, 1)
        win_rate = round(wins / settled * 100, 1)
        roi      = round((wins - losses) / settled * 100, 1)

        cur.execute(f"""
            SELECT league,
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value
            FROM ({dedup}) u
            GROUP BY league
        """)
        by_league = rows_to_dicts(cur, cur.fetchall())

        cur.execute(f"""
            SELECT ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value
            FROM ({dedup}) u
        """)
        avg_row = cur.fetchone()
        avg_value_pct = (avg_row[0] if avg_row else 0) or 0

        return {
            "overall": {
                "total":         total,
                "wins":          wins,
                "losses":        losses,
                "pending":       pending,
                "win_rate":      win_rate,
                "roi":           roi,
                "avg_value_pct": float(avg_value_pct),
            },
            "by_league": by_league,
        }
    finally:
        conn.close()


def delete_today_pending_bets():
    """
    Supprime tous les bets du jour non encore résolus.
    Appelé au début de chaque run pour éviter les doublons.
    """
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        cur.execute(
            f"DELETE FROM bets WHERE match_date = {p} AND success = -1",
            (today,)
        )
        deleted = cur.rowcount
        conn.commit()
        log.info(f"[DB] {deleted} bets du jour supprimés avant réanalyse.")
    finally:
        conn.close()


def get_unique_bets(limit: int = 200) -> list:
    """
    Retourne les bets sans doublons.
    Pour chaque combinaison home_team + away_team + market,
    garde uniquement le plus récent.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                SELECT DISTINCT ON (home_team, away_team, market)
                    id, match_date, league, home_team, away_team,
                    market, bookmaker, bk_odds, model_odds, probability,
                    value, success, notified, created_at
                FROM bets
                ORDER BY home_team, away_team, market, created_at DESC
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT id, match_date, league, home_team, away_team,
                       market, bookmaker, bk_odds, model_odds, probability,
                       value, success, notified, created_at
                FROM bets
                WHERE id IN (
                    SELECT MAX(id) FROM bets
                    GROUP BY home_team, away_team, market
                )
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def reset_all_bets() -> int:
    """Supprime tous les paris de la DB. Retourne le nombre supprimé."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bets")
        row = cur.fetchone()
        count = row[0] if row else 0
        cur.execute("DELETE FROM bets")
        conn.commit()
        return count
    finally:
        conn.close()


# ─────────────────────────────────────────────
# STATS PAR MARCHÉ (pour ROI séparé)
# ─────────────────────────────────────────────

def get_stats_by_market() -> list:
    """ROI + win rate par type de marché : Home Win, Away Win, Over 2.5, etc."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        dedup = """
            SELECT id, market, value, bk_odds, success
            FROM bets
            WHERE id IN (
                SELECT MAX(id) FROM bets
                GROUP BY home_team, away_team, market
            )
        """
        cur.execute(f"""
            SELECT
                market,
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN success = -1 THEN 1 ELSE 0 END) as pending,
                ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value,
                ROUND(CAST(AVG(bk_odds) AS NUMERIC), 2) as avg_odds
            FROM ({dedup}) u
            GROUP BY market
            ORDER BY total DESC
        """)
        rows = rows_to_dicts(cur, cur.fetchall())
        result = []
        for r in rows:
            settled = max((r.get("total") or 0) - (r.get("pending") or 0), 1)
            wins    = r.get("wins") or 0
            losses  = r.get("losses") or 0
            result.append({
                **r,
                "win_rate": round(wins / settled * 100, 1),
                "roi":      round((wins - losses) / settled * 100, 1),
            })
        return result
    finally:
        conn.close()


def get_stats_by_league_detailed() -> list:
    """Stats détaillées par ligue avec ROI."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        dedup = """
            SELECT id, league, value, bk_odds, success
            FROM bets
            WHERE id IN (
                SELECT MAX(id) FROM bets
                GROUP BY home_team, away_team, market
            )
        """
        cur.execute(f"""
            SELECT
                league,
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN success = -1 THEN 1 ELSE 0 END) as pending,
                ROUND(CAST(AVG(value) * 100 AS NUMERIC), 1) as avg_value,
                ROUND(CAST(AVG(bk_odds) AS NUMERIC), 2) as avg_odds
            FROM ({dedup}) u
            GROUP BY league
            ORDER BY total DESC
        """)
        rows = rows_to_dicts(cur, cur.fetchall())
        result = []
        for r in rows:
            settled = max((r.get("total") or 0) - (r.get("pending") or 0), 1)
            wins    = r.get("wins") or 0
            losses  = r.get("losses") or 0
            result.append({
                **r,
                "win_rate": round(wins / settled * 100, 1),
                "roi":      round((wins - losses) / settled * 100, 1),
            })
        return result
    finally:
        conn.close()


def get_bete_noire_bets(limit: int = 200) -> list:
    """Retourne uniquement les bets avec flag bête noire."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        if is_postgres():
            cur.execute("""
                SELECT DISTINCT ON (home_team, away_team, market)
                    id, match_date, league, home_team, away_team,
                    market, bookmaker, bk_odds, model_odds, probability,
                    value, success, bete_noire, bete_noire_rate
                FROM bets
                WHERE bete_noire = 1
                ORDER BY home_team, away_team, market, created_at DESC
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT id, match_date, league, home_team, away_team,
                       market, bookmaker, bk_odds, model_odds, probability,
                       value, success, bete_noire, bete_noire_rate
                FROM bets
                WHERE bete_noire = 1
                  AND id IN (SELECT MAX(id) FROM bets GROUP BY home_team, away_team, market)
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def get_roi_over_time() -> list:
    """ROI cumulé par date pour le graphe d'évolution."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT match_date, success, value, bk_odds
            FROM bets
            WHERE success != -1
              AND id IN (SELECT MAX(id) FROM bets GROUP BY home_team, away_team, market)
            ORDER BY match_date ASC
        """)
        rows = rows_to_dicts(cur, cur.fetchall())
        result = []
        cumulative = 0.0
        count = 0
        for r in rows:
            count += 1
            if r["success"] == 1:
                cumulative += (r["bk_odds"] - 1)
            else:
                cumulative -= 1
            roi = round(cumulative / count * 100, 1)
            result.append({"date": r["match_date"], "roi": roi, "count": count})
        return result
    finally:
        conn.close()


def get_streak() -> dict:
    """Retourne la série en cours (victoires ou défaites consécutives)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT success FROM bets
            WHERE success != -1
              AND id IN (SELECT MAX(id) FROM bets GROUP BY home_team, away_team, market)
            ORDER BY created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        if not rows:
            return {"type": None, "count": 0}
        first = rows[0][0]
        count = 0
        for r in rows:
            if r[0] == first:
                count += 1
            else:
                break
        return {"type": "win" if first == 1 else "loss", "count": count}
    finally:
        conn.close()