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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS h2h_cache (
                    id          SERIAL PRIMARY KEY,
                    league_id   INTEGER NOT NULL,
                    season      INTEGER NOT NULL,
                    matches_json TEXT NOT NULL,
                    fetched_at  TIMESTAMP DEFAULT NOW(),
                    UNIQUE(league_id, season)
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS h2h_cache (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id   INTEGER NOT NULL,
                    season      INTEGER NOT NULL,
                    matches_json TEXT NOT NULL,
                    fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(league_id, season)
                )
            """)

        conn.commit()

        # ── Migration : table h2h_cache (ajoutée après création initiale)
        try:
            if is_postgres():
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS h2h_cache (
                        id           SERIAL PRIMARY KEY,
                        league_id    INTEGER NOT NULL,
                        season       INTEGER NOT NULL,
                        matches_json TEXT NOT NULL,
                        fetched_at   TIMESTAMP DEFAULT NOW(),
                        UNIQUE(league_id, season)
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS h2h_cache (
                        id           INTEGER PRIMARY KEY AUTOINCREMENT,
                        league_id    INTEGER NOT NULL,
                        season       INTEGER NOT NULL,
                        matches_json TEXT NOT NULL,
                        fetched_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(league_id, season)
                    )
                """)
            conn.commit()
        except Exception as e:
            log.warning(f"[DB] Migration h2h_cache ignorée : {e}")

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

FR_BOOKMAKERS = {"winamax", "betclic", "unibet"}

def _is_fr_bookmaker(name: str) -> bool:
    if not name:
        return False
    n = name.strip().lower()
    for bk in FR_BOOKMAKERS:
        if n.startswith(bk):
            rest = n[len(bk):].strip().replace("(","").replace(")","")
            if rest in ("", "fr", "(fr)"):
                return True
    return False


def purge_non_fr_bets() -> int:
    """Supprime de la DB tous les bets dont le bookmaker n'est pas FR."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, bookmaker FROM bets")
        rows = cur.fetchall()
        to_delete = [row[0] for row in rows if not _is_fr_bookmaker(row[1] or "")]
        if to_delete:
            p = ph()
            placeholders = ",".join([p] * len(to_delete))
            cur.execute(f"DELETE FROM bets WHERE id IN ({placeholders})", to_delete)
            conn.commit()
        return len(to_delete)
    finally:
        conn.close()


def save_bet(bet: dict) -> int:
    """
    Insère un bet en DB.
    Si un bet identique existe déjà (même match + marché + bookmaker),
    retourne l'ID existant sans créer de doublon.
    """
    if not _is_fr_bookmaker(bet.get("bookmaker", "")):
        return -1  # bookmaker non-FR, refusé silencieusement
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


def get_unique_bets(limit: int = 500) -> list:
    """
    Retourne les bets sans doublons, bookmakers FR uniquement.
    Pour chaque combinaison home_team + away_team + market,
    garde uniquement le plus récent.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Filtre bookmakers FR
        fr_filter = """
            AND (
                LOWER(bookmaker) LIKE '%winamax%'
                OR LOWER(bookmaker) LIKE '%betclic%'
                OR LOWER(bookmaker) LIKE '%unibet%fr%'
                OR LOWER(bookmaker) = 'unibet'
            )
        """
        if is_postgres():
            cur.execute(f"""
                SELECT DISTINCT ON (home_team, away_team, market)
                    id, match_date, league, home_team, away_team,
                    market, bookmaker, bk_odds, model_odds, probability,
                    value, success, notified, created_at
                FROM bets
                WHERE 1=1 {fr_filter}
                ORDER BY home_team, away_team, market, created_at DESC
                LIMIT %s
            """, (limit,))
        else:
            cur.execute(f"""
                SELECT id, match_date, league, home_team, away_team,
                       market, bookmaker, bk_odds, model_odds, probability,
                       value, success, notified, created_at
                FROM bets
                WHERE id IN (
                    SELECT MAX(id) FROM bets
                    WHERE 1=1 {fr_filter}
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
        # Calcul séries par marché
        cur.execute(f"""
            SELECT market, success
            FROM ({dedup}) u
            WHERE success != -1
            ORDER BY market, id
        """)
        series_rows = cur.fetchall()

        # Grouper par marché
        from collections import defaultdict
        market_results = defaultdict(list)
        for row in series_rows:
            market_results[row[0]].append(row[1])

        def calc_series(results):
            if not results: return 0, 0, 0
            # Série actuelle
            current = 1
            for i in range(len(results)-1, 0, -1):
                if results[i] == results[i-1]: current += 1
                else: break
            current = current if results[-1] == 1 else -current
            # Séries moyennes
            win_streaks, loss_streaks = [], []
            streak, streak_type = 1, results[0]
            for i in range(1, len(results)):
                if results[i] == streak_type: streak += 1
                else:
                    (win_streaks if streak_type == 1 else loss_streaks).append(streak)
                    streak, streak_type = 1, results[i]
            (win_streaks if streak_type == 1 else loss_streaks).append(streak)
            avg_win  = round(sum(win_streaks)/len(win_streaks), 2)  if win_streaks  else 0
            avg_loss = round(sum(loss_streaks)/len(loss_streaks), 2) if loss_streaks else 0
            return current, avg_win, avg_loss

        result = []
        for r in rows:
            settled = max((r.get("total") or 0) - (r.get("pending") or 0), 1)
            wins    = r.get("wins") or 0
            losses  = r.get("losses") or 0
            current, avg_win, avg_loss = calc_series(market_results.get(r["market"], []))
            result.append({
                **r,
                "win_rate":    round(wins / settled * 100, 1),
                "roi":         round((wins - losses) / settled * 100, 1),
                "streak_cur":  current,
                "streak_win":  avg_win,
                "streak_loss": avg_loss,
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
        # Calcul séries par ligue
        cur.execute(f"""
            SELECT league, success
            FROM ({dedup}) u
            WHERE success != -1
            ORDER BY league, id
        """)
        series_rows = cur.fetchall()

        # Grouper par ligue
        from collections import defaultdict
        league_results = defaultdict(list)
        for row in series_rows:
            league_results[row[0]].append(row[1])

        def calc_series(results):
            if not results: return 0, 0, 0
            # Série actuelle
            current = 1
            for i in range(len(results)-1, 0, -1):
                if results[i] == results[i-1]: current += 1
                else: break
            current = current if results[-1] == 1 else -current
            # Séries moyennes
            win_streaks, loss_streaks = [], []
            streak, streak_type = 1, results[0]
            for i in range(1, len(results)):
                if results[i] == streak_type: streak += 1
                else:
                    (win_streaks if streak_type == 1 else loss_streaks).append(streak)
                    streak, streak_type = 1, results[i]
            (win_streaks if streak_type == 1 else loss_streaks).append(streak)
            avg_win  = round(sum(win_streaks)/len(win_streaks), 2)  if win_streaks  else 0
            avg_loss = round(sum(loss_streaks)/len(loss_streaks), 2) if loss_streaks else 0
            return current, avg_win, avg_loss

        result = []
        for r in rows:
            settled = max((r.get("total") or 0) - (r.get("pending") or 0), 1)
            wins    = r.get("wins") or 0
            losses  = r.get("losses") or 0
            current, avg_win, avg_loss = calc_series(league_results.get(r["league"], []))
            result.append({
                **r,
                "win_rate":    round(wins / settled * 100, 1),
                "roi":         round((wins - losses) / settled * 100, 1),
                "streak_cur":  current,
                "streak_win":  avg_win,
                "streak_loss": avg_loss,
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


# ─────────────────────────────────────────────
# H2H CACHE — stockage persistant des matchs historiques
# ─────────────────────────────────────────────

import json as _json

H2H_CACHE_TTL_DAYS = 7  # renouvelle le cache toutes les semaines max


def get_h2h_cache(league_id: int, season: int) -> list:
    """
    Retourne les matchs stockés pour cette ligue+saison.
    Retourne None si absent ou expiré (> TTL_DAYS jours).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        if is_postgres():
            cur.execute(f"""
                SELECT matches_json, fetched_at,
                       NOW() - fetched_at AS age
                FROM h2h_cache
                WHERE league_id = {p} AND season = {p}
            """, (league_id, season))
        else:
            cur.execute(f"""
                SELECT matches_json, fetched_at,
                       CAST((julianday('now') - julianday(fetched_at)) AS INTEGER) AS age_days
                FROM h2h_cache
                WHERE league_id = {p} AND season = {p}
            """, (league_id, season))
        row = cur.fetchone()
        if not row:
            return None

        matches_json, fetched_at, age = row[0], row[1], row[2]

        # Vérifie l'âge
        if is_postgres():
            age_days = age.days if hasattr(age, 'days') else H2H_CACHE_TTL_DAYS + 1
        else:
            age_days = int(age) if age is not None else H2H_CACHE_TTL_DAYS + 1

        if age_days > H2H_CACHE_TTL_DAYS:
            return None  # expiré → refetch

        return _json.loads(matches_json)
    except Exception as e:
        log.warning(f"[DB] get_h2h_cache error: {e}")
        return None
    finally:
        conn.close()


def set_h2h_cache(league_id: int, season: int, matches: list):
    """Stocke ou met à jour les matchs H2H pour cette ligue+saison."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        p = ph()
        data = _json.dumps(matches)
        if is_postgres():
            cur.execute(f"""
                INSERT INTO h2h_cache (league_id, season, matches_json, fetched_at)
                VALUES ({p}, {p}, {p}, NOW())
                ON CONFLICT (league_id, season)
                DO UPDATE SET matches_json = EXCLUDED.matches_json, fetched_at = NOW()
            """, (league_id, season, data))
        else:
            cur.execute(f"""
                INSERT OR REPLACE INTO h2h_cache (league_id, season, matches_json, fetched_at)
                VALUES ({p}, {p}, {p}, CURRENT_TIMESTAMP)
            """, (league_id, season, data))
        conn.commit()
    except Exception as e:
        log.warning(f"[DB] set_h2h_cache error: {e}")
    finally:
        conn.close()


def get_h2h_cache_status() -> list:
    """Retourne le statut du cache H2H pour l'affichage (config page)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                SELECT league_id, season,
                       jsonb_array_length(matches_json::jsonb) as match_count,
                       fetched_at,
                       EXTRACT(DAY FROM NOW() - fetched_at)::INTEGER as age_days
                FROM h2h_cache
                ORDER BY league_id, season DESC
            """)
        else:
            cur.execute("""
                SELECT league_id, season,
                       json_array_length(matches_json) as match_count,
                       fetched_at,
                       CAST((julianday('now') - julianday(fetched_at)) AS INTEGER) as age_days
                FROM h2h_cache
                ORDER BY league_id, season DESC
            """)
        return rows_to_dicts(cur, cur.fetchall())
    except Exception as e:
        log.warning(f"[DB] get_h2h_cache_status error: {e}")
        return []
    finally:
        conn.close()
def init_biathlon_watchlist():
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""CREATE TABLE IF NOT EXISTS biathlon_watchlist (
                id SERIAL PRIMARY KEY, race_id TEXT, race_desc TEXT,
                race_fmt TEXT, race_date TEXT,
                ibu_a TEXT, name_a TEXT, nat_a TEXT,
                ibu_b TEXT, name_b TEXT, nat_b TEXT,
                result INTEGER DEFAULT -1,
                created_at TIMESTAMP DEFAULT NOW())""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS biathlon_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT, race_id TEXT, race_desc TEXT,
                race_fmt TEXT, race_date TEXT,
                ibu_a TEXT, name_a TEXT, nat_a TEXT,
                ibu_b TEXT, name_b TEXT, nat_b TEXT,
                result INTEGER DEFAULT -1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.commit()
    finally:
        conn.close()

def save_biathlon_watchlist(item: dict) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        cur.execute(f"SELECT id FROM biathlon_watchlist WHERE race_id={p} AND ibu_a={p} AND ibu_b={p}",
            (item["race_id"], item["ibu_a"], item["ibu_b"]))
        if cur.fetchone(): return -1
        if is_postgres():
            cur.execute("""INSERT INTO biathlon_watchlist
                (race_id,race_desc,race_fmt,race_date,ibu_a,name_a,nat_a,ibu_b,name_b,nat_b)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (item["race_id"],item.get("race_desc",""),item.get("race_fmt",""),
                 item.get("race_date",""),item["ibu_a"],item.get("name_a",""),item.get("nat_a",""),
                 item["ibu_b"],item.get("name_b",""),item.get("nat_b","")))
            rid = cur.fetchone()[0]; conn.commit(); return rid
        else:
            cur.execute("""INSERT INTO biathlon_watchlist
                (race_id,race_desc,race_fmt,race_date,ibu_a,name_a,nat_a,ibu_b,name_b,nat_b)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (item["race_id"],item.get("race_desc",""),item.get("race_fmt",""),
                 item.get("race_date",""),item["ibu_a"],item.get("name_a",""),item.get("nat_a",""),
                 item["ibu_b"],item.get("name_b",""),item.get("nat_b","")))
            rid = cur.lastrowid; conn.commit(); return rid
    finally:
        conn.close()

def get_biathlon_watchlist() -> list:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM biathlon_watchlist ORDER BY race_date, race_id")
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()

def delete_biathlon_watchlist(item_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        cur.execute(f"DELETE FROM biathlon_watchlist WHERE id={p}", (item_id,))
        conn.commit()
    finally:
        conn.close()

def update_biathlon_watchlist_result(item_id: int, result: int):
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        cur.execute(f"UPDATE biathlon_watchlist SET result={p} WHERE id={p}", (result, item_id))
        conn.commit()
    finally:
        conn.close()
