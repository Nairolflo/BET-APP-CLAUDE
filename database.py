# database.py — shim de compatibilité → core/database.py
from core.database import *  # noqa
from core.database import (
    get_connection, is_postgres, ph, row_to_dict, rows_to_dicts,
    init_db, save_bet, get_all_bets, update_bet_result,
    is_bet_notified, mark_bet_notified, get_pending_bets,
    save_team_stats, get_team_stats, get_stats,
    delete_today_pending_bets, get_unique_bets, reset_all_bets,
    get_stats_by_market, get_stats_by_league_detailed,
    get_bete_noire_bets, get_roi_over_time, get_streak,
    get_h2h_cache, set_h2h_cache, get_h2h_cache_status,
)

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


def update_biathlon_watchlist_result(item_id: int, result: int):
    conn = get_connection()
    try:
        cur = conn.cursor(); p = ph()
        cur.execute(f"UPDATE biathlon_watchlist SET result={p} WHERE id={p}", (result, item_id))
        conn.commit()
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
