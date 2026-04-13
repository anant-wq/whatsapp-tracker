import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracker.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            person TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            message TEXT DEFAULT '',
            status TEXT DEFAULT '',
            additional_message TEXT DEFAULT '',
            last_sent TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS groups_ (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT DEFAULT '',
            group_jid TEXT UNIQUE DEFAULT '',
            last_sent TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            phone TEXT UNIQUE DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS webhook_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now')),
            event TEXT DEFAULT '',
            details TEXT DEFAULT ''
        );
    """)
    conn.commit()
    conn.close()


# ---- Logging ----

def log_event(event, details):
    conn = get_db()
    conn.execute(
        "INSERT INTO webhook_log (event, details) VALUES (?, ?)",
        (event, str(details)[:50000])
    )
    # Trim to 500 entries
    conn.execute("""
        DELETE FROM webhook_log WHERE id NOT IN (
            SELECT id FROM webhook_log ORDER BY id DESC LIMIT 500
        )
    """)
    conn.commit()
    conn.close()


def get_logs(limit=200):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM webhook_log ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return rows


# ---- Tasks ----

def add_task(date_str, message, phone="", person="", status=""):
    conn = get_db()
    # Auto-resolve person from contacts if phone provided and no person given
    if phone and not person:
        row = conn.execute(
            "SELECT name FROM contacts WHERE phone = ?", (phone,)
        ).fetchone()
        if row:
            person = row["name"]
    conn.execute(
        "INSERT INTO tasks (date, person, phone, message, status) VALUES (?, ?, ?, ?, ?)",
        (date_str, person, phone, message, status)
    )
    conn.commit()
    conn.close()
    log_event("TASK_ADDED", message[:80])


def get_tasks():
    conn = get_db()
    rows = conn.execute("SELECT * FROM tasks ORDER BY id DESC").fetchall()
    conn.close()
    return rows


def update_task(task_id, **kwargs):
    conn = get_db()
    allowed = {"person", "phone", "status", "additional_message", "last_sent"}
    sets = []
    vals = []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if sets:
        vals.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
    conn.close()


def delete_task(task_id):
    conn = get_db()
    conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()


# ---- Groups ----

def upsert_groups(groups_list):
    """Replace all groups with fresh data from API."""
    conn = get_db()
    conn.execute("DELETE FROM groups_")
    for g in groups_list:
        conn.execute(
            "INSERT INTO groups_ (group_name, group_jid) VALUES (?, ?)",
            (g.get("name", "Unknown Group"), g.get("id") or g.get("jid", ""))
        )
    conn.commit()
    conn.close()


def get_groups():
    conn = get_db()
    rows = conn.execute("SELECT * FROM groups_ ORDER BY group_name").fetchall()
    conn.close()
    return rows


def update_group(group_id, **kwargs):
    conn = get_db()
    allowed = {"last_sent"}
    sets = []
    vals = []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if sets:
        vals.append(group_id)
        conn.execute(f"UPDATE groups_ SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
    conn.close()


def get_group_name_by_jid(jid):
    conn = get_db()
    row = conn.execute(
        "SELECT group_name FROM groups_ WHERE group_jid = ?", (jid,)
    ).fetchone()
    conn.close()
    return row["group_name"] if row else jid


# ---- Contacts ----

def add_contact(name, phone):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO contacts (name, phone) VALUES (?, ?)",
        (name, phone)
    )
    conn.commit()
    conn.close()


def get_contacts():
    conn = get_db()
    rows = conn.execute("SELECT * FROM contacts ORDER BY name").fetchall()
    conn.close()
    return rows


def delete_contact(contact_id):
    conn = get_db()
    conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()
