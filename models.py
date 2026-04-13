import sqlite3
import os
import json
from datetime import datetime, timedelta, timezone

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

        CREATE TABLE IF NOT EXISTS checklists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            frequency TEXT NOT NULL DEFAULT 'daily',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS checklist_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checklist_id INTEGER NOT NULL,
            period_key TEXT NOT NULL,
            completed_at TEXT DEFAULT (datetime('now')),
            UNIQUE(checklist_id, period_key)
        );

        CREATE TABLE IF NOT EXISTS forms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            fields TEXT NOT NULL DEFAULT '[]',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS form_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            form_id INTEGER NOT NULL,
            data TEXT NOT NULL DEFAULT '{}',
            submitted_at TEXT DEFAULT (datetime('now')),
            submitted_by TEXT DEFAULT ''
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


# ---- Checklists ----

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist():
    return datetime.now(IST)


def _period_key(frequency):
    now = _now_ist()
    if frequency == "daily":
        return now.strftime("%Y-%m-%d")
    elif frequency == "weekly":
        start = now - timedelta(days=now.weekday())
        return f"W{start.strftime('%Y-%m-%d')}"
    else:
        return now.strftime("%Y-%m")


def add_checklist(title, frequency):
    conn = get_db()
    conn.execute(
        "INSERT INTO checklists (title, frequency) VALUES (?, ?)",
        (title, frequency)
    )
    conn.commit()
    conn.close()


def get_checklists():
    conn = get_db()
    rows = conn.execute("SELECT * FROM checklists ORDER BY frequency, title").fetchall()
    result = []
    for r in rows:
        pk = _period_key(r["frequency"])
        done = conn.execute(
            "SELECT id FROM checklist_completions WHERE checklist_id = ? AND period_key = ?",
            (r["id"], pk)
        ).fetchone()
        result.append({
            "id": r["id"], "title": r["title"], "frequency": r["frequency"],
            "done": done is not None, "period_key": pk
        })
    conn.close()
    return result


def toggle_checklist(checklist_id):
    conn = get_db()
    row = conn.execute("SELECT frequency FROM checklists WHERE id = ?", (checklist_id,)).fetchone()
    if not row:
        conn.close()
        return
    pk = _period_key(row["frequency"])
    existing = conn.execute(
        "SELECT id FROM checklist_completions WHERE checklist_id = ? AND period_key = ?",
        (checklist_id, pk)
    ).fetchone()
    if existing:
        conn.execute("DELETE FROM checklist_completions WHERE id = ?", (existing["id"],))
    else:
        conn.execute(
            "INSERT INTO checklist_completions (checklist_id, period_key) VALUES (?, ?)",
            (checklist_id, pk)
        )
    conn.commit()
    conn.close()


def delete_checklist(checklist_id):
    conn = get_db()
    conn.execute("DELETE FROM checklists WHERE id = ?", (checklist_id,))
    conn.execute("DELETE FROM checklist_completions WHERE checklist_id = ?", (checklist_id,))
    conn.commit()
    conn.close()


# ---- Forms ----

def add_form(title, slug, fields):
    conn = get_db()
    conn.execute(
        "INSERT INTO forms (title, slug, fields) VALUES (?, ?, ?)",
        (title, slug, json.dumps(fields))
    )
    conn.commit()
    conn.close()


def get_forms():
    conn = get_db()
    rows = conn.execute("SELECT * FROM forms ORDER BY id DESC").fetchall()
    result = []
    for r in rows:
        count = conn.execute(
            "SELECT COUNT(*) as c FROM form_responses WHERE form_id = ?", (r["id"],)
        ).fetchone()["c"]
        result.append({
            "id": r["id"], "title": r["title"], "slug": r["slug"],
            "fields": json.loads(r["fields"]), "response_count": count,
            "created_at": r["created_at"]
        })
    conn.close()
    return result


def get_form_by_slug(slug):
    conn = get_db()
    row = conn.execute("SELECT * FROM forms WHERE slug = ?", (slug,)).fetchone()
    conn.close()
    if row:
        return {
            "id": row["id"], "title": row["title"], "slug": row["slug"],
            "fields": json.loads(row["fields"])
        }
    return None


def get_form_by_id(form_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM forms WHERE id = ?", (form_id,)).fetchone()
    conn.close()
    if row:
        return {
            "id": row["id"], "title": row["title"], "slug": row["slug"],
            "fields": json.loads(row["fields"])
        }
    return None


def add_form_response(form_id, data, submitted_by=""):
    conn = get_db()
    conn.execute(
        "INSERT INTO form_responses (form_id, data, submitted_by) VALUES (?, ?, ?)",
        (form_id, json.dumps(data), submitted_by)
    )
    conn.commit()
    conn.close()


def get_form_responses(form_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM form_responses WHERE form_id = ? ORDER BY id DESC", (form_id,)
    ).fetchall()
    result = []
    for r in rows:
        result.append({
            "id": r["id"], "data": json.loads(r["data"]),
            "submitted_at": r["submitted_at"], "submitted_by": r["submitted_by"]
        })
    conn.close()
    return result


def delete_form(form_id):
    conn = get_db()
    conn.execute("DELETE FROM forms WHERE id = ?", (form_id,))
    conn.execute("DELETE FROM form_responses WHERE form_id = ?", (form_id,))
    conn.commit()
    conn.close()
