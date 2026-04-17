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

        CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_jid TEXT NOT NULL,
            group_name TEXT DEFAULT '',
            sender_phone TEXT DEFAULT '',
            sender_name TEXT DEFAULT '',
            body TEXT DEFAULT '',
            timestamp TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_jid TEXT NOT NULL,
            group_name TEXT DEFAULT '',
            time_range TEXT DEFAULT '',
            summary TEXT DEFAULT '',
            message_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            group_jid TEXT DEFAULT '',
            group_name TEXT DEFAULT '',
            sender_phone TEXT DEFAULT '',
            sender_name TEXT DEFAULT '',
            message TEXT DEFAULT '',
            additional_message TEXT DEFAULT '',
            status TEXT DEFAULT '',
            last_sent TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
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
    # Add tag column if missing (migration for existing DBs)
    try:
        conn.execute("ALTER TABLE tasks ADD COLUMN tag TEXT DEFAULT ''")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
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

def add_task(date_str, message, phone="", person="", status="", tag=""):
    person = person.strip()
    tag = tag.strip()
    if phone and "@" not in phone:
        phone = _clean_phone(phone)
    conn = get_db()
    # Auto-resolve person from contacts if phone provided and no person given
    # Skip for own phone number (tasks from self should stay unassigned)
    my_phone = os.environ.get("MY_PHONE", "")
    if phone and not person and not (my_phone and (my_phone in phone or phone in my_phone)):
        row = conn.execute(
            "SELECT name FROM contacts WHERE phone = ?", (phone,)
        ).fetchone()
        if row:
            person = row["name"]
    conn.execute(
        "INSERT INTO tasks (date, person, phone, message, status, tag) VALUES (?, ?, ?, ?, ?, ?)",
        (date_str, person, phone, message, status, tag)
    )
    conn.commit()
    conn.close()
    log_event("TASK_ADDED", f"[{tag}] {message[:70]}" if tag else message[:80])


def get_tasks():
    conn = get_db()
    rows = conn.execute("SELECT * FROM tasks ORDER BY id DESC").fetchall()
    conn.close()
    return rows


def update_task(task_id, **kwargs):
    if "phone" in kwargs and kwargs["phone"] and "@" not in kwargs["phone"]:
        kwargs["phone"] = _clean_phone(kwargs["phone"])
    conn = get_db()
    allowed = {"person", "phone", "status", "message", "additional_message", "last_sent"}
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
            (g.get("name", "Unknown Group").strip(), g.get("id") or g.get("jid", ""))
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

def _clean_phone(phone):
    """Strip invisible unicode and non-digit chars from phone numbers."""
    import re
    return re.sub(r"[^\d]", "", phone.strip())


def add_contact(name, phone):
    name = name.strip()
    phone = _clean_phone(phone)
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


# ---- Group Messages ----

def add_group_message(group_jid, group_name, sender_phone, sender_name, body, timestamp):
    conn = get_db()
    conn.execute(
        "INSERT INTO group_messages (group_jid, group_name, sender_phone, sender_name, body, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (group_jid, group_name.strip(), sender_phone, sender_name.strip(), body, timestamp)
    )
    # Keep only last 7 days of messages
    conn.execute("""
        DELETE FROM group_messages WHERE timestamp < datetime('now', '-7 days')
    """)
    conn.commit()
    conn.close()


def get_group_messages(group_jid, hours=1):
    """Get messages from a group within the last N hours."""
    conn = get_db()
    ist_now = _now_ist()
    cutoff = (ist_now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT * FROM group_messages WHERE group_jid = ? AND timestamp >= ? ORDER BY timestamp ASC",
        (group_jid, cutoff)
    ).fetchall()
    conn.close()
    return rows


# ---- Summaries ----

def add_summary(group_jid, group_name, time_range, summary, message_count):
    conn = get_db()
    conn.execute(
        "INSERT INTO summaries (group_jid, group_name, time_range, summary, message_count) VALUES (?, ?, ?, ?, ?)",
        (group_jid, group_name, time_range, summary, message_count)
    )
    conn.commit()
    conn.close()


def get_summaries(limit=50):
    conn = get_db()
    rows = conn.execute("SELECT * FROM summaries ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def delete_form(form_id):
    conn = get_db()
    conn.execute("DELETE FROM forms WHERE id = ?", (form_id,))
    conn.execute("DELETE FROM form_responses WHERE form_id = ?", (form_id,))
    conn.commit()
    conn.close()


# ---- Approvals ----

def add_approval(date_str, message, group_jid="", group_name="",
                 sender_phone="", sender_name=""):
    sender_phone = _clean_phone(sender_phone) if sender_phone else ""
    # Resolve sender name from contacts if not provided
    if sender_phone and not sender_name:
        conn = get_db()
        row = conn.execute(
            "SELECT name FROM contacts WHERE phone = ?", (sender_phone,)
        ).fetchone()
        if row:
            sender_name = row["name"]
        conn.close()
    conn = get_db()
    conn.execute(
        """INSERT INTO approvals
           (date, group_jid, group_name, sender_phone, sender_name, message)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (date_str, group_jid, group_name.strip(), sender_phone,
         sender_name.strip(), message)
    )
    conn.commit()
    conn.close()
    log_event("APPROVAL_ADDED", f"{sender_name} @ {group_name}: {message[:70]}")


def get_approvals():
    conn = get_db()
    rows = conn.execute("SELECT * FROM approvals ORDER BY id DESC").fetchall()
    conn.close()
    return rows


def update_approval(approval_id, **kwargs):
    conn = get_db()
    allowed = {"sender_name", "sender_phone", "group_name", "group_jid",
               "message", "additional_message", "status", "last_sent"}
    sets = []
    vals = []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if sets:
        vals.append(approval_id)
        conn.execute(
            f"UPDATE approvals SET {', '.join(sets)} WHERE id = ?", vals
        )
        conn.commit()
    conn.close()


def delete_approval(approval_id):
    conn = get_db()
    conn.execute("DELETE FROM approvals WHERE id = ?", (approval_id,))
    conn.commit()
    conn.close()
