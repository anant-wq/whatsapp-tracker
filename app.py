import os
import re
import json
import atexit
import secrets
from datetime import datetime, timezone, timedelta
from functools import wraps

import requests
from flask import (
    Flask, request, redirect, url_for, session,
    render_template, jsonify, flash
)
from authlib.integrations.flask_client import OAuth

from apscheduler.schedulers.background import BackgroundScheduler

import models


# ---- Prefix middleware (app lives at /whatsapp/ behind nginx) ----

class PrefixMiddleware:
    def __init__(self, app, prefix=""):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        environ["SCRIPT_NAME"] = self.prefix
        path = environ.get("PATH_INFO", "")
        if path.startswith(self.prefix):
            environ["PATH_INFO"] = path[len(self.prefix):]
        return self.app(environ, start_response)


# ---- Config ----

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.permanent_session_lifetime = timedelta(days=30)
app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix=os.environ.get("APP_PREFIX", "/whatsapp"))

WASENDER_BASE = "https://api.wasenderapi.com"
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY", "")
MY_PHONE = os.environ.get("MY_PHONE", "918447731703")
ALLOWED_EMAIL = os.environ.get("ALLOWED_EMAIL", "anant@xpertpack.in")
MY_NAME = os.environ.get("MY_NAME", "Anant Khirbat")

# ---- OAuth ----

oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


@app.before_request
def make_session_permanent():
    session.permanent = True


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if os.environ.get("SKIP_AUTH"):
            session["user"] = {"email": ALLOWED_EMAIL, "name": MY_NAME}
        if not session.get("user"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ---- Auth Routes ----

@app.route("/login")
def login():
    if session.get("user"):
        return redirect(url_for("tasks_page"))
    return render_template("login.html")


@app.route("/auth/login")
def auth_login():
    redirect_uri = url_for("auth_callback", _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route("/auth/callback")
def auth_callback():
    token = google.authorize_access_token()
    user_info = token.get("userinfo") or google.userinfo()
    email = user_info.get("email", "")

    if email.lower() != ALLOWED_EMAIL.lower():
        flash(f"Access denied for {email}. Only {ALLOWED_EMAIL} is allowed.", "error")
        return redirect(url_for("login"))

    session.permanent = True
    session["user"] = {"email": email, "name": user_info.get("name", email)}
    return redirect(url_for("tasks_page"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---- Webhook (no auth — WaSenderAPI calls this) ----

@app.route("/webhook", methods=["GET"])
def webhook_get():
    return jsonify({
        "status": "active",
        "service": "WhatsApp Task Tracker",
        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
    })


@app.route("/webhook", methods=["POST"])
def webhook_post():
    try:
        raw = request.get_data(as_text=True)
        data = json.loads(raw)
        event = data.get("event", "")
        models.log_event(event, raw[:3000])

        if "message" not in event:
            return jsonify({"status": "ok", "message": f"ignored event: {event}"})

        inner = data.get("data", {})
        msgs = inner.get("messages", {})
        key = msgs.get("key", {})

        body = msgs.get("messageBody", "")
        if not body and msgs.get("message"):
            body = msgs["message"].get("conversation", "")
            if not body and msgs["message"].get("extendedTextMessage"):
                body = msgs["message"]["extendedTextMessage"].get("text", "")

        from_me = key.get("fromMe") is True
        chat_jid = key.get("remoteJid") or msgs.get("remoteJid", "")
        sender_phone = (
            key.get("cleanedSenderPn")
            or key.get("cleanedParticipantPn")
            or key.get("senderPn", "")
        )
        sender_phone = re.sub(r"@.*", "", sender_phone)
        sender_phone = re.sub(r"[^0-9]", "", sender_phone)
        is_group = "@g.us" in chat_jid

        models.log_event("PARSED", f"fromMe={from_me} | body={body[:80]} | sender={sender_phone} | isGroup={is_group}")

        # Store all group messages for summaries
        if is_group and body:
            ts_raw = data.get("timestamp") or msgs.get("messageTimestamp")
            ts_str = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(ts_raw, (int, float)):
                ts_val = ts_raw if ts_raw > 9999999999 else ts_raw * 1000
                ts_str = datetime.fromtimestamp(ts_val / 1000, tz=timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")
            group_name = models.get_group_name_by_jid(chat_jid)
            # Resolve sender name from contacts
            sender_name = ""
            if sender_phone:
                contacts = models.get_contacts()
                for c in contacts:
                    if c["phone"] == sender_phone:
                        sender_name = c["name"]
                        break
            if not sender_name:
                sender_name = sender_phone
            models.add_group_message(chat_jid, group_name, sender_phone, sender_name, body, ts_str)

        body_lower = body.lower()
        # Extract person hashtags (any #name that isn't #todo/#task)
        person_tags = [
            tag for tag in re.findall(r"#(\w+)", body_lower)
            if tag not in ("todo", "task")
        ]
        has_todo = "#todo" in body_lower or "#task" in body_lower

        # Group messages with #todo/#task
        if is_group and body and has_todo:
            is_my_phone = (MY_PHONE in sender_phone or sender_phone in MY_PHONE)
            if is_my_phone:
                ts = data.get("timestamp") or msgs.get("messageTimestamp")
                date_str = _parse_timestamp(ts)
                group_name = models.get_group_name_by_jid(chat_jid)
                if person_tags:
                    for tag in person_tags:
                        models.add_task(
                            date_str=date_str, message=body, phone=chat_jid,
                            person=MY_NAME, tag=tag.capitalize()
                        )
                    return jsonify({"status": "ok", "message": f"my task added tagged {', '.join(person_tags)}"})
                else:
                    models.add_task(
                        date_str=date_str, message=body, phone=chat_jid,
                        person=group_name
                    )
                    return jsonify({"status": "ok", "message": "group todo added"})

        # Direct messages sent BY me — check for person hashtags or #todo
        if from_me and not is_group and body:
            if person_tags or has_todo:
                ts = data.get("timestamp") or msgs.get("messageTimestamp")
                date_str = _parse_timestamp(ts)
                if person_tags:
                    for tag in person_tags:
                        models.add_task(
                            date_str=date_str, message=body, phone="",
                            person=MY_NAME, tag=tag.capitalize()
                        )
                    return jsonify({"status": "ok", "message": f"my task added tagged {', '.join(person_tags)}"})
                else:
                    models.add_task(date_str=date_str, message=body, phone="",
                                    person=MY_NAME)
                    return jsonify({"status": "ok", "message": "my task added"})
            return jsonify({"status": "ok", "message": "skipped - fromMe, no tags"})

        if from_me:
            return jsonify({"status": "ok", "message": "skipped - fromMe"})
        if is_group:
            return jsonify({"status": "ok", "message": "skipped - group"})
        if MY_PHONE not in sender_phone and sender_phone not in MY_PHONE:
            return jsonify({"status": "ok", "message": "skipped - not my number"})
        if not body:
            return jsonify({"status": "ok", "message": "skipped - empty"})

        ts = data.get("timestamp") or msgs.get("messageTimestamp")
        date_str = _parse_timestamp(ts)
        if person_tags:
            for tag in person_tags:
                models.add_task(
                    date_str=date_str, message=body, phone=sender_phone,
                    person=MY_NAME, tag=tag.capitalize()
                )
            return jsonify({"status": "ok", "message": f"my task added tagged {', '.join(person_tags)}"})
        models.add_task(date_str=date_str, message=body, phone=sender_phone)
        return jsonify({"status": "ok", "message": "task added"})

    except Exception as e:
        models.log_event("ERROR", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def _parse_timestamp(ts):
    if ts is None:
        return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
    if isinstance(ts, (int, float)):
        ts = ts if ts > 9999999999 else ts * 1000
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")


# ---- Helper: build people list (contacts + groups combined) ----

def _get_people():
    contacts = models.get_contacts()
    groups = models.get_groups()
    people = [{"name": c["name"], "phone": c["phone"]} for c in contacts]
    people += [{"name": g["group_name"], "phone": g["group_jid"]} for g in groups]
    return people


# ---- Tasks ----

@app.route("/")
@login_required
def index():
    return redirect(url_for("tasks_page"))


@app.route("/tasks")
@login_required
def tasks_page():
    all_tasks = models.get_tasks()
    my_name = session.get("user", {}).get("name", "")
    tasks = [t for t in all_tasks if t["person"] != my_name]
    people = _get_people()
    return render_template("tasks.html", tasks=tasks, people=people)


@app.route("/my-tasks")
@login_required
def my_tasks_page():
    all_tasks = models.get_tasks()
    my_name = session.get("user", {}).get("name", "")
    tasks = [t for t in all_tasks if t["person"] == my_name]
    # Group tagged tasks by tag for subheadings
    tagged = {}
    untagged = []
    for t in tasks:
        tag = t["tag"] if "tag" in t.keys() else ""
        if tag:
            tagged.setdefault(tag, []).append(t)
        else:
            untagged.append(t)
    # Sort tag groups alphabetically
    tagged = dict(sorted(tagged.items()))
    people = _get_people()
    return render_template("my_tasks.html", tasks=untagged, tagged_tasks=tagged,
                           people=people, my_name=my_name)


@app.route("/tasks/send/<int:task_id>", methods=["POST"])
@login_required
def send_reminder(task_id):
    tasks = models.get_tasks()
    task = None
    for t in tasks:
        if t["id"] == task_id:
            task = t
            break
    if not task:
        flash("Task not found", "error")
        return redirect(url_for("tasks_page"))

    phone = task["phone"]
    message = task["message"]
    additional = request.form.get("additional_message", "").strip()

    if not phone or len(phone) < 5:
        models.update_task(task_id, last_sent="ERROR: No phone")
        flash("No phone number", "error")
        return redirect(url_for("tasks_page"))

    full_message = (
        f"{additional}\n\n--- Earlier context ---\n{message}"
        if additional else message
    )

    success = _send_whatsapp(phone, full_message)
    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    last_sent = f"Sent {now}" if success else f"Failed {now}"
    models.update_task(
        task_id,
        last_sent=last_sent,
        additional_message=additional
    )
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok" if success else "error", "last_sent": last_sent})
    flash("Message sent!" if success else "Send failed", "success" if success else "error")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/update/<int:task_id>", methods=["POST"])
@login_required
def update_task(task_id):
    person = request.form.get("person", "")
    phone = request.form.get("phone", "")
    status = request.form.get("status", "")
    message = request.form.get("message", "")
    additional = request.form.get("additional_message", "")
    updates = dict(person=person, phone=phone, status=status, additional_message=additional)
    if message:
        updates["message"] = message
    models.update_task(task_id, **updates)
    # Return JSON for fetch requests, redirect for form submissions
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok"})
    flash("Task updated", "success")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/delete/<int:task_id>", methods=["POST"])
@login_required
def delete_task(task_id):
    models.delete_task(task_id)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok"})
    flash("Task deleted", "success")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/bulk-delete", methods=["POST"])
@login_required
def bulk_delete_tasks():
    ids = request.form.getlist("task_ids")
    count = 0
    for tid in ids:
        models.delete_task(int(tid))
        count += 1
    flash(f"Deleted {count} task(s)", "success")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/bulk-send", methods=["POST"])
@login_required
def bulk_send_tasks():
    ids = request.form.getlist("task_ids")
    tasks = models.get_tasks()
    task_map = {t["id"]: t for t in tasks}
    sent = 0
    failed = 0
    for tid in ids:
        task = task_map.get(int(tid))
        if not task:
            continue
        phone = task["phone"]
        message = task["message"]
        additional = task["additional_message"] or ""
        if not phone or len(phone) < 5:
            models.update_task(int(tid), last_sent="ERROR: No phone")
            failed += 1
            continue
        full_message = (
            f"{additional}\n\n--- Earlier context ---\n{message}"
            if additional else message
        )
        success = _send_whatsapp(phone, full_message)
        now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
        models.update_task(
            int(tid),
            last_sent=f"Sent {now}" if success else f"Failed {now}"
        )
        if success:
            sent += 1
        else:
            failed += 1
    flash(f"Sent {sent}, failed {failed} of {len(ids)} selected", "success" if failed == 0 else "error")
    return redirect(url_for("tasks_page"))


# ---- Contacts (merged with Groups) ----

@app.route("/contacts")
@login_required
def contacts_page():
    contacts = models.get_contacts()
    groups = models.get_groups()
    return render_template("contacts.html", contacts=contacts, groups=groups)


@app.route("/contacts/add", methods=["POST"])
@login_required
def add_contact():
    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    if name and phone:
        models.add_contact(name, phone)
        flash("Contact added", "success")
    else:
        flash("Name and phone required", "error")
    return redirect(url_for("contacts_page"))


@app.route("/contacts/bulk", methods=["POST"])
@login_required
def bulk_import_contacts():
    raw = request.form.get("bulk", "").strip()
    if not raw:
        flash("Nothing to import", "error")
        return redirect(url_for("contacts_page"))
    count = 0
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in re.split(r"\t", line) if p.strip()]
        if len(parts) < 2:
            parts = [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]
        if len(parts) < 2:
            continue
        name = parts[0]
        phone = ""
        for p in reversed(parts):
            digits = re.sub(r"[^0-9]", "", p)
            if len(digits) >= 7:
                phone = digits
                break
        if not phone:
            continue
        if len(phone) == 10:
            phone = "91" + phone
        models.add_contact(name, phone)
        count += 1
    flash(f"Imported {count} contact(s)", "success")
    return redirect(url_for("contacts_page"))


@app.route("/contacts/delete/<int:contact_id>", methods=["POST"])
@login_required
def delete_contact(contact_id):
    models.delete_contact(contact_id)
    flash("Contact deleted", "success")
    return redirect(url_for("contacts_page"))


# ---- Groups (fetch + send kept, but page merged into contacts) ----

@app.route("/groups")
@login_required
def groups_page():
    return redirect(url_for("contacts_page"))


@app.route("/groups/fetch", methods=["POST"])
@login_required
def fetch_groups():
    if not WASENDER_API_KEY:
        flash("No API key configured", "error")
        return redirect(url_for("contacts_page"))

    try:
        resp = requests.get(
            f"{WASENDER_BASE}/api/groups",
            headers={
                "Authorization": f"Bearer {WASENDER_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=30
        )
        models.log_event(f"FETCH_GROUPS ({resp.status_code})", resp.text[:3000])

        if resp.status_code < 200 or resp.status_code >= 300:
            flash(f"Failed to fetch groups (HTTP {resp.status_code})", "error")
            return redirect(url_for("contacts_page"))

        data = resp.json()
        groups = data.get("data") or data if isinstance(data, list) else data.get("data", [])
        if not isinstance(groups, list):
            flash("Unexpected response format", "error")
            return redirect(url_for("contacts_page"))

        models.upsert_groups(groups)
        flash(f"Found {len(groups)} group(s)", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")

    return redirect(url_for("contacts_page"))


@app.route("/groups/send/<int:group_id>", methods=["POST"])
@login_required
def send_group_message(group_id):
    groups = models.get_groups()
    group = None
    for g in groups:
        if g["id"] == group_id:
            group = g
            break
    if not group:
        flash("Group not found", "error")
        return redirect(url_for("contacts_page"))

    message = request.form.get("message", "").strip()
    if not message:
        flash("No message to send", "error")
        return redirect(url_for("contacts_page"))

    jid = group["group_jid"]
    success = _send_whatsapp(jid, message)
    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    models.update_group(group_id, last_sent=f"Sent {now}" if success else f"Failed {now}")
    flash("Message sent!" if success else "Send failed", "success" if success else "error")
    return redirect(url_for("contacts_page"))


# ---- Checklists ----

@app.route("/checklists")
@login_required
def checklists_page():
    items = models.get_checklists()
    daily = [i for i in items if i["frequency"] == "daily"]
    weekly = [i for i in items if i["frequency"] == "weekly"]
    monthly = [i for i in items if i["frequency"] == "monthly"]
    return render_template("checklists.html", daily=daily, weekly=weekly, monthly=monthly)


@app.route("/checklists/add", methods=["POST"])
@login_required
def add_checklist():
    title = request.form.get("title", "").strip()
    frequency = request.form.get("frequency", "daily")
    if title:
        models.add_checklist(title, frequency)
        flash("Item added", "success")
    else:
        flash("Title required", "error")
    return redirect(url_for("checklists_page"))


@app.route("/checklists/toggle/<int:item_id>", methods=["POST"])
@login_required
def toggle_checklist(item_id):
    models.toggle_checklist(item_id)
    return redirect(url_for("checklists_page"))


@app.route("/checklists/delete/<int:item_id>", methods=["POST"])
@login_required
def delete_checklist(item_id):
    models.delete_checklist(item_id)
    flash("Item deleted", "success")
    return redirect(url_for("checklists_page"))


# ---- Forms ----

@app.route("/forms")
@login_required
def forms_page():
    forms = models.get_forms()
    return render_template("forms.html", forms=forms)


@app.route("/forms/create", methods=["POST"])
@login_required
def create_form():
    title = request.form.get("title", "").strip()
    fields_raw = request.form.get("fields", "").strip()
    if not title or not fields_raw:
        flash("Title and fields required", "error")
        return redirect(url_for("forms_page"))

    fields = []
    for line in fields_raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        label = parts[0]
        ftype = parts[1] if len(parts) > 1 else "text"
        required = "required" in (parts[2].lower() if len(parts) > 2 else "required")
        fields.append({"label": label, "type": ftype, "required": required})

    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    # Ensure unique slug
    existing = models.get_form_by_slug(slug)
    if existing:
        slug = slug + "-" + secrets.token_hex(3)

    models.add_form(title, slug, fields)
    flash("Form created!", "success")
    return redirect(url_for("forms_page"))


@app.route("/forms/delete/<int:form_id>", methods=["POST"])
@login_required
def delete_form(form_id):
    models.delete_form(form_id)
    flash("Form deleted", "success")
    return redirect(url_for("forms_page"))


@app.route("/forms/responses/<int:form_id>")
@login_required
def form_responses(form_id):
    form = models.get_form_by_id(form_id)
    if not form:
        flash("Form not found", "error")
        return redirect(url_for("forms_page"))
    responses = models.get_form_responses(form_id)
    return render_template("form_responses.html", form=form, responses=responses)


@app.route("/f/<slug>", methods=["GET", "POST"])
def public_form(slug):
    form = models.get_form_by_slug(slug)
    if not form:
        return "Form not found", 404
    if request.method == "POST":
        data = {}
        for field in form["fields"]:
            data[field["label"]] = request.form.get(field["label"], "")
        name = request.form.get("_name", "")
        models.add_form_response(form["id"], data, submitted_by=name)
        return render_template("form_thanks.html", form=form)
    return render_template("form_public.html", form=form)


# ---- Summaries ----

MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_MODEL = "mistral-large-latest"


def _generate_summary(group_jid, hours=1):
    """Generate a summary for a group (no Flask request context needed)."""
    messages = models.get_group_messages(group_jid, hours=hours)
    if not messages:
        return None

    conversation = ""
    for m in messages:
        conversation += f"[{m['timestamp']}] {m['sender_name']}: {m['body']}\n"

    group_name = models.get_group_name_by_jid(group_jid)

    prompt = f"""You are an operations analyst at a corrugation/packaging company (XpertPack).
Summarize this WhatsApp group conversation from the "{group_name}" group.

Extract and organize into these sections:
1. **Key Operational Issues** - production problems, delays, quality issues
2. **Action Items** - tasks that need to be done, who needs to do what
3. **Pending Decisions** - things waiting for approval or decision
4. **Dispatch/Logistics Updates** - any movement, truck, delivery updates
5. **Escalations** - urgent matters that need immediate attention

Be concise. Use bullet points. Skip sections that have no relevant content.
If messages are in Hindi/Hinglish, still summarize in English.

--- CONVERSATION ({len(messages)} messages, last {hours}h) ---
{conversation}
--- END ---"""

    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            json={
                "model": MISTRAL_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 2000
            },
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=60
        )
        if resp.status_code == 200:
            result = resp.json()
            summary_text = result["choices"][0]["message"]["content"]
        else:
            summary_text = f"Mistral API error ({resp.status_code}): {resp.text[:500]}"
    except Exception as e:
        summary_text = f"Error calling Mistral: {str(e)}"

    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    time_range = f"Last {hours}h as of {now}"
    models.add_summary(group_jid, group_name, time_range, summary_text, len(messages))
    return summary_text


def _auto_generate_summaries():
    """Scheduled job: generate hourly summaries for groups matching 'Dispatch' and send to the group."""
    groups = models.get_groups()
    for g in groups:
        name = g["group_name"]
        jid = g["group_jid"]
        if "dispatch" in name.lower():
            summary = _generate_summary(jid, hours=2)
            if summary:
                _send_whatsapp(MY_PHONE + "@s.whatsapp.net", f"*Summary (last 2h) — {name}*\n\n{summary}")


# ---- Scheduler (gunicorn runs multiple workers; use a file lock to start only once) ----

_scheduler_started = False

def _start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    try:
        lock_path = "/tmp/whatsapp_tracker_scheduler.lock"
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
    except FileExistsError:
        # Check if the process that created the lock is still alive
        try:
            with open(lock_path) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)  # Check if process exists
            return  # Another worker already running scheduler
        except (ProcessLookupError, ValueError, FileNotFoundError):
            os.unlink(lock_path)
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(lock_fd, str(os.getpid()).encode())
            os.close(lock_fd)

    sched = BackgroundScheduler(daemon=True)
    sched.add_job(_auto_generate_summaries, "interval", hours=2, id="dispatch_summary_2h")
    sched.start()
    _scheduler_started = True

    def _cleanup_lock():
        try:
            os.unlink(lock_path)
        except FileNotFoundError:
            pass
    atexit.register(_cleanup_lock)

_start_scheduler()


@app.route("/summaries")
@login_required
def summaries_page():
    groups = models.get_groups()
    summaries = models.get_summaries()
    return render_template("summaries.html", groups=groups, summaries=summaries)


@app.route("/summaries/generate", methods=["POST"])
@login_required
def generate_summary():
    group_jid = request.form.get("group_jid", "")
    hours = int(request.form.get("hours", 1))

    if not group_jid:
        flash("Select a group", "error")
        return redirect(url_for("summaries_page"))

    result = _generate_summary(group_jid, hours=hours)
    if result is None:
        flash(f"No messages found in the last {hours} hour(s)", "error")
    else:
        flash("Summary generated!", "success")
    return redirect(url_for("summaries_page"))


# ---- Webhook Log ----

@app.route("/log")
@login_required
def log_page():
    logs = models.get_logs()
    return render_template("log.html", logs=logs)


# ---- WaSender Helper ----

def _send_whatsapp(to, text):
    if not WASENDER_API_KEY:
        models.log_event("SEND_ERROR", "No API key")
        return False
    # Clean phone number and ensure proper JID format
    to = to.strip()
    if "@" not in to:
        # Strip any non-digit characters (unicode markers, spaces, etc.)
        to = re.sub(r"[^\d]", "", to)
        to = to + "@s.whatsapp.net"
    else:
        # For group JIDs, just strip unicode junk around the JID
        to = re.sub(r"[^\x20-\x7E]", "", to).strip()
    try:
        resp = requests.post(
            f"{WASENDER_BASE}/api/send-message",
            json={"to": to, "text": text},
            headers={
                "Authorization": f"Bearer {WASENDER_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=30
        )
        models.log_event(f"API_RESPONSE ({resp.status_code})", resp.text[:3000])
        return 200 <= resp.status_code < 300
    except Exception as e:
        models.log_event("SEND_ERROR", str(e))
        return False


# ---- Init & Run ----

if __name__ == "__main__":
    models.init_db()
    app.run(host="0.0.0.0", port=5003, debug=True)
else:
    models.init_db()
