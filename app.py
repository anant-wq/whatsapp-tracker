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


# ---- Jinja filter: linkify + minimal markdown (bold, newlines) ----

from markupsafe import Markup, escape as _html_escape

_URL_RE = re.compile(r"(https?://[^\s<>\"'\)]+)")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)

@app.template_filter("linkify_md")
def linkify_md(text):
    if not text:
        return ""
    # HTML-escape first (safety)
    s = str(_html_escape(text))
    # Links
    s = _URL_RE.sub(
        lambda m: f'<a href="{m.group(1)}" target="_blank" rel="noopener">{m.group(1)}</a>',
        s
    )
    # Bold **x**
    s = _BOLD_RE.sub(r"<strong>\1</strong>", s)
    # Newlines
    s = s.replace("\n", "<br>")
    return Markup(s)

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

        # ---- Extract quoted-reply context (if this message is a reply) ----
        quoted_text, quoted_sender_phone = _extract_quoted(msgs)
        if quoted_text:
            quoted_sender_name = ""
            if quoted_sender_phone:
                for c in models.get_contacts():
                    if c["phone"] == quoted_sender_phone:
                        quoted_sender_name = c["name"]
                        break
            if not quoted_sender_name:
                quoted_sender_name = quoted_sender_phone or "Someone"
            # Prepend quoted context so downstream handlers capture it
            body = (f"↪ Replying to {quoted_sender_name}: \"{quoted_text}\"\n\n"
                    f"{body}")

        models.log_event("PARSED", f"fromMe={from_me} | body={body[:80]} | sender={sender_phone} | isGroup={is_group} | quoted={'Y' if quoted_text else 'N'}")

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
        # Extract person hashtags (any #name that isn't #todo/#task/#approval)
        person_tags = [
            tag for tag in re.findall(r"#(\w+)", body_lower)
            if tag not in ("todo", "task", "approval")
        ]
        has_todo = "#todo" in body_lower or "#task" in body_lower
        has_approval = "#approval" in body_lower

        # Capture #approval messages (from anyone, group or direct, not from me)
        if has_approval and body and not from_me:
            ts = data.get("timestamp") or msgs.get("messageTimestamp")
            date_str = _parse_timestamp(ts)
            if is_group:
                group_name = models.get_group_name_by_jid(chat_jid)
            else:
                group_name = "Direct Message"
            # Resolve sender name from contacts
            sender_name = ""
            if sender_phone:
                for c in models.get_contacts():
                    if c["phone"] == sender_phone:
                        sender_name = c["name"]
                        break
            if not sender_name:
                sender_name = sender_phone or "Unknown"
            models.add_approval(
                date_str=date_str, message=body,
                group_jid=chat_jid if is_group else "",
                group_name=group_name,
                sender_phone=sender_phone, sender_name=sender_name
            )
            # Don't return — let the message also flow through any other handlers below
            # (but only if there are no other tags, #approval messages typically don't need
            #  to be tracked as tasks too). Actually, fall-through is fine — #approval is
            #  an independent signal.

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


def _extract_quoted(msgs):
    """Pull quoted-reply text + original sender phone from a webhook message payload.
    Returns (text, phone). Empty strings if the message is not a reply."""
    try:
        message = msgs.get("message") or {}
        # contextInfo can live under several wrappers depending on the reply type
        ctx = None
        for wrapper in ("extendedTextMessage", "imageMessage", "videoMessage",
                        "documentMessage", "audioMessage", "stickerMessage"):
            sub = message.get(wrapper)
            if isinstance(sub, dict) and sub.get("contextInfo"):
                ctx = sub["contextInfo"]
                break
        if not ctx:
            return ("", "")
        quoted = ctx.get("quotedMessage") or {}
        if not quoted:
            return ("", "")

        # Try to get text from whichever sub-type the quoted message is
        text = (
            quoted.get("conversation")
            or (quoted.get("extendedTextMessage") or {}).get("text", "")
            or (quoted.get("imageMessage") or {}).get("caption", "")
            or (quoted.get("videoMessage") or {}).get("caption", "")
            or (quoted.get("documentMessage") or {}).get("caption", "")
            or ""
        )
        if not text:
            # Non-text quoted message (image/video/doc with no caption)
            if "imageMessage" in quoted:
                text = "[image]"
            elif "videoMessage" in quoted:
                text = "[video]"
            elif "documentMessage" in quoted:
                text = "[document]"
            elif "audioMessage" in quoted:
                text = "[audio]"
            elif "stickerMessage" in quoted:
                text = "[sticker]"
            else:
                text = "[non-text message]"

        # Phone of the ORIGINAL sender (the person being replied to)
        phone = ctx.get("participant", "") or ""
        phone = re.sub(r"@.*", "", phone)
        phone = re.sub(r"[^0-9]", "", phone)

        # Trim overly-long quotes so bodies don't explode
        if len(text) > 500:
            text = text[:497] + "..."
        return (text.strip(), phone)
    except Exception as e:
        models.log_event("QUOTE_EXTRACT_ERROR", str(e))
        return ("", "")


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


# ---- Approvals ----

@app.route("/approvals")
@login_required
def approvals_page():
    approvals = models.get_approvals()
    people = _get_people()
    return render_template("approvals.html", approvals=approvals, people=people)


@app.route("/approvals/update/<int:approval_id>", methods=["POST"])
@login_required
def update_approval(approval_id):
    sender_name = request.form.get("sender_name", "")
    sender_phone = request.form.get("sender_phone", "")
    message = request.form.get("message", "")
    additional = request.form.get("additional_message", "")
    updates = dict(
        sender_name=sender_name, sender_phone=sender_phone,
        additional_message=additional
    )
    if message:
        updates["message"] = message
    models.update_approval(approval_id, **updates)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok"})
    flash("Approval updated", "success")
    return redirect(url_for("approvals_page"))


@app.route("/approvals/send/<int:approval_id>", methods=["POST"])
@login_required
def send_approval(approval_id):
    approvals = models.get_approvals()
    approval = None
    for a in approvals:
        if a["id"] == approval_id:
            approval = a
            break
    if not approval:
        flash("Approval not found", "error")
        return redirect(url_for("approvals_page"))

    phone = approval["sender_phone"]
    message = approval["message"]
    additional = request.form.get("additional_message", "").strip()

    if not phone or len(phone) < 5:
        models.update_approval(approval_id, last_sent="ERROR: No phone")
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"status": "error", "last_sent": "ERROR: No phone"})
        flash("No phone number", "error")
        return redirect(url_for("approvals_page"))

    full_message = (
        f"{additional}\n\n--- Original approval request ---\n{message}"
        if additional else message
    )

    success = _send_whatsapp(phone, full_message)
    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    last_sent = f"Sent {now}" if success else f"Failed {now}"
    models.update_approval(
        approval_id, last_sent=last_sent, additional_message=additional
    )
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok" if success else "error", "last_sent": last_sent})
    flash("Reply sent!" if success else "Send failed", "success" if success else "error")
    return redirect(url_for("approvals_page"))


@app.route("/approvals/delete/<int:approval_id>", methods=["POST"])
@login_required
def delete_approval(approval_id):
    models.delete_approval(approval_id)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"status": "ok"})
    flash("Approval deleted", "success")
    return redirect(url_for("approvals_page"))


@app.route("/approvals/bulk-delete", methods=["POST"])
@login_required
def bulk_delete_approvals():
    ids = request.form.getlist("approval_ids")
    count = 0
    for aid in ids:
        models.delete_approval(int(aid))
        count += 1
    flash(f"Deleted {count} approval(s)", "success")
    return redirect(url_for("approvals_page"))


@app.route("/approvals/bulk-send", methods=["POST"])
@login_required
def bulk_send_approvals():
    ids = request.form.getlist("approval_ids")
    approvals = models.get_approvals()
    amap = {a["id"]: a for a in approvals}
    sent = 0
    failed = 0
    for aid in ids:
        a = amap.get(int(aid))
        if not a:
            continue
        phone = a["sender_phone"]
        message = a["message"]
        additional = a["additional_message"] or ""
        if not phone or len(phone) < 5:
            models.update_approval(int(aid), last_sent="ERROR: No phone")
            failed += 1
            continue
        full_message = (
            f"{additional}\n\n--- Original approval request ---\n{message}"
            if additional else message
        )
        success = _send_whatsapp(phone, full_message)
        now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
        models.update_approval(
            int(aid),
            last_sent=f"Sent {now}" if success else f"Failed {now}"
        )
        if success:
            sent += 1
        else:
            failed += 1
    flash(f"Sent {sent}, failed {failed} of {len(ids)} selected",
          "success" if failed == 0 else "error")
    return redirect(url_for("approvals_page"))


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

    # Build a name -> phone map from contacts to help attribution
    contacts_by_phone = {c["phone"]: c["name"] for c in models.get_contacts()}

    conversation = ""
    for m in messages:
        # Prefer resolved contact name; include phone if name was only a fallback
        name = m["sender_name"]
        phone = m["sender_phone"] or ""
        if name == phone and phone in contacts_by_phone:
            name = contacts_by_phone[phone]
        label = f"{name} ({phone})" if phone and phone != name else name
        conversation += f"[{m['timestamp']}] {label}: {m['body']}\n"

    group_name = models.get_group_name_by_jid(group_jid)

    prompt = f"""You are an operations analyst at a corrugation/packaging company (XpertPack).
Summarize this WhatsApp group conversation from the "{group_name}" group.

Organize into these sections (skip any that have no content):
1. **Key Operational Issues** - production problems, delays, quality issues
2. **Action Items** - tasks that need to be done, who needs to do what
3. **Pending Decisions** - things waiting for approval or decision
4. **Dispatch/Logistics Updates** - any movement, truck, delivery updates
5. **Escalations** - urgent matters that need immediate attention

CRITICAL RULE — ATTRIBUTION:
For EVERY single bullet, identify WHO raised it and WHO is pushing/escalating it.
Senders appear in the transcript as "[timestamp] Name (phone): message".
Format each bullet exactly like this:
   - **<topic>** — <what / status>. _Raised by <Name>_
If multiple people are involved, list all of them:
   _Raised by Ravi, escalated by Jha sir, awaiting reply from Tomar_
If a phone number is given in the transcript but no name is available,
write the phone number in place of the name so the user can look it up.
Never omit the attribution — it is the whole point of this summary.

Be concise. Use bullet points. If messages are in Hindi/Hinglish, summarize in English.

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


def _generate_email_digest_24h():
    """Fetch last-24h Gmail and produce a triaged digest; store in email_summaries.
    Non-destructive: doesn't touch /opt/email-digest/ state or WhatsApp notifications.
    Returns the summary text or None on error/no emails."""
    token_file = os.environ.get("GMAIL_TOKEN_FILE", "/opt/whatsapp-tracker/token.json")
    if not os.path.exists(token_file):
        models.log_event("EMAIL_DIGEST_ERROR", f"Token file missing: {token_file}")
        return None
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError as e:
        models.log_event("EMAIL_DIGEST_ERROR", f"Google libs missing: {e}")
        return None

    try:
        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
        creds = Credentials.from_authorized_user_file(token_file, scopes)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())
        service = build("gmail", "v1", credentials=creds)

        # Last 24 hours, excluding noise senders (same list as email_checker.py)
        skip = " ".join(f"-from:{s}" for s in [
            "alerts@yes.bank.in", "noreply@zen-makemytrip.com",
            "information@yes.bank.in",
        ])
        query = f"newer_than:1d {skip}"
        results = service.users().messages().list(
            userId="me", q=query, maxResults=100
        ).execute()
        stubs = results.get("messages", [])

        if not stubs:
            models.add_email_summary(
                time_range=f"Last 24h as of {_now_ist_str()}",
                summary="No emails in the last 24 hours.",
                email_count=0, action_count=0
            )
            return "No emails in the last 24 hours."

        emails = []
        for stub in stubs[:60]:
            msg = service.users().messages().get(
                userId="me", id=stub["id"], format="metadata",
                metadataHeaders=["From", "To", "Cc", "Subject", "Date"]
            ).execute()
            headers = msg.get("payload", {}).get("headers", [])
            def hv(name):
                for h in headers:
                    if h["name"].lower() == name.lower():
                        return h["value"]
                return ""
            emails.append({
                "id": stub["id"],
                "from": hv("From"), "to": hv("To"), "cc": hv("Cc"),
                "subject": hv("Subject"), "date": hv("Date"),
                "snippet": msg.get("snippet", "")[:200],
            })

        # Ask Mistral to triage and summarise
        email_text = json.dumps(emails, indent=2)
        prompt = """You are an email triage assistant for Anant (anant@xpertpack.in),
who runs XpertPack (corrugation/packaging) and is partner in KLPL (clothing).

Group related emails into threads. For each thread/entry classify:
  Type: ACTION (anant@xpertpack.in is in To/CC AND the email asks him to do
        something specific — approve, respond, review, confirm, decide),
        INFO (anant is in To/CC but no specific response required; or
        management@xpertpack.in is in To containing important business info),
        or FYI (everything else — cc-only, automated alerts, internal forwards).

For each entry produce a 1-2 line summary including: customer/external party
name if any, who sent to whom, and what the ask/status is.

Sort: all ACTION first, then INFO, then FYI. Skip bank/OTP/promotional/
birthday/analytics emails.

Format the OUTPUT in plain text (no markdown fences), like:

**ACTION REQUIRED**
• [Sender name] <1-2 line summary>
  https://mail.google.com/mail/u/0/#inbox/<id>

**INFORMATION**
• [Sender name] <summary>

**FYI**
• [Sender name] <summary>

At the end add one line: "Total: X emails (Y action, Z info, rest FYI)".
Skipped noise emails should not appear in the output.
"""
        try:
            resp = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {MISTRAL_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": MISTRAL_MODEL,
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": f"Triage these emails:\n{email_text}"},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 3000,
                },
                timeout=90,
            )
            if resp.status_code == 200:
                summary_text = resp.json()["choices"][0]["message"]["content"]
            else:
                summary_text = f"Mistral API error ({resp.status_code}): {resp.text[:500]}"
        except Exception as e:
            summary_text = f"Error calling Mistral: {e}"

        # Count action items (naive — count 'ACTION' lines / bullet points under ACTION section)
        action_count = 0
        in_action = False
        for line in summary_text.split("\n"):
            l = line.strip()
            if "ACTION" in l.upper() and ("REQUIRED" in l.upper() or "ITEMS" in l.upper()):
                in_action = True
                continue
            if in_action:
                if l.startswith("**") or (l.upper().startswith(("INFO", "FYI"))):
                    break
                if l.startswith(("-", "•", "*")) or (l and l[0].isdigit() and "." in l[:3]):
                    action_count += 1

        models.add_email_summary(
            time_range=f"Last 24h as of {_now_ist_str()}",
            summary=summary_text,
            email_count=len(emails),
            action_count=action_count
        )
        return summary_text
    except Exception as e:
        models.log_event("EMAIL_DIGEST_ERROR", str(e))
        return None


def _now_ist_str():
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")


def _selected_groups():
    """Groups included in the auto-summary / 24h digest.
    Returns list of (jid, name) tuples."""
    keywords = ("dispatch", "daily report")
    selected = []
    for g in models.get_groups():
        name = g["group_name"] or ""
        jid = g["group_jid"] or ""
        if any(k in name.lower() for k in keywords):
            selected.append((jid, name))
    return selected


def _auto_generate_summaries():
    """Scheduled job: every 2h, summarise selected groups and DM me,
    and generate the 24h email digest."""
    # Group summaries (2h window, DM to me)
    for jid, name in _selected_groups():
        summary = _generate_summary(jid, hours=2)
        if summary:
            _send_whatsapp(MY_PHONE + "@s.whatsapp.net",
                           f"*Summary (last 2h) — {name}*\n\n{summary}")
    # 24h email digest (stored in DB for the Emails tab; no WhatsApp — the
    # existing /opt/email-digest timer still handles incremental notifications)
    try:
        _generate_email_digest_24h()
    except Exception as e:
        models.log_event("EMAIL_DIGEST_ERROR", str(e))


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


# ---- 24h Email Digest ----

@app.route("/emails", methods=["GET", "POST"])
@login_required
def emails_page():
    """Last-24h email digest. Latest at top. Refresh button regenerates now."""
    if request.method == "POST":
        result = _generate_email_digest_24h()
        if result is None:
            flash("Could not generate digest — check log (Gmail token or Mistral)", "error")
        else:
            flash("Fresh 24h email digest generated!", "success")
        return redirect(url_for("emails_page"))

    summaries = models.get_email_summaries(limit=20)
    return render_template("emails.html", summaries=summaries)


# ---- 24h Digest (flat view of all selected groups) ----

@app.route("/summaries/digest", methods=["GET", "POST"])
@login_required
def digest_page():
    """Flat 24h summary of all selected groups on one page. No dropdowns."""
    selected = _selected_groups()

    if request.method == "POST":
        # Regenerate 24h summary for each selected group
        generated = 0
        skipped = 0
        for jid, _ in selected:
            if _generate_summary(jid, hours=24):
                generated += 1
            else:
                skipped += 1
        flash(f"Generated {generated} fresh digest(s){' (' + str(skipped) + ' groups had no messages)' if skipped else ''}",
              "success")
        return redirect(url_for("digest_page"))

    # GET: show the latest 24h summary for each selected group
    all_summaries = models.get_summaries(limit=500)
    # Pick the newest 24h-range summary per group
    latest_by_jid = {}
    for s in all_summaries:
        if s["group_jid"] in (jid for jid, _ in selected) and "24h" in (s["time_range"] or ""):
            if s["group_jid"] not in latest_by_jid:
                latest_by_jid[s["group_jid"]] = s
    digest_items = []
    for jid, name in selected:
        digest_items.append({
            "group_jid": jid,
            "group_name": name,
            "summary": latest_by_jid.get(jid)
        })
    return render_template("digest.html", items=digest_items,
                           total_groups=len(selected))


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
