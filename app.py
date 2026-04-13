import os
import json
import secrets
from datetime import datetime
from functools import wraps

import requests
from flask import (
    Flask, request, redirect, url_for, session,
    render_template, jsonify, flash
)
from authlib.integrations.flask_client import OAuth

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
app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix=os.environ.get("APP_PREFIX", "/whatsapp"))

WASENDER_BASE = "https://api.wasenderapi.com"
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY", "")
MY_PHONE = os.environ.get("MY_PHONE", "918447731703")
ALLOWED_EMAIL = os.environ.get("ALLOWED_EMAIL", "anant@xpertpack.in")
APP_PREFIX = os.environ.get("APP_PREFIX", "/whatsapp")  # for nginx sub-path

# ---- OAuth ----

oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
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
        "timestamp": datetime.now().isoformat()
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

        # Extract body
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
        import re
        sender_phone = re.sub(r"@.*", "", sender_phone)
        sender_phone = re.sub(r"[^0-9]", "", sender_phone)
        is_group = "@g.us" in chat_jid

        models.log_event("PARSED", f"fromMe={from_me} | body={body[:80]} | sender={sender_phone} | isGroup={is_group}")

        # Group #todo capture
        if is_group and body and "#todo" in body.lower():
            is_my_phone = (MY_PHONE in sender_phone or sender_phone in MY_PHONE)
            if is_my_phone:
                ts = data.get("timestamp") or msgs.get("messageTimestamp")
                date_str = _parse_timestamp(ts)
                group_name = models.get_group_name_by_jid(chat_jid)
                models.add_task(
                    date_str=date_str,
                    message=body,
                    phone=chat_jid,
                    status=f"#todo | {group_name}"
                )
                return jsonify({"status": "ok", "message": "group todo added"})

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
        models.add_task(date_str=date_str, message=body, phone=sender_phone)
        return jsonify({"status": "ok", "message": "task added"})

    except Exception as e:
        models.log_event("ERROR", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def _parse_timestamp(ts):
    if ts is None:
        return datetime.now().strftime("%Y-%m-%d")
    if isinstance(ts, (int, float)):
        ts = ts if ts > 9999999999 else ts * 1000
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


# ---- Pages ----

@app.route("/")
@login_required
def index():
    return redirect(url_for("tasks_page"))


@app.route("/tasks")
@login_required
def tasks_page():
    tasks = models.get_tasks()
    return render_template("tasks.html", tasks=tasks)


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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    models.update_task(
        task_id,
        last_sent=f"Sent {now}" if success else f"Failed {now}",
        additional_message=additional
    )
    flash("Message sent!" if success else "Send failed", "success" if success else "error")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/update/<int:task_id>", methods=["POST"])
@login_required
def update_task(task_id):
    person = request.form.get("person", "")
    status = request.form.get("status", "")
    additional = request.form.get("additional_message", "")
    models.update_task(task_id, person=person, status=status, additional_message=additional)
    flash("Task updated", "success")
    return redirect(url_for("tasks_page"))


@app.route("/tasks/delete/<int:task_id>", methods=["POST"])
@login_required
def delete_task(task_id):
    models.delete_task(task_id)
    flash("Task deleted", "success")
    return redirect(url_for("tasks_page"))


# ---- Groups ----

@app.route("/groups")
@login_required
def groups_page():
    groups = models.get_groups()
    return render_template("groups.html", groups=groups)


@app.route("/groups/fetch", methods=["POST"])
@login_required
def fetch_groups():
    if not WASENDER_API_KEY:
        flash("No API key configured", "error")
        return redirect(url_for("groups_page"))

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
            return redirect(url_for("groups_page"))

        data = resp.json()
        groups = data.get("data") or data if isinstance(data, list) else data.get("data", [])
        if not isinstance(groups, list):
            flash("Unexpected response format", "error")
            return redirect(url_for("groups_page"))

        models.upsert_groups(groups)
        flash(f"Found {len(groups)} group(s)", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")

    return redirect(url_for("groups_page"))


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
        return redirect(url_for("groups_page"))

    message = request.form.get("message", "").strip()
    if not message:
        flash("No message to send", "error")
        return redirect(url_for("groups_page"))

    jid = group["group_jid"]
    success = _send_whatsapp(jid, message)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    models.update_group(group_id, last_sent=f"Sent {now}" if success else f"Failed {now}")
    flash("Message sent!" if success else "Send failed", "success" if success else "error")
    return redirect(url_for("groups_page"))


# ---- Contacts ----

@app.route("/contacts")
@login_required
def contacts_page():
    contacts = models.get_contacts()
    return render_template("contacts.html", contacts=contacts)


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


@app.route("/contacts/delete/<int:contact_id>", methods=["POST"])
@login_required
def delete_contact(contact_id):
    models.delete_contact(contact_id)
    flash("Contact deleted", "success")
    return redirect(url_for("contacts_page"))


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
