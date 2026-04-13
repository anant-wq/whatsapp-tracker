#!/bin/bash
# Deploy WhatsApp Task Tracker to Vultr server (jarvis-xp)
# Run this once on the server: bash deploy.sh

set -e

APP_NAME="whatsapp-tracker"
APP_DIR="/opt/$APP_NAME"
APP_PORT=5003
DOMAIN="tools.xpertpack.in"

echo "=== Deploying $APP_NAME ==="

# Create app directory
mkdir -p $APP_DIR
cd $APP_DIR

# Copy files (or git clone)
echo "Copy your code to $APP_DIR first, then re-run this script."

# Python venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env if not exists
if [ ! -f .env ]; then
cat > .env << 'ENVEOF'
SECRET_KEY=CHANGE_ME
WASENDER_API_KEY=CHANGE_ME
GOOGLE_CLIENT_ID=CHANGE_ME
GOOGLE_CLIENT_SECRET=CHANGE_ME
ALLOWED_EMAIL=anant@xpertpack.in
MY_PHONE=918447731703
ENVEOF
echo ">>> Edit $APP_DIR/.env with your actual values!"
fi

# Systemd service
cat > /etc/systemd/system/$APP_NAME.service << EOF
[Unit]
Description=WhatsApp Task Tracker
After=network.target

[Service]
User=www-data
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/gunicorn app:app --workers 2 --bind 0.0.0.0:$APP_PORT --timeout 120
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable $APP_NAME
systemctl restart $APP_NAME

# Nginx config (add location block)
# Add this to your existing nginx config for tools.xpertpack.in:
echo ""
echo "=== Add this to your nginx config ==="
echo ""
cat << 'NGINXEOF'
    location /whatsapp/ {
        proxy_pass         http://127.0.0.1:5003/;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
    }
NGINXEOF

echo ""
echo "=== Done! ==="
echo "1. Edit $APP_DIR/.env with your WaSender API key"
echo "2. Add the nginx location block above"
echo "3. Run: nginx -t && systemctl reload nginx"
echo "4. Set WaSender webhook URL to: http://$DOMAIN/whatsapp/webhook"
echo "5. Add OAuth redirect URI in Google Cloud Console:"
echo "   http://$DOMAIN/whatsapp/auth/callback"
