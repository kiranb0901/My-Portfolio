# File: alerts.py

import json
import os
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone("Asia/Kolkata")

TELEGRAM_CHAT_IDS = ["1377602734", "724546690", "1097122255", "964078358", "966351905"]
TELEGRAM_TOKEN = "8187057037:AAEpXhQuo-NjPMcmZhdkufmqbyPzVRPYZh0"
ALERTS_FILE = "alerts.json"

def send_telegram_alert(msg):
    import requests
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data={"chat_id": chat_id, "text": msg},
                timeout=3
            )
        except:
            pass

def round_tick(p):
    return round(round(p * 10) / 10, 2)

def parse_alert_message(msg):
    try:
        alert = json.loads(msg)
        for k in ["action", "symbol", "entry", "stoploss", "time"]:
            if k not in alert:
                return None

        return {
            "action": alert["action"].lower(),
            "symbol": alert["symbol"],
            "entry_price": round_tick(float(alert["entry"])),
            "stoploss_price": round_tick(float(alert["stoploss"])),
            "timestamp": int(alert["time"]),
            "alert_time": datetime.now(IST)
        }
    except:
        return None

def load_alerts():
    if not os.path.exists(ALERTS_FILE):
        return []
    try:
        with open(ALERTS_FILE) as f:
            return json.load(f)
    except:
        return []

def save_alerts(alerts):
    try:
        with open(ALERTS_FILE, "w") as f:
            json.dump(alerts, f, indent=2, default=str)
    except:
        pass

def get_recent_alerts(minutes=10):
    now = datetime.now(IST)
    return [a for a in load_alerts() if now - datetime.fromisoformat(a["alert_time"]) < timedelta(minutes=minutes)]

def clear_all():
    save_alerts([])

def save_alert(alert):
    alerts = load_alerts()
    alerts.append(alert)
    save_alerts(alerts)

class AlertManager:
    def get_recent_alerts(self, minutes=10):
        return get_recent_alerts(minutes)

    def clear_all(self):
        clear_all()

    def save_alert(self, alert):
        save_alert(alert)

alert_manager = AlertManager()
