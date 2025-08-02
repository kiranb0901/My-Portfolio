from flask import Flask, request, jsonify
from datetime import datetime, time as dt_time
import threading
import pytz
import time
from alerts import parse_alert_message, alert_manager, send_telegram_alert
from orders import process_alert
from login import login_manager

IST = pytz.timezone("Asia/Kolkata")
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw = request.get_data(as_text=True)
        send_telegram_alert(f"📡 Webhook Received:\n{raw}")
        
        alert = parse_alert_message(raw)
        if not alert:
            send_telegram_alert("❌ Invalid alert format.")
            return jsonify({"status": "error", "message": "Invalid alert format"}), 400

        required_keys = ["symbol", "action", "entry_price", "stoploss_price"]
        if not all(k in alert for k in required_keys):
            send_telegram_alert("❌ Alert missing keys.")
            return jsonify({"status": "error", "message": "Missing required keys"}), 400

        # ✅ Now properly placed
        result = process_alert(alert)
        return jsonify(result)

    except Exception as e:
        send_telegram_alert(f"🚨 Webhook crashed: {e}")
        return jsonify({"status": "error", "message": "Internal server error", "details": str(e)}), 500

@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/logout")
def logout():
    login_manager.logout()
    return jsonify({"status": "logged out"})

@app.route("/status")
def status():
    return jsonify({
        "logged_in": login_manager.is_logged_in(),
        "active_alerts": len(alert_manager.get_recent_alerts()),
        "now": datetime.now(IST).isoformat()
    })

def session_heartbeat():
    while True:
        login_manager.keep_alive()
        time.sleep(600)

def daily_scheduler():
    login_done = False
    logout_done = False

    while True:
        now = datetime.now(IST).time()
        if now >= dt_time(10, 15) and not login_done:
            login_manager.login()
            send_telegram_alert("✅ Auto Login at 10:15 AM")
            login_done = True

        # 🔒 Logout at 3:30 PM
        if now >= dt_time(15, 30) and not logout_done:
            login_manager.logout()
            send_telegram_alert("🔒 Auto Logout at 3:30 PM")
            logout_done = True

        if now < dt_time(10, 0):
            login_done = False
            logout_done = False

        time.sleep(30)

def start_bot():
    login_manager.login()
    threading.Thread(target=session_heartbeat, daemon=True).start()
    threading.Thread(target=daily_scheduler, daemon=True).start()

start_bot()

if __name__ == "__main__":
    app.run(debug=True, port=8000)
