# File: login.py

from NorenRestApiPy.NorenApi import NorenApi
import json
import time
import pytz
from datetime import datetime, time as dt_time
from alerts import send_telegram_alert
import pyotp

IST = pytz.timezone("Asia/Kolkata")

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        super().__init__(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/"
        )

class LoginManager:
    def __init__(self):
        self.api = ShoonyaApiPy()
        self.logged_in = False
        self.session_data = {}

        with open("config.json") as f:
            config = json.load(f)
            self.token = config["totp_token"]
            self.userid = config["userid"]
            self.password = config["password"]
            self.vendor_code = config["vendor_code"]
            self.api_secret = config["api_secret"]
            self.imei = config["imei"]

    def login(self):
        try:
            factor2 = pyotp.TOTP(self.token).now()

            print("🔐 Attempting login with:")
            print(f"📛 User ID: {self.userid}")
            print(f"🔑 TOTP: {factor2}")
            print(f"🧾 Vendor: {self.vendor_code}, IMEI: {self.imei}")

            ret = self.api.login(
                userid=self.userid,
                password=self.password,
                twoFA=factor2,
                vendor_code=self.vendor_code,
                api_secret=self.api_secret,
                imei=self.imei
            )

            print("🧪 Raw login response:", ret)

            if ret and ret.get("stat") == "Ok":
                self.logged_in = True
                self.session_data = ret
                send_telegram_alert("✅ Bot Login Successful")
                print("✅ Logged in successfully")
                return True
            else:
                send_telegram_alert(f"❌ Login Failed: {ret}")
                print(f"❌ Login failed: {ret}")
        except Exception as e:
            send_telegram_alert(f"❌ Login Exception: {e}")
            print(f"❌ Exception during login: {e}")
        return False

    def logout(self):
        try:
            self.api.logout()
            self.logged_in = False
            send_telegram_alert("👋 Logged Out of Session")
        except Exception as e:
            send_telegram_alert(f"❌ Logout Error: {e}")

    def keep_alive(self):
        try:
            now = datetime.now(IST).time()
            if dt_time(10, 0) <= now <= dt_time(15, 15):
                if not self.logged_in:
                    return  # skip ping if not logged in
                response = self.api.get_quotes(exchange="NSE", token="26000")
                if not response or "lp" not in response:
                    send_telegram_alert("⚠️ Keep-alive: Empty response, re-logging in...")
                    self.logout()
                    self.login()
        except Exception as e:
            send_telegram_alert(f"⚠️ Session ping failed: {e}")
            self.logout()
            self.login()

    def get_api(self):
        return self.api

    def is_logged_in(self):
        return self.logged_in

# Initialize login manager
login_manager = LoginManager()
