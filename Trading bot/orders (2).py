import logging
from datetime import datetime, timedelta, time as dt_time
import pytz
import time
import threading
import urllib.parse
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from alerts import round_tick, alert_manager, send_telegram_alert
from login import login_manager

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Google Sheet Setup
scope = ["Confidential"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gcreds.json", scope)
gc = gspread.authorize(creds)
sheet = gc.open("Trade Alerts DB").sheet1

# In-memory state
active_positions = {}
pending_entries = {}
closed_trades = []

# Auto-login at startup
if not login_manager.is_logged_in():
    login_manager.login()

# --- Session Heartbeat ---
def session_heartbeat():
    while True:
        try:
            login_manager.keep_alive()
        except Exception as e:
            print(f"Session heartbeat error: {e}")
        time.sleep(90)

# Util to get column index by name (updated for date column)
COLS = {
    "date": 1,
    "symbol": 2,
    "action": 3,
    "entry_price": 4,
    "stoploss_price": 5,
    "entry_order_id": 6,
    "entry_timestamp": 7,
    "sl_order_id": 8,
    "sl_timestamp": 9,
    "exit_price": 10,
    "market_order_id": 11,
    "market_exit_timestamp": 12,
    "status": 13,
    "closed_flag": 14
}

class SmartSymbolMapper:
    def __init__(self):
        # Only complex edge cases
        self.manual_overrides = {
            "m&m": "M&M",
            "l&t": "LT", 
            "dr_reddy": "DRREDDY",
            "asian_paints": "ASIANPAINT",
            "bharti_airtel": "BHARTIARTL",
            "bajaj_finserv": "BAJAJFINSV",
            "bajaj_auto": "BAJAJ-AUTO"
        }
    
    def clean_and_convert(self, symbol):
        cleaned = symbol.lower().strip()
        
        # Check manual overrides first
        if cleaned in self.manual_overrides:
            return self.manual_overrides[cleaned]
        
        # Replace underscores with hyphens
        converted = cleaned.replace('_', '-')
        converted = re.sub(r'[_\s]+', '-', converted)
        converted = re.sub(r'-(eq|nse|bse)$', '', converted)
        converted = converted.upper()
        
        # Special handling for AUTO
        if 'AUTO' in converted and 'BAJAJ' in converted:
            converted = 'BAJAJ-AUTO'
        
        return converted
    
    def prepare_for_api(self, symbol):
        converted = self.clean_and_convert(symbol)
        
        if not converted.endswith('-EQ'):
            converted = f"{converted}-EQ"
        
        return urllib.parse.quote(converted, safe='')

symbol_mapper = SmartSymbolMapper()

def restore_state_from_sheet():
    records = sheet.get_all_records()
    for row in records:
        entry_id = str(row.get("entry_order_id", ""))
        sl_id = str(row.get("sl_order_id", ""))
        status = row.get("status")
        if status == "exited":
            continue
        if entry_id and sl_id and status == "sl_placed":
            pid = f"{row['symbol']}_{row['action']}_{entry_id}"
            ts_str = row.get("entry_timestamp")
            if ts_str:
                today = datetime.now(IST).date()
                entry_time = datetime.strptime(ts_str, "%H:%M").replace(
                    year=today.year, month=today.month, day=today.day, tzinfo=IST
                )
            else:
                entry_time = datetime.now(IST)

            active_positions[pid] = {
                "symbol": row["symbol"],
                "action": row["action"],
                "entry_price": float(row["entry_price"]),
                "stoploss_price": float(row["stoploss_price"]),
                "entry_order_id": entry_id,
                "sl_order_id": sl_id,
                "entry_time": entry_time,
                "exit_time": calculate_exit_time(entry_time)
            }

def update_status_in_sheet(entry_order_id, status, closed_flag):
    try:
        cell = sheet.find(entry_order_id)
        sheet.update_cell(cell.row, COLS["status"], status)
        sheet.update_cell(cell.row, COLS["closed_flag"], closed_flag)
    except Exception as e:
        print(f"[update_status_in_sheet error] {e}")

def update_sl_in_sheet(entry_order_id, sl_id):
    try:
        cell = sheet.find(entry_order_id)
        sheet.update_cell(cell.row, COLS["sl_order_id"], sl_id)
        sheet.update_cell(cell.row, COLS["sl_timestamp"], datetime.now(IST).strftime("%H:%M"))
        sheet.update_cell(cell.row, COLS["status"], "sl_placed")
    except Exception as e:
        print(f"update_sl_in_sheet error: {e}")

def update_exit_in_sheet(entry_order_id, exit_price, market_order_id):
    try:
        cell = sheet.find(entry_order_id)
        sheet.update_cell(cell.row, COLS["exit_price"], str(exit_price))
        sheet.update_cell(cell.row, COLS["market_order_id"], market_order_id)
        sheet.update_cell(cell.row, COLS["market_exit_timestamp"], datetime.now(IST).strftime("%H:%M"))
        sheet.update_cell(cell.row, COLS["status"], "exited")
        sheet.update_cell(cell.row, COLS["closed_flag"], "Yes")
    except Exception as e:
        print(f"update_exit_in_sheet error: {e}")

def append_to_sheet(row):
    try:
        current_date = datetime.now(IST).date().strftime("%Y-%m-%d")
        sheet.append_row([current_date] + row)  # Prepend date as first column
    except Exception as e:
        print(f"[append_to_sheet error] {e}")
        send_telegram_alert(f"❌ Failed to append row to sheet: {e}")

def fetch_sl_price(entry_order_id):
    try:
        records = sheet.get_all_records()
        for row in records:
            if str(row.get("entry_order_id")) == str(entry_order_id):
                return float(row.get("stoploss_price", 0))
    except:
        return 0
    return 0

def _parse_time(exchtime_str):
    try:
        return datetime.strptime(exchtime_str, "%d-%b-%Y %H:%M:%S").astimezone(IST)
    except:
        return datetime.now(IST)

def fetch_order_book():
    if not login_manager.is_logged_in():
        login_manager.login()
    try:
        api = login_manager.get_api()
        orders = api.get_order_book()
        if not isinstance(orders, list):
            raise ValueError("Invalid order book format")
        return orders
    except Exception as e:
        print(f"[WARN] Order book fetch failed: {e}")
        return []

def get_filled_price(order_id):
    try:
        orders = fetch_order_book()
        for order in orders:
            if order.get("norenordno") == order_id:
                return float(order.get("avgprc", 0))
    except:
        pass
    return 0

def place_order(symbol, action, price):
    # Clean symbol before placing order
    clean_symbol = symbol_mapper.clean_and_convert(symbol)
    
    def submit_order(api_client):
        transaction = "B" if action == "buy" else "S"
        raw_symbol = f"{clean_symbol}-EQ" if not clean_symbol.endswith("-EQ") else clean_symbol
        tradingsymbol = urllib.parse.quote(raw_symbol, safe='')
        trigger_price = round_tick(price)
        order_price = round_tick(price)
        print(f"\U0001f4e4 Submitting SL-LMT {action.upper()} order for {tradingsymbol} @ ₹{order_price} (trigger ₹{trigger_price})")
        return api_client.place_order(
            buy_or_sell=transaction,
            product_type="I",
            exchange="NSE",
            tradingsymbol=tradingsymbol,
            quantity="1",
            discloseqty=0,
            price_type="SL-LMT",
            price=str(order_price),
            trigger_price=str(trigger_price),
            retention="DAY",
            remarks=f"Auto {action.upper()} {clean_symbol}"
        )

    for attempt in range(2):
        try:
            if not login_manager.is_logged_in():
                login_manager.login()
            api = login_manager.get_api()
            response = submit_order(api)
            if response and response.get("stat") == "Ok":
                order_id = response.get("norenordno")
                send_telegram_alert(f"🔕 Entry Order Placed: {clean_symbol} ({action.upper()}) @ ₹{price} | Order ID: {order_id}")
                return order_id
            else:
                raise Exception(f"Order Failed: {response}")
        except Exception as e:
            print(f"[place_order error] Attempt {attempt+1}: {e}")
            login_manager.logout()
            time.sleep(2)
    send_telegram_alert(f"❌ Entry Order Failed for {clean_symbol} ({action.upper()})")
    return None

def place_market_order(symbol, action):
    api = login_manager.get_api()
    side = "B" if action == "buy" else "S"
    clean_symbol = symbol_mapper.clean_and_convert(symbol)
    raw_symbol = f"{clean_symbol}-EQ" if not clean_symbol.endswith("-EQ") else clean_symbol
    tradingsymbol = urllib.parse.quote(raw_symbol, safe='')
    try:
        response = api.place_order(
            buy_or_sell=side,
            product_type="I",
            exchange="NSE",
            tradingsymbol=tradingsymbol,
            quantity="1",
            discloseqty=0,
            price_type="MKT",
            price="0",
            trigger_price="0",
            retention="DAY",
            remarks="Exit at Market"
        )
        if response and response.get("stat") == "Ok":
            order_id = response.get("norenordno")
            price = get_filled_price(order_id)
            send_telegram_alert(f"✅ Exit MKT order placed for {clean_symbol} ({action.upper()}) @ ₹{price}")
            return price, order_id
        else:
            send_telegram_alert(f"❌ MKT Exit failed: {response}")
    except Exception as e:
        send_telegram_alert(f"❌ MKT Exit error: {e}")
    return 0, ""

def place_stoploss(symbol, action, trigger, entry_order_id):
    try:
        records = sheet.get_all_records()
        for row in records:
            if str(row.get("entry_order_id")) == str(entry_order_id) and row.get("status") == "exited":
                print(f"[skip] SL not placed for {entry_order_id} (already exited)")
                return None
    except:
        return None

    if fetch_sl_price(entry_order_id) == 0:
        return None

    api = login_manager.get_api()
    side = "S" if action == "buy" else "B"
    trigger_price = round_tick(trigger)
    clean_symbol = symbol_mapper.clean_and_convert(symbol)
    raw_symbol = f"{clean_symbol}-EQ" if not clean_symbol.endswith("-EQ") else clean_symbol
    tradingsymbol = urllib.parse.quote(raw_symbol, safe='')

    def try_order():
        return api.place_order(
            buy_or_sell=side,
            product_type="I",
            exchange="NSE",
            tradingsymbol=tradingsymbol,
            quantity="1",
            discloseqty=0,
            price_type="SL-LMT",
            price=str(trigger_price),
            trigger_price=str(trigger_price),
            retention="DAY",
            remarks="SL Order"
        )

    if not login_manager.is_logged_in():
        login_manager.login()
    ret = try_order()
    if not ret or ret.get("stat") != "Ok":
        login_manager.logout()
        login_manager.login()
        api = login_manager.get_api()
        ret = try_order()
    if ret and ret.get("stat") == "Ok":
        sl_id = ret.get("norenordno")
        send_telegram_alert(f"🔭 SL-LMT Placed: {clean_symbol} @ ₹{trigger} | SL Order ID: {sl_id}")
        update_sl_in_sheet(entry_order_id, sl_id)
        return sl_id
    else:
        send_telegram_alert(f"❌ SL Placement Failed: {ret}")
        return None

def calculate_exit_time(entry_time):
    t = entry_time.time()
    if dt_time(11, 15) <= t < dt_time(12, 15):
        return entry_time.replace(hour=12, minute=15, second=0, microsecond=0)
    elif dt_time(12, 15) <= t < dt_time(13, 15):
        return entry_time.replace(hour=13, minute=15, second=0, microsecond=0)
    elif dt_time(13, 15) <= t < dt_time(14, 15):
        return entry_time.replace(hour=14, minute=15, second=0, microsecond=0)
    elif dt_time(14, 15) <= t:
        return entry_time.replace(hour=14, minute=55, second=0, microsecond=0)
    return None

def process_alert(alert):
    symbol = symbol_mapper.clean_and_convert(alert["symbol"])
    action = alert["action"]
    entry = alert["entry_price"]
    sl = alert["stoploss_price"]
    pid = f"{symbol}_{action}_{int(datetime.now().timestamp())}"
    oid = place_order(symbol, action, entry)
    if oid:
        row = [symbol, action, entry, sl, oid,
               datetime.now(IST).strftime("%H:%M"), "", "", "", "", "", "pending", ""]
        append_to_sheet(row)
        pending_entries[pid] = {
            "symbol": symbol,
            "action": action,
            "entry_price": entry,
            "stoploss_price": sl,
            "entry_order_id": oid,
            "alert_time": datetime.now(IST)
        }
        return {"status": "success", "position_id": pid}
    else:
        alert_manager.save_alert(alert)
        return {"status": "failed", "reason": "order placement failed"}

def process_complete(order):
    symbol = order.get("tsym", "").replace("-EQ", "")
    order_id = order.get("norenordno")
    action = "buy" if order.get("trantype") == "B" else "sell"
    pid = f"{symbol}_{action}_{order_id}"
    if pid not in active_positions:
        entry_price = float(order.get("avgprc", 0))
        order_time = _parse_time(order.get("exch_tm"))
        sl_price = fetch_sl_price(order_id)
        sl_id = place_stoploss(symbol, action, sl_price, order_id)
        if sl_id:
            active_positions[pid] = {
                "symbol": symbol,
                "action": action,
                "entry_price": entry_price,
                "stoploss_price": sl_price,
                "entry_order_id": order_id,
                "sl_order_id": sl_id,
                "entry_time": order_time,
                "exit_time": calculate_exit_time(order_time)
            }
            print(f"[monitor] SL placed for {pid}: {sl_id}")

# --- Monitor Thread for Pending Orders ---
def monitor_pending():
    while True:
        try:
            now = datetime.now(IST)
            records = sheet.get_all_records()
            orders = fetch_order_book()

            for row in records:
                if row.get("closed_flag") == "Yes":
                    continue

                entry_id = str(row.get("entry_order_id", ""))
                if not entry_id:
                    continue

                status = row.get("status", "")
                symbol = row.get("symbol", "")
                entry_timestamp = row.get("entry_timestamp", "")

                # Determine deadline: use entry_timestamp if possible, else skip (or handle according to your needs)
                if entry_timestamp:
                    today = now.date()
                    try:
                        entry_time = datetime.strptime(entry_timestamp, "%H:%M").replace(
                            year=today.year, month=today.month, day=today.day, tzinfo=IST
                        )
                        deadline = entry_time + timedelta(hours=1)
                    except:
                        continue  # Could not parse timestamp, skip this row
                else:
                    continue  # Cannot determine deadline for rows missing timestamp

                # Find if order is present in order book and its status
                order_found = False
                order_status = ""
                for order in orders:
                    if order.get("norenordno") == entry_id:
                        order_found = True
                        order_status = order.get("status", "")
                        break

                # Cancel stale orders that haven't been filled within their actual next-hour deadline
                if order_found and order_status in ["OPEN", "TRIGGER PENDING"] and now >= deadline:
                    try:
                        api = login_manager.get_api()
                        response = api.cancel_order(entry_id)
                        if response and response.get("stat") == "Ok":
                            send_telegram_alert(f"🚫 Stale Entry Auto-Cancelled: {symbol} | ID: {entry_id} | Deadline: {deadline.strftime('%H:%M')}")
                            update_status_in_sheet(entry_id, "cancelled", "Yes")
                            print(f"[cancel] Stale order {entry_id} cancelled at {now.strftime('%H:%M')}")
                        else:
                            print(f"[error] Could not cancel {entry_id}: {response}")
                    except Exception as e:
                        print(f"[cancel error] Failed to cancel order {entry_id}: {e}")
                # Orders that get COMPLETE within deadline are managed by existing branch as before

        except Exception as e:
            print(f"[monitor_pending error] {e}")
            send_telegram_alert(f"🚨 Monitor Pending error: {e}")

        time.sleep(30)  # Check every 30 seconds


# --- Monitor Active Positions for Market Exit ---
def monitor_active_positions():
    while True:
        now = datetime.now(IST)
        for pid in list(active_positions.keys()):
            pos = active_positions[pid]

            if now >= pos["exit_time"]:
                order_statuses = fetch_order_book()
                sl_filled = False

                for order in order_statuses:
                    if order.get("norenordno") == pos["sl_order_id"]:
                        if order.get("status") == "COMPLETE":
                            sl_filled = True
                            break

                if sl_filled:
                    print(f"[info] SL already filled for {pid}, skipping market exit.")
                    active_positions.pop(pid, None)
                    update_status_in_sheet(pos["entry_order_id"], "exited", "Yes")
                    continue

                api = login_manager.get_api()
                try:
                    api.cancel_order(pos["sl_order_id"])
                    print(f"[cancel] SL cancelled for {pid}: {pos['sl_order_id']}")
                except Exception as e:
                    print(f"[warn] Could not cancel SL for {pid}: {e}")
                reverse = "sell" if pos["action"] == "buy" else "buy"
                price, mkt_order_id = place_market_order(pos["symbol"], reverse)
                update_exit_in_sheet(pos["entry_order_id"], price, mkt_order_id)
                closed_trades.append(pos)
                active_positions.pop(pid, None)
                send_telegram_alert(f"💡 Exit: {pos['symbol']} @ MKT | Entry: ₹{pos['entry_price']} | SL: ₹{pos['stoploss_price']}")
        time.sleep(10)

# --- Startup ---
restore_state_from_sheet()
threading.Thread(target=session_heartbeat, daemon=True).start()
threading.Thread(target=monitor_pending, daemon=True).start()
threading.Thread(target=monitor_active_positions, daemon=True).start()
logger.info("✅ orders.py initialized and monitoring threads started.")
