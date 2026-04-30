# tradecontrol.in 
# https://www.tradecontrol.in/
# Tradecontrol_in AlgoToolKit Sample Code
# Disclaimer: This is a sample code and is provided for educational purposes only. It is not intended to be used for any commercial purposes. 
# Tradecontrol.in is not responsible for any losses incurred by using this code.
# Read the code carefully before using the same.
# To be run inside openalgo.in openalgo run on local machine/ cloud.
# Ideally suitable for use with tradecontrol.in Desktop App.

# ===================================================================
# Import necessary libraries
# ===================================================================
import os
import json
import time
import asyncio
import threading
import zmq
import csv
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, jsonify

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# User Define: 
MASTER_DIR = r'D:\openalgo\strategies\TradeControlAlgoToolKit' # Path to the strategy folder
app = Flask(__name__, 
            template_folder=os.path.join(MASTER_DIR, 'templates'),
            static_folder=os.path.join(MASTER_DIR, 'static'))

# ====================================================================
# SETTINGS SYSTEM (persisted to JSON, editable via UI)
# ====================================================================
SETTINGS_FILE_PATH = r'D:\openalgo\strategies\TradeControlAlgoToolKit\journal_settings.json'

DEFAULT_SETTINGS = {
    'poll_interval_seconds': 15,
    'order_fill_timeout_seconds': 300,
    'sl_market_chase_delay_seconds': 30,
    'default_limit_buffer_pct': 0.05,
    'max_log_lines': 500,
    'eod_square_off_time': '15:15',
    'default_nfo_lot_size': 65,
    'max_trade_duration_hours': 2.0,
    'loss_exit_time_hours': 1.0,
    'loss_exit_percent': 2.0,
    'state_file_path': os.path.join(BASE_DIR, 'journal_active_state.json'),
    'execution_log_path': os.path.join(BASE_DIR, 'trade_executions_journal.csv'),
    'openalgo_api_key': 'YOUR_OPEN_ALGO_KEY',
    'openalgo_host': 'http://127.0.0.1:5000',
    'openalgo_ws_url': 'ws://127.0.0.1:8765',
    'zmq_host': '127.0.0.1',
    'zmq_port': 5555,
    'symbol_db_path': 'D:/openalgo/db/openalgo.db',
    'stale_data_alert_delay': 180,
}

app_settings = dict(DEFAULT_SETTINGS)

def load_settings():
    """Load user settings from JSON, merging with defaults."""
    global app_settings
    if os.path.exists(SETTINGS_FILE_PATH):
        try:
            with open(SETTINGS_FILE_PATH, 'r') as f:
                saved = json.load(f)
            app_settings = {**DEFAULT_SETTINGS, **saved}
            print(f"[SETTINGS] Loaded from {SETTINGS_FILE_PATH}")
        except Exception as e:
            print(f"[SETTINGS] Load error: {e}, using defaults")
            app_settings = dict(DEFAULT_SETTINGS)
    else:
        app_settings = dict(DEFAULT_SETTINGS)

def save_settings():
    """Persist current settings to JSON file."""
    try:
        tmp = SETTINGS_FILE_PATH + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(app_settings, f, indent=2)
        os.replace(tmp, SETTINGS_FILE_PATH)
    except Exception as e:
        print(f"[SETTINGS] Save error: {e}")

def get_setting(key):
    """Get a live setting value."""
    return app_settings.get(key, DEFAULT_SETTINGS.get(key))

load_settings()

# Note: No static aliases — always use get_setting() for live values

# ====================================================================
# OPENALGO CLIENT INITIALIZATION
# ====================================================================
try:
    from openalgo import api as openalgo_api_class
    API_KEY = "YOUR_OPEN_ALGO_KEY"
    HOST = 'http://127.0.0.1:5000'
    WS_URL = "ws://127.0.0.1:8765"
    openalgo_client = openalgo_api_class(api_key=API_KEY, host=HOST, ws_url=WS_URL)
    openalgo_client.connect()
except Exception as e:
    openalgo_client = None
    print(f" openalgo unavailable: {e}. Running in MOCK mode.")

# ====================================================================
# GLOBAL STATE
# ====================================================================
ltp_dict = {}                          # symbol -> LTP from ZMQ
trade_logs = []                        # In-memory log ring buffer
TRADE_LOG_FILE = os.path.join(BASE_DIR, 'trade_logs_persistent.json')
last_heartbeat = 0                     # Last manager heartbeat epoch
last_vwap_check = {}                   # trade_id -> last vwap check time
cached_orderbook = {}                  # Cached order map (refreshed every poll)
cached_positionbook = []               # Cached position book (refreshed every poll)
last_book_refresh = 0                  # Epoch of last orderbook/positionbook fetch

all_trades = []
trade_id_counter = 0
state_lock = threading.RLock()

closed_trades_pnl = {}                 # Tracking per-symbol realized PnL
clean_slate_active = False             # Global emergency shutdown state
ltp_last_update_time = {}              # symbol -> epoch of last LTP

# ====================================================================
# STATE PERSISTENCE (JSON — crash recovery)
# ====================================================================
def save_state():
    """Thread-safe state persistence to JSON file."""
    with state_lock:
        try:
            state_path = get_setting('state_file_path')
            payload = {'trades': all_trades, 'last_id': trade_id_counter}
            tmp_path = state_path + '.tmp'
            with open(tmp_path, 'w') as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp_path, state_path)   # atomic on Windows NTFS
        except Exception as e:
            print(f"[STATE] Save failed: {e}")

def load_state():
    """Load state from JSON on startup for crash recovery."""
    global all_trades, trade_id_counter
    state_path = get_setting('state_file_path')
    if os.path.exists(state_path):
        try:
            with open(state_path, 'r') as f:
                data = json.load(f)
            all_trades = data.get('trades', [])
            trade_id_counter = data.get('last_id', 0)
            running = sum(1 for t in all_trades if t['status'] == 'RUNNING')
            print(f"[STATE] Recovered {len(all_trades)} trades ({running} RUNNING)")
        except Exception as e:
            print(f"[STATE] Load error: {e}")

load_state()

# Load persisted trade logs
def _load_trade_logs():
    global trade_logs
    if os.path.exists(TRADE_LOG_FILE):
        try:
            with open(TRADE_LOG_FILE, 'r', encoding='utf-8') as f:
                trade_logs = json.load(f)
            print(f"[LOGS] Recovered {len(trade_logs)} persisted log entries")
        except Exception as e:
            print(f"[LOGS] Load error: {e}")

def _save_trade_logs():
    try:
        tmp = TRADE_LOG_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(trade_logs[-1000:], f)  # Keep last 1000 entries
        os.replace(tmp, TRADE_LOG_FILE)
    except Exception as e:
        print(f"[LOGS] Save error: {e}")

_load_trade_logs()

# ====================================================================
# LOGGING
# ====================================================================
def algo_log(msg, is_scan=False):
    """Log to in-memory ring buffer and stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    if is_scan:
        # scan-level logs go to stdout only, not to UI terminal
        print(line)
        return
    trade_logs.append(line)
    max_lines = get_setting('max_log_lines')
    if len(trade_logs) > max_lines:
        trade_logs.pop(0)
    print(line)
    # Persist to file periodically (every 10 new entries)
    if len(trade_logs) % 10 == 0:
        _save_trade_logs()

def log_trade_execution(trade_id, symbol, action, qty, price, status, msg=""):
    """Write execution event to CSV trade journal and algo log."""
    exec_path = get_setting('execution_log_path')
    file_exists = os.path.isfile(exec_path)
    try:
        with open(exec_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Time', 'Trade ID', 'Symbol', 'Action', 'Qty', 'Price', 'Status', 'Message'])
            writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             trade_id, symbol, action, qty, price, status, msg])
    except Exception as e:
        print(f"[LOG] CSV write error: {e}")
    algo_log(f" [EXEC] #{trade_id} {symbol} {action} {qty}x @ {price} | {status} | {msg}")

# ====================================================================
# VWAP CALCULATION
# ====================================================================
def get_vwap_3min(symbol, exchange="NSE"):
    """Calculate 3min VWAP from intraday history. Returns (vwap, last_close) or (None, None).
    For options (NFO/BFO) where volume is often zero, falls back to TP-mean."""
    now = datetime.now()
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if not openalgo_client:
        return None, None
    try:
        hist_res = openalgo_client.history(
            symbol=symbol, exchange=exchange, interval="3m",
            start_date=start.strftime("%Y-%m-%d"), end_date=now.strftime("%Y-%m-%d")
        )
    except Exception as e:
        algo_log(f"[VWAP] History API error {symbol} ({exchange}): {e}", is_scan=True)
        return None, None

    if hist_res is None or not isinstance(hist_res, pd.DataFrame) or hist_res.empty:
        return None, None
    df = hist_res
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    last_close = float(df['close'].iloc[-1])
    total_vol = df['volume'].sum()
    if total_vol == 0:
        # Options/illiquid: use simple Typical Price mean as VWAP proxy
        vwap = float(df['tp'].mean())
        algo_log(f"[VWAP] {symbol}({exchange}): Zero volume — using TP-mean VWAP proxy ₹{vwap:.2f}", is_scan=True)
        return vwap, last_close
    vwap = (df['tp'] * df['volume']).sum() / total_vol
    return vwap, last_close

# ====================================================================
# TYPE SAFETY HELPERS
# ====================================================================
def safe_float(val, default=0.0):
    """Safely converts string/None to float without crashing."""
    try:
        if val is None or str(val).strip() == "": return default
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    """Safely converts string/None to int without crashing."""
    try:
        if val is None or str(val).strip() == "": return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

# ====================================================================
# OPENALGO WEBSOCKET CALLBACK
# ====================================================================
def openalgo_quote_callback(data):
    """Callback for OpenAlgo's websocket to update LTPs."""
    try:
        if isinstance(data, str):
            import json
            data = json.loads(data)
            
        payload = data.get('data', data) if isinstance(data, dict) else data
        if not isinstance(payload, dict):
            return
            
        raw_sym = str(payload.get('symbol', payload.get('instrument', ''))).upper()
        if not raw_sym:
            return
            
        sym = raw_sym
        for prefix in ['NSE:', 'NFO:', 'BFO:', 'MCX:', 'BSE:', 'CDS:', 'NSE_INDEX:']:
            sym = sym.replace(prefix, '')
        sym = sym.replace('-EQ', '').strip()
        
        # Absorb feed data freely for ANY explicitly requested ticker
        pass
            
        price = safe_float(payload.get('ltp', payload.get('last_price', 0)))
        
        if price > 0:
            # Log first tick for a symbol (debug: confirms subscription is working)
            is_first_tick = sym not in ltp_dict or ltp_dict[sym] == 0
            
            ltp_dict[sym] = price
            ltp_dict[raw_sym] = price
            ltp_dict[sym + "-EQ"] = price
            
            now_ts = time.time()
            ltp_last_update_time[sym] = now_ts
            ltp_last_update_time[raw_sym] = now_ts
            ltp_last_update_time[sym + "-EQ"] = now_ts
            
            if is_first_tick:
                algo_log(f" [WS] First tick: {sym} (raw: {raw_sym}) → ₹{price}")
    except Exception as e:
        algo_log(f" [WS] Callback error: {e}", is_scan=True)

# ====================================================================
# ZMQ LISTENER (The Proven Pipeline)
# ====================================================================
def start_zmq_listener():
    """Start ZMQ subscriber for real-time LTP data securely."""
    def _listener():
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        
        host = str(get_setting('zmq_host')).strip()
        port = str(get_setting('zmq_port')).strip()
        addr = f"tcp://{host}:{port}"
            
        socket.connect(addr)
        socket.setsockopt(zmq.SUBSCRIBE, b"")
        socket.setsockopt(zmq.RCVTIMEO, 5000)
        for _ in iter(int, 1):
            try:
                msg = socket.recv_multipart()
                topic = msg[0].decode('utf-8')
                data_json = json.loads(msg[1].decode('utf-8'))
                openalgo_quote_callback(data_json)
            except zmq.Again:
                pass
            except Exception:
                time.sleep(0.05)
    threading.Thread(target=_listener, daemon=True).start()

def subscribe_active_trades():
    """Silently ping OpenAlgo to subscribe active trades seamlessly starting the Python WebSocket loop."""
    if not openalgo_client: return
    instruments = []
    
    # Always forcefully subscribe to the Data Health Monitor stocks
    monitor_symbols = ["RELIANCE", "JSWENERGY", "GODREJAGRO", "ICICIBANK", "TCS", "HDFCLIFE", "NIFTYBEES", "SAIL", "OIL", "ONGC", "DMART", "PETRONET", "MCX", "GAIL", "POWERGRID", "IGL"]
    for sym in monitor_symbols:
        instruments.append({'exchange': 'NSE', 'symbol': sym})
        
    for t in all_trades:
        if t['status'] in ['RUNNING', 'PENDING']:
            ex = t.get('parameters', {}).get('exchange', 'NSE')
            instruments.append({'exchange': ex, 'symbol': t['symbol']})
            
    if instruments:
        try:
            unique = list({(i['exchange'], i['symbol']) for i in instruments})
            subs = [{'exchange': e, 'symbol': s} for e, s in unique]
            openalgo_client.subscribe_quote(subs)
            sub_detail = ', '.join([f"{s['exchange']}:{s['symbol']}" for s in subs])
            algo_log(f" [SUB] Subscribed {len(subs)} instruments: {sub_detail}")
        except Exception as e:
            algo_log(f" [SUB] Subscription failed: {e}")

# ====================================================================
# BROKER API HELPERS
# ====================================================================
def refresh_books():
    """Fetch orderbook and positionbook from broker. Called once per poll cycle."""
    global cached_orderbook, cached_positionbook, last_book_refresh
    if not openalgo_client:
        return
    now = time.time()
    if (now - last_book_refresh) < 10:   # don't refresh more than once per 10s
        return
    last_book_refresh = now
    try:
        ob = openalgo_client.orderbook()
        if ob and ob.get('status') == 'success':
            orders = ob['data'].get('orders', ob['data'])
            if isinstance(orders, list):
                cached_orderbook = {str(o.get('orderid')): o for o in orders}
    except Exception as e:
        algo_log(f"[BOOKS] Orderbook fetch error: {e}", is_scan=True)
    try:
        pb = openalgo_client.positionbook()
        if pb and pb.get('status') == 'success':
            cached_positionbook = pb['data'] if isinstance(pb['data'], list) else []
    except Exception as e:
        algo_log(f"[BOOKS] Positionbook fetch error: {e}", is_scan=True)

def check_fill(order_id, order_map, symbol=None):
    """Check if an order is filled, rejected, or still pending.
    Returns: (True, qty, price) | ('FAILED', 0, 0) | (False, 0, 0)
    """
    if not order_id:
        return False, 0.0, 0.0
    oid = str(order_id)
    if oid in order_map:
        status = str(order_map[oid].get('order_status', '')).lower()
        if status in ['complete', 'filled']:
            qty = float(order_map[oid].get('filledquantity', order_map[oid].get('quantity', 0)))
            price = float(order_map[oid].get('average_price', 0.0))
            return True, qty, price
        elif status in ['rejected', 'cancelled']:
            return 'FAILED', 0.0, 0.0
    
    return False, 0.0, 0.0

def place_order_safe(symbol, action, exchange, price_type, product, quantity, price=0, trigger_price=0):
    """Place order with error handling. Returns order_id string or ''."""
    if not openalgo_client:
        return ''
    try:
        kwargs = dict(strategy="JOURNAL", symbol=symbol, action=action, exchange=exchange,
                      price_type=price_type, product=product, quantity=quantity, price=price)
        if trigger_price > 0:
            kwargs['trigger_price'] = trigger_price
        res = openalgo_client.placeorder(**kwargs)
        return str(res.get('orderid', '')) if res else ''
    except Exception as e:
        algo_log(f" [ORDER] Place failed {symbol} {action}: {e}")
        return ''

def cancel_order_safe(order_id):
    """Cancel an order with error handling."""
    if not openalgo_client or not order_id:
        return
    try:
        openalgo_client.cancelorder(order_id=str(order_id))
    except Exception as e:
        algo_log(f"[ORDER] Cancel failed OID:{order_id}: {e}", is_scan=True)

# ====================================================================
# TRIGGER CONDITION CHECK
# ====================================================================
def check_trigger_condition(ltp, target_px, trigger_dir, buffer_pct):
    """Check if LTP satisfies the trigger condition within buffer bounds."""
    if ltp <= 0 or target_px <= 0:
        return False
    bounds = target_px * (buffer_pct / 100.0)
    if trigger_dir == 'Below':
        return ltp >= (target_px - bounds)
    elif trigger_dir == 'Above':
        return ltp <= (target_px + bounds)
    return False

# ====================================================================
# CORE ALGO MANAGER — runs every ALGO_POLL_INTERVAL_SECONDS via asyncio
# ====================================================================
def execute_controlled_exit(trade, reason, sym, params, ex, product, exit_action, rem_qty):
    try:
        if params.get('sl_order_id'):
            cancel_order_safe(params['sl_order_id'])
            params.pop('sl_order_id', None)
        ltp = ltp_dict.get(sym, 0)
        limit_px = round(ltp * 1.005, 2) if exit_action == "BUY" else round(ltp * 0.995, 2)
        if limit_px <= 0: limit_px = 0
        oid = place_order_safe(sym, exit_action, ex, "LIMIT" if limit_px > 0 else "MARKET", product, rem_qty, price=limit_px)
        log_trade_execution(trade['id'], sym, exit_action, rem_qty, limit_px if limit_px > 0 else 'MARKET', reason + "_LIMIT", f"OID:{oid}")
        params['exit_chase'] = {'oid': oid, 'time': time.time(), 'qty': rem_qty, 'reason': reason}
        return True
    except Exception as e:
        algo_log(f" #{trade['id']} Controlled exit failed: {e}")
        return False

def _force_exit_trade(trade, reason):
    params = trade['parameters']
    sym = trade['symbol']
    algo = trade['algo_type']
    ex = params.get('exchange', 'NSE')
    product = params.get('product', 'CNC')
    exit_action = "BUY" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "SELL"
    
    if params.get('pending_entry_order_id'):
        cancel_order_safe(params['pending_entry_order_id'])
        params.pop('pending_entry_order_id', None)
        
    rem_qty = int(params.get('remaining_qty', 0))
    if rem_qty > 0:
        # Cancel any pending orders: SL, exit_chase, manual_exit_chase
        for key in ['sl_order_id', 'exit_chase', 'manual_exit_chase', 'sl_chase_order_id']:
            if key in params:
                oid = params[key] if isinstance(params[key], str) else params[key].get('oid', '') if isinstance(params[key], dict) else ''
                if oid:
                    cancel_order_safe(oid)
                params.pop(key, None)
        exit_oid = place_order_safe(sym, exit_action, ex, "MARKET", product, rem_qty)
        log_trade_execution(trade['id'], sym, exit_action, rem_qty, 'MARKET', reason, f"OID:{exit_oid}")
        _record_exit_pnl(trade, rem_qty)
        params['remaining_qty'] = 0
    trade['status'] = 'COMPLETED'
    return True

def _record_exit_pnl(trade, exit_qty, exit_price=0):
    """Record realized PnL for any exit. Uses LTP if exit_price unknown."""
    sym = trade['symbol']
    algo = trade['algo_type']
    epx = safe_float(trade['parameters'].get('entry_price', 0))
    if exit_price <= 0:
        exit_price = ltp_dict.get(sym, 0)
    if epx <= 0 or exit_price <= 0 or exit_qty <= 0:
        return
    is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
    pnl = (epx - exit_price) * exit_qty if is_short else (exit_price - epx) * exit_qty
    closed_trades_pnl[sym] = closed_trades_pnl.get(sym, 0.0) + pnl
    algo_log(f" #{trade['id']} {sym} PnL: {'+'if pnl>=0 else ''}₹{pnl:.2f} ({exit_qty}x @ {exit_price})")

def run_manager_check():
    """Main algo manager tick. Processes all RUNNING trades."""
    global last_heartbeat
    last_heartbeat = time.time()
    
    run_manager_check.last_heartbeat = getattr(run_manager_check, 'last_heartbeat', 0)
    last_heartbeat = time.time()
    
    # Websocket stale monitoring checks removed as per User Request.

    running_count = sum(1 for t in all_trades if t['status'] == 'RUNNING')
    if running_count > 0:
        algo_log(f" Manager tick | {running_count} RUNNING | LTPs: {dict(list(ltp_dict.items())[:5])}", is_scan=True)

    refresh_books()
    order_map = cached_orderbook
    state_changed = False
    
    try: eod_time = datetime.strptime(get_setting('eod_square_off_time'), "%H:%M").time()
    except: eod_time = None
    is_eod = False
    if eod_time and datetime.now().time() >= eod_time:
        is_eod = True

    with state_lock:
        for trade in all_trades:
            if trade['status'] != 'RUNNING':
                continue

            try:
                if clean_slate_active:
                    changed = _force_exit_trade(trade, "CLEAN_SLATE")
                elif is_eod:
                    changed = _force_exit_trade(trade, "EOD_SQUARE_OFF")
                else:
                    changed = _process_single_trade(trade, order_map)
                if changed:
                    state_changed = True
            except Exception as e:
                algo_log(f" [ERROR] Trade #{trade['id']} {trade['symbol']}: {e}")

        if state_changed:
            save_state()

def _process_single_trade(trade, order_map):
    """Process a single trade through its lifecycle. Returns True if state changed."""
    algo = trade['algo_type']
    params = trade['parameters']
    sym = trade['symbol']
    ex = params.get('exchange', 'NSE')
    product = params.get('product', 'CNC')
    ltp = ltp_dict.get(sym, 0)

    # ── PHASE 0: EXPIRY CHECK ──────────────────────────────────
    cancel_by_str = params.get('cancel_by')
    if cancel_by_str and not params.get('entry_time'):
        try:
            dt = datetime.strptime(cancel_by_str[:16], "%Y-%m-%dT%H:%M")
            if datetime.now() >= dt:
                if params.get('pending_entry_order_id'):
                    cancel_order_safe(params['pending_entry_order_id'])
                trade['status'] = 'CANCELLED'
                algo_log(f" #{trade['id']} {sym} auto-cancelled (expired)")
                return True
        except Exception:
            pass

    # ── PHASE 1: PENDING ENTRY FILL CHECK ──────────────────────
    pending_oid = params.get('pending_entry_order_id')
    if pending_oid and not params.get('entry_time'):
        filled, f_qty, f_px = check_fill(pending_oid, order_map, sym)

        if filled is True:
            log_trade_execution(trade['id'], sym, "ENTRY", f_qty, f_px, "FILLED")
            params['entry_time'] = time.time()
            params['entry_price'] = f_px
            if algo == 'BRACKET_ACCUMULATION':
                params['remaining_qty'] = int(params.get('remaining_qty', 0)) + int(f_qty)
            else:
                params['remaining_qty'] = int(f_qty)
            params.setdefault('order_log', []).append({
                'action': 'ENTRY', 'qty': int(f_qty), 'price': f_px,
                'order_id': str(pending_oid), 'time': time.time()
            })

            # Place initial SL order
            sl_trig = safe_float(params.get('stop_loss', 0))
            if sl_trig > 0 and algo != 'BRACKET_ACCUMULATION':
                sl_action = "BUY" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "SELL"
                sl_exit_px = safe_float(params.get('sl_exit_price', 0))
                sl_limit = sl_exit_px if sl_exit_px > 0 else sl_trig
                sl_oid = place_order_safe(sym, sl_action, ex, "SL", product, int(f_qty),
                                          price=sl_limit, trigger_price=sl_trig)
                params['sl_order_id'] = sl_oid
                if sl_oid:
                    algo_log(f" #{trade['id']} {sym} SL placed @ {sl_trig} (OID:{sl_oid})")

            params.pop('pending_entry_order_id', None)
            return True

        elif filled == 'FAILED':
            log_trade_execution(trade['id'], sym, "ENTRY", 0, 0, "REJECTED",
                                f"OID:{pending_oid}")
            trade['status'] = 'CANCELLED'
            params.pop('pending_entry_order_id', None)
            return True

        else:
            # Still pending — check timeout
            elapsed = time.time() - params.get('entry_placed_time', time.time())
            if elapsed > get_setting('order_fill_timeout_seconds'):
                cancel_order_safe(pending_oid)
                time.sleep(1.0)
                refresh_books() # forces a fresh lookup immediately
                final_chk, f_q, f_p = check_fill(pending_oid, cached_orderbook, sym)
                if final_chk is True:
                    algo_log(f" #{trade['id']} POST-CANCEL FILL DETECTED! Proceeding to active management.")
                    log_trade_execution(trade['id'], sym, "ENTRY", f_q, f_p, "FILLED_POST_CANCEL")
                    params['entry_time'] = time.time()
                    params['entry_price'] = f_p
                    params['remaining_qty'] = int(f_q)
                    params.pop('pending_entry_order_id', None)
                    return True
                else:
                    log_trade_execution(trade['id'], sym, "ENTRY", 0, 0, "TIMEOUT_CANCEL")
                    trade['status'] = 'CANCELLED'
                    params.pop('pending_entry_order_id', None)
                    return True
            return False   # Still waiting, no state change

    # ── PHASE 2: ENTRY TRIGGER ─────────────────────────────────
    can_enter = not params.get('pending_entry_order_id') and ltp > 0
    if algo != 'BRACKET_ACCUMULATION' and params.get('entry_time'):
        can_enter = False
        
    if can_enter:
        if algo == 'BRACKET_ACCUMULATION':
            freq_mins = float(params.get('frequency', 3))
            last_hit = params.get('last_bracket_buy_time', 0)
            if (time.time() - last_hit) < (freq_mins * 60):
                can_enter = False

    if can_enter:
        entry_px = safe_float(params.get('entry_level', params.get('buy_level', 0)))
        qty = safe_int(params.get('qty', params.get('sell_qty', 0)))
        action = "SELL" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "BUY"

        trigger_satisfied = False
        if algo in ['VWAP_LONG', 'VWAP_SHORT']:
            trigger_satisfied = check_trigger_condition(
                ltp, entry_px, params.get('trigger_dir', 'Below'),
                safe_float(params.get('buffer_pct', get_setting('default_limit_buffer_pct')))
            )
        else:
            buy_trigger_dir = str(params.get('buy_trigger_dir', 'none')).lower()
            if action == "BUY":
                if buy_trigger_dir == 'above':
                    # Price rises to/above buy_level (from below)
                    trigger_satisfied = (ltp >= entry_px)
                elif buy_trigger_dir == 'below':
                    # Price drops to/below buy_level (from above) 
                    trigger_satisfied = (ltp <= entry_px)
                else:
                    # 'none'/immediate: buy at level or market if already below
                    trigger_satisfied = (ltp <= entry_px)
            else:
                trigger_satisfied = (ltp >= entry_px)

        if trigger_satisfied and qty > 0 and entry_px > 0:
            if algo == 'BRACKET_ACCUMULATION' and params.get('t1_triggered'):
                pass
            else:
                oid = place_order_safe(sym, action, ex, "LIMIT", product, qty, price=entry_px)
                if oid:
                    params['pending_entry_order_id'] = oid
                    params['entry_placed_time'] = time.time()
                    if algo == 'BRACKET_ACCUMULATION':
                        params['last_bracket_buy_time'] = time.time()
                    algo_log(f" #{trade['id']} {sym} {action} LIMIT @ {entry_px} placed (OID:{oid})")
                    return True

    # ── PHASE 3: ACTIVE POSITION MANAGEMENT ────────────────────
    rem_qty = int(params.get('remaining_qty', 0))
    if not params.get('entry_time') or rem_qty <= 0 or ltp <= 0:
        return False

    exit_action = "BUY" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "SELL"

    # ── 3.0: PENDING EXIT CHASES (must be checked FIRST!) ─────
    # If there's already an exit order pending, don't fire new ones
    chase_info = params.get('exit_chase')
    if chase_info:
        filled, _, f_px = check_fill(chase_info['oid'], order_map, sym)
        if filled is True:
            log_trade_execution(trade['id'], sym, exit_action, chase_info['qty'], f_px, chase_info['reason']+"_FILLED")
            _record_exit_pnl(trade, chase_info['qty'], f_px if f_px > 0 else 0)
            params['remaining_qty'] = 0
            trade['status'] = 'COMPLETED'
            params.pop('exit_chase', None)
            return True
        elif filled == 'FAILED' or (time.time() - chase_info['time']) > get_setting('sl_market_chase_delay_seconds'):
            cancel_order_safe(chase_info['oid'])
            algo_log(f" #{trade['id']} {sym} LIMIT Chase failed/timeout. Going MARKET.")
            oid = place_order_safe(sym, exit_action, ex, "MARKET", product, chase_info['qty'])
            log_trade_execution(trade['id'], sym, exit_action, chase_info['qty'], 'MARKET', chase_info['reason']+"_MARKET_CHASE", f"OID:{oid}")
            _record_exit_pnl(trade, chase_info['qty'])
            params['remaining_qty'] = 0
            trade['status'] = 'COMPLETED'
            params.pop('exit_chase', None)
            return True
        return False

    # ── 3.0b: PENDING MANUAL EXIT CHASE ───────────────────────
    m_chase = params.get('manual_exit_chase')
    if m_chase:
        m_filled, m_qty, m_px = check_fill(m_chase['oid'], order_map, sym)
        if m_filled is True:
            log_trade_execution(trade['id'], sym, exit_action, int(m_qty), m_px, "MANUAL_EXIT_FILLED", f"OID:{m_chase['oid']}")
            _record_exit_pnl(trade, int(m_qty), m_px)
            params['remaining_qty'] = max(0, rem_qty - int(m_qty))
            if params['remaining_qty'] <= 0:
                trade['status'] = 'COMPLETED'
            else:
                old_sl = safe_float(m_chase.get('reinstate_sl', 0))
                if old_sl > 0:
                    sl_action = "BUY" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "SELL"
                    sl_exit_px = safe_float(params.get('sl_exit_price', 0))
                    sl_limit = sl_exit_px if sl_exit_px > 0 else old_sl
                    new_sl_oid = place_order_safe(sym, sl_action, ex, "SL", product, params['remaining_qty'], price=sl_limit, trigger_price=old_sl)
                    if new_sl_oid:
                        params['sl_order_id'] = new_sl_oid
                        algo_log(f" #{trade['id']} {sym} Partial exit. SL rebuilt for {params['remaining_qty']} qty @ {old_sl}.")
            params.pop('manual_exit_chase', None)
            return True
        elif m_filled == 'FAILED' or (time.time() - m_chase['time']) > get_setting('order_fill_timeout_seconds'):
            cancel_order_safe(m_chase['oid'])
            algo_log(f" #{trade['id']} {sym} Manual Exit Failed/Timeout! Rebuilding old Stop Loss...")
            old_sl = safe_float(m_chase.get('reinstate_sl', 0))
            if old_sl > 0:
                sl_action = "BUY" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "SELL"
                sl_exit_px = safe_float(params.get('sl_exit_price', 0))
                sl_limit = sl_exit_px if sl_exit_px > 0 else old_sl
                new_sl_oid = place_order_safe(sym, sl_action, ex, "SL", product, rem_qty, price=sl_limit, trigger_price=old_sl)
                if new_sl_oid:
                    params['sl_order_id'] = new_sl_oid
            params.pop('manual_exit_chase', None)
            return True
        return False

    # ── 3.1: PER-TRADE HOLD TIME CHECK ────────────────────────
    fill_time = params.get('entry_time')
    if fill_time:
        dur_hrs = (time.time() - fill_time) / 3600.0

        # Per-trade hold time from form (hold_val + hold_unit)
        hold_val = safe_float(params.get('hold_val', 0))
        hold_unit = str(params.get('hold_unit', 'h'))
        if hold_val > 0:
            if hold_unit == 'm': max_dur_hrs = hold_val / 60.0
            elif hold_unit == 'd': max_dur_hrs = hold_val * 24.0
            elif hold_unit == 'w': max_dur_hrs = hold_val * 168.0
            else: max_dur_hrs = hold_val  # default hours
            if dur_hrs > max_dur_hrs:
                algo_log(f" #{trade['id']} {sym} Hold time exceeded ({hold_val}{hold_unit} = {max_dur_hrs:.2f}hrs). Exit!")
                return _force_exit_trade(trade, f"HOLD_TIME_{hold_val}{hold_unit}")

        # Global max duration fallback
        if dur_hrs > float(get_setting('max_trade_duration_hours')):
            algo_log(f" #{trade['id']} {sym} Max duration exceeded. Market exit!")
            return _force_exit_trade(trade, "MAX_DURATION")

        # Time-based loss check (global settings)
        if dur_hrs > safe_float(get_setting('loss_exit_time_hours')):
            epx = safe_float(params.get('entry_price', 0))
            if epx > 0:
                loss_pct = ((epx - ltp) / epx * 100) if exit_action == "SELL" else ((ltp - epx) / epx * 100)
                if loss_pct > safe_float(get_setting('loss_exit_percent')):
                    algo_log(f" #{trade['id']} {sym} Time-based loss exit ({loss_pct:.1f}%).")
                    return execute_controlled_exit(trade, f"TIME_LOSS_{loss_pct:.1f}%", sym, params, ex, product, exit_action, rem_qty)

    # ── 3.1b: TIME-BOUND STOP LOSS ───────────────────────────
    if params.get('time_sl_enabled'):
        time_sl_mins = safe_float(params.get('time_sl_after_mins', 0))
        time_sl_price = safe_float(params.get('time_sl_price', 0))
        if fill_time and time_sl_mins > 0 and time_sl_price > 0:
            elapsed_mins = (time.time() - fill_time) / 60.0
            if elapsed_mins >= time_sl_mins:
                is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
                sl_breached = (ltp >= time_sl_price) if is_short else (ltp <= time_sl_price)
                if sl_breached:
                    algo_log(f" #{trade['id']} {sym} Time-bound SL hit after {elapsed_mins:.0f}min (LTP:{ltp} vs SL:{time_sl_price})")
                    return execute_controlled_exit(trade, f"TIME_SL_{elapsed_mins:.0f}min", sym, params, ex, product, exit_action, rem_qty)

    t1_px = safe_float(params.get('target', params.get('target1', params.get('exit_level', 0))))
    t2_px = safe_float(params.get('target2', 0))
    sl_trig = safe_float(params.get('stop_loss', 0))

    # ── 3a: SL ORDER STATUS CHECK + MARKET CHASE ──────────────
    sl_oid = params.get('sl_order_id')
    if sl_oid:
        sl_filled, sl_qty, sl_px = check_fill(sl_oid, order_map, sym)
        if sl_filled is True:
            log_trade_execution(trade['id'], sym, exit_action, int(sl_qty), sl_px, "SL_FILLED", f"OID:{sl_oid}")
            _record_exit_pnl(trade, int(sl_qty), sl_px)
            params['remaining_qty'] = max(0, rem_qty - int(sl_qty))
            params['sl_triggered'] = True
            params.pop('sl_order_id', None)
            if params['remaining_qty'] <= 0:
                trade['status'] = 'COMPLETED'
            return True
        elif sl_filled == 'FAILED':
            algo_log(f" #{trade['id']} {sym} SL REJECTED! Initiating MARKET chase...")
            params.pop('sl_order_id', None)
            params['sl_market_chase_time'] = time.time()
            chase_oid = place_order_safe(sym, exit_action, ex, "MARKET", product, rem_qty)
            if chase_oid:
                params['sl_chase_order_id'] = chase_oid
                log_trade_execution(trade['id'], sym, exit_action, rem_qty, 'MARKET', "SL_MARKET_CHASE", f"OID:{chase_oid}")
            return True

    # ── 3b: SL MARKET CHASE FILL CHECK ────────────────────────
    chase_oid = params.get('sl_chase_order_id')
    if chase_oid:
        chase_filled, ch_qty, ch_px = check_fill(chase_oid, order_map, sym)
        if chase_filled is True:
            log_trade_execution(trade['id'], sym, exit_action, int(ch_qty), ch_px, "SL_CHASE_FILLED", f"OID:{chase_oid}")
            _record_exit_pnl(trade, int(ch_qty), ch_px)
            params['remaining_qty'] = 0
            params['sl_triggered'] = True
            params.pop('sl_chase_order_id', None)
            trade['status'] = 'COMPLETED'
            return True
        elif chase_filled == 'FAILED':
            algo_log(f" #{trade['id']} {sym} SL MARKET CHASE ALSO REJECTED!")
            params.pop('sl_chase_order_id', None)
            params['open_position'] = True
            trade['status'] = 'CANCELLED'
            return True

    # ── 3c: LTP-based SL FALLBACK ─────────────────────────────
    if sl_trig > 0 and not params.get('sl_triggered') and not sl_oid:
        is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
        sl_breached = (ltp >= sl_trig) if is_short else (ltp <= sl_trig)
        if sl_breached:
            algo_log(f" #{trade['id']} {sym} LTP breached SL {sl_trig}. MARKET exit!")
            chase_oid = place_order_safe(sym, exit_action, ex, "MARKET", product, rem_qty)
            if chase_oid:
                params['sl_chase_order_id'] = chase_oid
                log_trade_execution(trade['id'], sym, exit_action, rem_qty, 'MARKET', "SL_LTP_BREACH", f"OID:{chase_oid}")
            params['sl_triggered'] = True
            return True

    # ── 3d: SL ORDER TIMEOUT CHASE ────────────────────────────
    if sl_oid and sl_trig > 0 and not params.get('sl_triggered'):
        is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
        sl_breached = (ltp >= sl_trig) if is_short else (ltp <= sl_trig)
        if sl_breached:
            breach_time = params.get('sl_breach_time')
            if not breach_time:
                params['sl_breach_time'] = time.time()
            elif (time.time() - breach_time) > get_setting('sl_market_chase_delay_seconds'):
                algo_log(f" #{trade['id']} {sym} SL not filled in {get_setting('sl_market_chase_delay_seconds')}s. MARKET chase!")
                cancel_order_safe(sl_oid)
                params.pop('sl_order_id', None)
                chase_oid = place_order_safe(sym, exit_action, ex, "MARKET", product, rem_qty)
                if chase_oid:
                    params['sl_chase_order_id'] = chase_oid
                    log_trade_execution(trade['id'], sym, exit_action, rem_qty, 'MARKET', "SL_TIMEOUT_CHASE", f"OID:{chase_oid}")
                params['sl_triggered'] = True
                return True
        else:
            params.pop('sl_breach_time', None)

    # ── 3e: VWAP EXIT LOGIC ───────────────────────────────────
    if algo in ['VWAP_LONG', 'VWAP_SHORT']:
        trade_id = trade['id']
        if (time.time() - last_vwap_check.get(trade_id, 0)) > 60:
            vwap, last_close = get_vwap_3min(sym, ex)
            last_vwap_check[trade_id] = time.time()
            if vwap and last_close:
                is_long = (algo == 'VWAP_LONG')
                vwap_exit = (is_long and last_close < vwap) or (not is_long and last_close > vwap)
                if vwap_exit:
                    cancel_order_safe(params.get('sl_order_id'))
                    params.pop('sl_order_id', None)
                    exit_oid = place_order_safe(sym, exit_action, ex, "MARKET", product, rem_qty)
                    log_trade_execution(trade['id'], sym, exit_action, rem_qty, 'MARKET',
                                        "VWAP_EXIT", f"VWAP:{vwap:.2f} Close:{last_close:.2f} OID:{exit_oid}")
                    _record_exit_pnl(trade, rem_qty)
                    params['remaining_qty'] = 0
                    trade['status'] = 'COMPLETED'
                    return True

    # ── 3f: TARGET LOGIC ──────────────────────────────────────
    if t1_px > 0 and not params.get('t1_triggered'):
        is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
        t1_hit = (ltp <= t1_px) if is_short else (ltp >= t1_px)
        if t1_hit:
            t1_qty = safe_int(params.get('target1_qty', 0))
            if t1_qty <= 0 or algo == 'BRACKET_ACCUMULATION':
                t1_qty = rem_qty // 2 if (t2_px > 0 and algo != 'BRACKET_ACCUMULATION') else rem_qty
            t1_qty = min(t1_qty, rem_qty)
            if t1_qty > 0:
                oid = place_order_safe(sym, exit_action, ex, "LIMIT", product, t1_qty, price=t1_px)
                log_trade_execution(trade['id'], sym, exit_action, t1_qty, t1_px, "T1_HIT", f"OID:{oid}")
                _record_exit_pnl(trade, t1_qty, t1_px)
                params['t1_triggered'] = True
                if oid: params['t1_order_id'] = oid
                params.setdefault('order_log', []).append({
                    'action': exit_action, 'qty': t1_qty, 'price': t1_px,
                    'order_id': oid, 'time': time.time()
                })
                params['remaining_qty'] = rem_qty - t1_qty
                if params['remaining_qty'] <= 0:
                    trade['status'] = 'COMPLETED'
                return True

    if t2_px > 0 and not params.get('t2_triggered') and params.get('t1_triggered'):
        rem_now = int(params.get('remaining_qty', 0))
        if rem_now > 0:
            is_short = algo in ['SHORT_MIS', 'VWAP_SHORT']
            t2_hit = (ltp <= t2_px) if is_short else (ltp >= t2_px)
            if t2_hit:
                cancel_order_safe(params.get('sl_order_id'))
                params.pop('sl_order_id', None)
                t2_qty = safe_int(params.get('target2_qty', 0))
                if t2_qty <= 0:
                    t2_qty = rem_now
                t2_qty = min(t2_qty, rem_now)
                oid = place_order_safe(sym, exit_action, ex, "LIMIT", product, t2_qty, price=t2_px)
                log_trade_execution(trade['id'], sym, exit_action, t2_qty, t2_px, "T2_HIT", f"OID:{oid}")
                _record_exit_pnl(trade, t2_qty, t2_px)
                params['t2_triggered'] = True
                if oid: params['t2_order_id'] = oid
                params.setdefault('order_log', []).append({
                    'action': exit_action, 'qty': t2_qty, 'price': t2_px,
                    'order_id': oid, 'time': time.time()
                })
                params['remaining_qty'] = rem_now - t2_qty
                if params['remaining_qty'] <= 0:
                    trade['status'] = 'COMPLETED'
                return True

    return False

# ====================================================================
# TIMER MANAGER (Zero While Loops)
# ====================================================================
def start_algo_manager_async():
    """Start event scheduling that polls run_manager_check every N seconds."""
    def _loop():
        for _ in iter(int, 1):
            try:
                run_manager_check()
            except Exception as e:
                print(f"[MANAGER] Error in tick: {e}")
            time.sleep(safe_int(get_setting('poll_interval_seconds'), 15))

    threading.Thread(target=_loop, daemon=True).start()

# ====================================================================
# FLASK ROUTES
# ====================================================================
@app.route('/api/clean_slate', methods=['POST'])
def trigger_clean_slate():
    global clean_slate_active
    data = request.json
    pwd = data.get('password')
    if pwd != 'CleanSlate':
        return jsonify({'error': 'Invalid password for Clean Slate Protocol'}), 401
    clean_slate_active = True
    algo_log(" CLEAN SLATE PROTOCOL ACTIVATED! Shutting down all trading! ", True)
    return jsonify({'status': 'success', 'message': 'Clean Slate Activated.'})

@app.route('/api/dashboard_metrics', methods=['GET'])
def get_dashboard_metrics():
    # Stale data detection based on high-volume monitor stocks
    monitor_symbols = ["RELIANCE", "JSWENERGY", "GODREJAGRO", "ICICIBANK", "TCS", "HDFCLIFE", "NIFTYBEES", "SAIL", "OIL", "ONGC", "DMART", "PETRONET", "MCX", "GAIL", "POWERGRID", "IGL"]
    
    current_time = time.time()
    stale_count = 100  # Default to stale (>10 triggers FE warning)
    
    most_recent_monitor = 0
    for sym in monitor_symbols:
        t = ltp_last_update_time.get(sym, ltp_last_update_time.get(sym + "-EQ", 0))
        if t > most_recent_monitor:
            most_recent_monitor = t
        if t > 0 and (current_time - t) <= 45:
            stale_count = 0  # Data is OK! At least one monitor stock is moving
            
    stale_sec_val = (current_time - most_recent_monitor) if most_recent_monitor > 0 else 999999
            
    realized = sum(closed_trades_pnl.values()) if closed_trades_pnl else 0
    unrealized = 0
    # Add basic unrealized pnl approximation
    for t in all_trades:
        if t['status'] == 'RUNNING' and int(t.get('parameters', {}).get('remaining_qty', 0)) > 0:
            sym = t['symbol']
            epx = float(t['parameters'].get('entry_price', 0))
            ltp = ltp_dict.get(sym, 0)
            algo = t['algo_type']
            action = "SELL" if algo in ['SHORT_MIS', 'VWAP_SHORT'] else "BUY"
            if epx > 0 and ltp > 0:
                pnl = (ltp - epx) if action == "BUY" else (epx - ltp)
                unrealized += pnl * int(t['parameters']['remaining_qty'])
    return jsonify({
        'clean_slate_active': clean_slate_active,
        'stale_symbols_count': stale_count,
        'realized_pnl': round(realized, 2),
        'unrealized_pnl': round(unrealized, 2),
        'total_pnl': round(realized + unrealized, 2),
        'stale_data_alert_delay': safe_int(get_setting('stale_data_alert_delay'), 180),
        'stale_seconds': stale_sec_val
    })

@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/audio/<path:filename>')
def serve_audio(filename):
    from flask import send_from_directory
    import os
    audio_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    return send_from_directory(audio_dir, filename)

@app.route('/api/logs', methods=['GET'])
def get_logs():
    since = int(request.args.get('since', 0))
    return jsonify({'logs': trade_logs[since:], 'total': len(trade_logs)})

@app.route('/api/trade_log/download', methods=['GET'])
def download_trade_log():
    """Download the persistent trade execution CSV log."""
    exec_path = get_setting('execution_log_path')
    if os.path.isfile(exec_path):
        from flask import send_file
        return send_file(exec_path, as_attachment=True,
                         download_name=f'trade_log_{datetime.now().strftime("%Y%m%d")}.csv',
                         mimetype='text/csv')
    # If no CSV exists yet, generate one from in-memory logs
    import io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Timestamp', 'Log Entry'])
    for line in trade_logs:
        # Extract timestamp from [HH:MM:SS] prefix
        ts = line[1:20] if line.startswith('[') else ''
        msg = line[22:] if line.startswith('[') else line
        writer.writerow([ts, msg])
    output.seek(0)
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=trade_log_{datetime.now().strftime("%Y%m%d")}.csv'}
    )

@app.route('/api/bot_status', methods=['GET'])
def bot_status():
    alive = (time.time() - last_heartbeat) < (safe_int(get_setting('poll_interval_seconds'), 15) * 3)
    return jsonify({'status': 'live' if alive else 'offline', 'last_heartbeat': last_heartbeat})

@app.route('/api/symbols', methods=['GET'])
def search_symbols():
    q = request.args.get('q', '').upper()
    exchange = request.args.get('exchange', '').upper()
    rich = request.args.get('rich', '0')  # If rich=1, return full metadata objects
    if not q:
        return jsonify([])
    try:
        import sqlite3
        db_path = get_setting('symbol_db_path')
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        cursor = conn.cursor()
        if rich == '1':
            # Full metadata mode — intelligent symbol lookup
            if exchange:
                cursor.execute(
                    "SELECT symbol, exchange, name, lotsize, instrumenttype, expiry, strike "
                    "FROM symtoken WHERE symbol LIKE ? AND exchange = ? ORDER BY symbol LIMIT 20",
                    (f"{q}%", exchange)
                )
            else:
                cursor.execute(
                    "SELECT symbol, exchange, name, lotsize, instrumenttype, expiry, strike "
                    "FROM symtoken WHERE symbol LIKE ? ORDER BY exchange, symbol LIMIT 20",
                    (f"{q}%",)
                )
            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol': row[0],
                    'exchange': row[1] or 'NSE',
                    'name': row[2] or '',
                    'lotsize': row[3] or 1,
                    'instrumenttype': row[4] or 'EQ',
                    'expiry': row[5] or '',
                    'strike': row[6] or 0,
                })
            conn.close()
            return jsonify(results)
        else:
            # Legacy simple mode
            if exchange:
                cursor.execute("SELECT DISTINCT symbol FROM symtoken WHERE symbol LIKE ? AND exchange = ? LIMIT 15", (f"{q}%", exchange))
            else:
                cursor.execute("SELECT DISTINCT symbol FROM symtoken WHERE symbol LIKE ? LIMIT 15", (f"{q}%",))
            symbols = [row[0] for row in cursor.fetchall()]
            conn.close()
            return jsonify(symbols)
    except Exception:
        return jsonify([])

@app.route('/api/trades', methods=['GET'])
def get_trades():
    sorted_trades = sorted(all_trades, key=lambda x: x.get('created_at', ''), reverse=True)
    for t in sorted_trades:
        sym = t.get('symbol', '').upper()
        rem_qty = int(t.get('parameters', {}).get('remaining_qty', 0))
        epx = float(t.get('parameters', {}).get('entry_price', 0))
        ltp = ltp_dict.get(sym, 0)
        action = "SELL" if t.get('algo_type') in ['SHORT_MIS', 'VWAP_SHORT'] else "BUY"
        pnl = 0.0
        if rem_qty > 0 and epx > 0 and ltp > 0:
            pnl = (ltp - epx) * rem_qty if action == "BUY" else (epx - ltp) * rem_qty
        t['live_pnl'] = round(pnl, 2)
        t['ltp'] = ltp
    return jsonify({'trades': sorted_trades})

@app.route('/api/ltp/<symbol>', methods=['GET'])
def get_ltp(symbol):
    sym = symbol.upper()
    exchange = request.args.get('exchange', 'NSE')
    ltp = ltp_dict.get(sym, 0.0)
    if ltp == 0.0 and openalgo_client:
        try:
            openalgo_client.subscribe_quote([{'exchange': exchange, 'symbol': sym}])
            algo_log(f" [SUB] LTP request triggered subscribe: {exchange}:{sym}", is_scan=True)
        except Exception as e:
            algo_log(f" [SUB] Subscribe failed for {exchange}:{sym}: {e}", is_scan=True)
    return jsonify({'symbol': sym, 'ltp': ltp})

@app.route('/api/debug/ltp', methods=['GET'])
def debug_ltp():
    """Debug endpoint: show all symbols with their LTP and last update time."""
    q = request.args.get('q', '').upper()
    entries = []
    for sym, price in sorted(ltp_dict.items()):
        if q and q not in sym:
            continue
        last_update = ltp_last_update_time.get(sym, 0)
        age = round(time.time() - last_update, 1) if last_update > 0 else -1
        entries.append({
            'symbol': sym,
            'ltp': price,
            'last_update_age_sec': age,
        })
    return jsonify({'count': len(entries), 'entries': entries})

@app.route('/api/debug/subscribe', methods=['POST'])
def debug_subscribe():
    """Debug endpoint: manually subscribe a symbol and log the result."""
    data = request.json or {}
    sym = data.get('symbol', '').upper()
    exchange = data.get('exchange', 'NFO')
    if not sym:
        return jsonify({'error': 'Missing symbol'}), 400
    if not openalgo_client:
        return jsonify({'error': 'OpenAlgo client not connected'}), 503
    try:
        sub_payload = [{'exchange': exchange, 'symbol': sym}]
        openalgo_client.subscribe_quote(sub_payload)
        algo_log(f" [DEBUG-SUB] Manual subscribe sent: {exchange}:{sym}")
        # Check if we already have an LTP
        existing_ltp = ltp_dict.get(sym, 0.0)
        return jsonify({
            'status': 'success',
            'message': f'Subscribe command sent for {exchange}:{sym}',
            'current_ltp': existing_ltp,
            'note': 'Check Trade Log for first-tick confirmation within ~5 seconds'
        })
    except Exception as e:
        algo_log(f" [DEBUG-SUB] Failed: {exchange}:{sym} → {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/broker/squareoff', methods=['POST'])
def square_off_raw():
    data = request.json
    sym = data.get('symbol', '').upper()
    price = safe_float(data.get('price', 0))
    rqty = safe_int(data.get('qty', 0))
    if not sym: return jsonify({'error': 'Missing symbol'}), 400

    # Cross reference the actual positionbook
    pos_match = None
    for p in cached_positionbook:
        if p.get('symbol', '').upper() == sym:
            pos_match = p
            break
            
    if not pos_match:
        return jsonify({'error': 'No corresponding open broker position found.'}), 404
        
    actual_qty = int(float(pos_match.get('quantity', 0)))
    if actual_qty == 0:
        return jsonify({'error': 'Position quantity is extremely closed/zeroed.'}), 400
        
    action = "SELL" if actual_qty > 0 else "BUY"
    target_qty = abs(actual_qty) if rqty <= 0 else rqty
    if target_qty > abs(actual_qty):
        return jsonify({'error': f'Cannot cover {target_qty}x. Max is {abs(actual_qty)}x.'}), 400
        
    ex = pos_match.get('exchange', 'NSE')
    prod = pos_match.get('product', 'CNC')
    price_type = "LIMIT" if price > 0 else "MARKET"
    
    oid = place_order_safe(sym, action, ex, price_type, prod, target_qty, price=price)
    if oid:
        algo_log(f" MANUAL RAW EXIT | {sym} {action} {target_qty}x @ {price if price>0 else 'MKT'} (OID:{oid})")
        return jsonify({'status': 'success', 'message': f'Fired {action} {target_qty}x via {price_type}.'})
    else:
        return jsonify({'error': 'Failed connecting to OpenAlgo Broker.'}), 500

@app.route('/api/trades', methods=['POST'])
def add_trade():
    global trade_id_counter
    data = request.json
    symbol = data.get('symbol', 'UNKNOWN').upper()
    algo_type = data.get('algo_type', 'BASIC_LONG')

    with state_lock:
        trade_id_counter += 1
        new_trade = {
            'id': trade_id_counter,
            'symbol': symbol,
            'algo_type': algo_type,
            'status': 'PENDING',
            'parameters': data.get('parameters', {}),
            'created_at': datetime.now().isoformat()
        }
        all_trades.append(new_trade)
        save_state()

    if openalgo_client and symbol != 'UNKNOWN':
        try:
            ex = data.get('parameters', {}).get('exchange', 'NSE')
            openalgo_client.subscribe_quote([{'exchange': ex, 'symbol': symbol}])
            algo_log(f" [API] Demanded OpenAlgo route {symbol} data to ZMQ.", is_scan=True)
        except Exception:
            pass

    algo_log(f" #{trade_id_counter} {symbol} ({algo_type}) registered → PENDING")
    return jsonify({'status': 'success', 'trade_id': trade_id_counter}), 201

@app.route('/api/trades/<int:trade_id>/action', methods=['POST'])
def action_trade(trade_id):
    action = request.json.get('action')
    trade = next((t for t in all_trades if t['id'] == trade_id), None)
    if not trade:
        return jsonify({'error': 'Trade not found'}), 404

    with state_lock:
        if action == 'SEND_TO_ALGO':
            if trade['status'] in ['RUNNING', 'COMPLETED', 'CANCELLED']:
                return jsonify({'error': 'Trade already active/archived.'}), 400
            trade['status'] = 'RUNNING'
            algo_log(f" #{trade_id} {trade['symbol']} ACTIVATED → RUNNING")
        elif action == 'CANCEL':
            # Cancel any pending orders tracked in params
            params = trade['parameters']
            for key in ['pending_entry_order_id', 'sl_order_id', 'sl_chase_order_id', 't1_order_id', 't2_order_id']:
                oid = params.get(key)
                if oid:
                    cancel_order_safe(oid)
            
            if 'exit_chase' in params and params['exit_chase'].get('oid'):
                cancel_order_safe(params['exit_chase']['oid'])
                params.pop('exit_chase')

            trade['status'] = 'CANCELLED'
            if params.get('entry_time') and int(params.get('remaining_qty', 0)) > 0:
                params['open_position'] = True
                algo_log(f" #{trade_id} {trade['symbol']} CANCELLED with OPEN POSITION")
            else:
                algo_log(f" #{trade_id} {trade['symbol']} CANCELLED")
        elif action == 'CLEAR_WARNING':
            trade['parameters'].pop('open_position', None)
        elif action == 'DELETE':
            all_trades.remove(trade)
            algo_log(f" #{trade_id} {trade['symbol']} DELETED from Journal")
            save_state()
            return jsonify({'status': 'success', 'new_status': 'DELETED'})
        else:
            return jsonify({'error': 'Invalid action'}), 400

        save_state()
    return jsonify({'status': 'success', 'new_status': trade['status']})

@app.route('/api/trades/<int:trade_id>/manual_exit', methods=['POST'])
def manual_exit_trade(trade_id):
    data = request.json
    qty = int(data.get('qty', 0))
    price = float(data.get('price', 0))
    trade = next((t for t in all_trades if t['id'] == trade_id), None)
    if not trade:
        return jsonify({'error': 'Trade not found'}), 404
    if qty <= 0:
        return jsonify({'error': 'Invalid qty'}), 400
    if not openalgo_client:
        return jsonify({'error': 'Algo offline'}), 503

    params = trade['parameters']
    ex = params.get('exchange', 'NSE')
    product = params.get('product', 'CNC')
    action = 'BUY' if trade['algo_type'] in ['SHORT_MIS', 'VWAP_SHORT'] else 'SELL'
    price_type = "LIMIT" if price > 0 else "MARKET"

    # Save original SL price in case exit fails
    old_sl_price = params.get('stop_loss', 0)

    # Cancel all existing pending exit orders
    for key in ['sl_order_id', 'sl_chase_order_id', 't1_order_id', 't2_order_id']:
        oid = params.get(key)
        if oid:
            cancel_order_safe(oid)
            params.pop(key, None)
            
    if 'exit_chase' in params and params['exit_chase'].get('oid'):
        cancel_order_safe(params['exit_chase']['oid'])
        params.pop('exit_chase')

    exit_oid = place_order_safe(trade['symbol'], action, ex, price_type, product, qty, price=price)
    
    if exit_oid:
        params['manual_exit_chase'] = {
            'oid': exit_oid,
            'qty': qty,
            'price': price,
            'time': time.time(),
            'reinstate_sl': old_sl_price
        }
        algo_log(f" #{trade['id']} {trade['symbol']} Manual Exit {qty}x sent. Suspended tracking for wait fill (OID:{exit_oid}).")
        save_state()
        return jsonify({'status': 'success', 'message': 'Manual exit sent. Waiting for broker fill.'})
    else:
        return jsonify({'error': 'Broker rejected manual exit immediately.'}), 500

# ── POSITION / ORDERBOOK API (polled by dashboard every 15s) ──
@app.route('/api/books', methods=['GET'])
def get_books():
    """Return cached orderbook and positionbook for dashboard display."""
    tracked_symbols = {str(t.get('symbol', '')).upper() for t in all_trades}
    
    orders_list = []
    for oid, o in cached_orderbook.items():
        if o.get('symbol', '').upper() not in tracked_symbols: continue
        orders_list.append({
            'order_id': oid,
            'symbol': o.get('symbol', ''),
            'action': o.get('transaction_type', o.get('action', '')),
            'qty': o.get('quantity', 0),
            'filled_qty': o.get('filledquantity', 0),
            'price': o.get('price', 0),
            'avg_price': o.get('average_price', 0),
            'status': o.get('order_status', ''),
            'order_type': o.get('order_type', ''),
            'product': o.get('product', ''),
        })
    positions_list = []
    for p in cached_positionbook:
        if p.get('symbol', '').upper() not in tracked_symbols: continue
        positions_list.append({
            'symbol': p.get('symbol', ''),
            'qty': p.get('quantity', 0),
            'avg_price': p.get('average_price', 0),
            'pnl': p.get('pnl', 0),
            'product': p.get('product', ''),
            'exchange': p.get('exchange', ''),
        })
    return jsonify({'orders': orders_list, 'positions': positions_list})

# ── SETTINGS API ──────────────────────────────────────────────
@app.route('/api/settings', methods=['GET'])
def get_settings_api():
    return jsonify({'settings': app_settings, 'defaults': DEFAULT_SETTINGS})

@app.route('/api/settings', methods=['POST'])
def update_settings_api():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    changes = []
    for key, val in data.items():
        if key in DEFAULT_SETTINGS:
            old = app_settings.get(key)
            app_settings[key] = val
            if old != val:
                changes.append(f"{key}: {old} → {val}")
    save_settings()
    if changes:
        algo_log(f" Settings updated: {'; '.join(changes)}")
    return jsonify({'status': 'success', 'settings': app_settings})

@app.route('/api/settings/reset', methods=['POST'])
def reset_settings_api():
    global app_settings
    app_settings = dict(DEFAULT_SETTINGS)
    save_settings()
    algo_log(" Settings reset to defaults")
    return jsonify({'status': 'success', 'settings': app_settings})

@app.route('/api/settings/form_defaults', methods=['GET'])
def get_form_defaults_api():
    """Return settings relevant to bot design forms (used to populate defaults)."""
    return jsonify({
        'default_limit_buffer_pct': get_setting('default_limit_buffer_pct'),
        'sl_market_chase_delay_seconds': get_setting('sl_market_chase_delay_seconds'),
        'order_fill_timeout_seconds': get_setting('order_fill_timeout_seconds'),
        'default_nfo_lot_size': get_setting('default_nfo_lot_size'),
    })

# ====================================================================
# ALERTS SYSTEM — Price Crossover & VWAP Crossover Alerts
# ====================================================================
import requests as http_requests

ALERTS_FILE_PATH = os.path.join(BASE_DIR, 'journal_alerts.json')
alerts_list = []
alerts_lock = threading.RLock()
alert_id_counter = 0
triggered_alerts_log = []  # Recently triggered alerts for UI display

# Index/Exchange mapping for quote API
INDEX_EXCHANGE_MAP = {
    'NIFTY': 'NSE_INDEX', 'NIFTY 50': 'NSE_INDEX', 'NIFTY50': 'NSE_INDEX',
    'BANKNIFTY': 'NSE_INDEX', 'NIFTY BANK': 'NSE_INDEX',
    'FINNIFTY': 'NSE_INDEX', 'NIFTY FIN SERVICE': 'NSE_INDEX',
    'SENSEX': 'BSE_INDEX', 'BANKEX': 'BSE_INDEX',
    'MIDCPNIFTY': 'NSE_INDEX',
}

def _get_index_ltp(symbol):
    """Fetch LTP for index symbols via OpenAlgo quotes API."""
    sym_upper = symbol.upper().strip()
    exchange = INDEX_EXCHANGE_MAP.get(sym_upper, None)
    if not exchange:
        return None
    try:
        r = http_requests.post('http://127.0.0.1:5000/api/v1/quotes',
            json={"apikey": get_setting('openalgo_api_key'), "symbol": sym_upper, "exchange": exchange},
            timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get('status') == 'success':
                return float(data['data']['ltp'])
    except Exception as e:
        algo_log(f"[ALERTS] Index quote error {sym_upper}: {e}", is_scan=True)
    return None

def _get_alert_ltp(symbol):
    """Get LTP for alert symbol — tries ZMQ cache first, then index API."""
    sym = symbol.upper().strip()
    ltp = ltp_dict.get(sym, 0.0)
    if ltp > 0:
        return ltp
    # Try index API
    idx_ltp = _get_index_ltp(sym)
    if idx_ltp and idx_ltp > 0:
        return idx_ltp
    return 0.0

def _get_vwap_for_alert(symbol, interval="3m", alert_exchange=None):
    """Get VWAP and last close for alert VWAP crossover detection.
    Uses alert_exchange if provided (critical for NFO options)."""
    if not openalgo_client:
        return None, None
    now = datetime.now()
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    sym = symbol.upper().strip()
    
    # Determine the correct exchange for the history API call
    if alert_exchange and alert_exchange not in ('', 'NSE'):
        exchange = alert_exchange  # Use the alert's configured exchange (NFO, BFO, MCX)
    elif sym in INDEX_EXCHANGE_MAP:
        exchange = INDEX_EXCHANGE_MAP[sym]
    else:
        exchange = "NSE"
    
    try:
        hist_res = openalgo_client.history(
            symbol=sym, exchange=exchange, interval=interval,
            start_date=start.strftime("%Y-%m-%d"), end_date=now.strftime("%Y-%m-%d")
        )
    except Exception as e:
        algo_log(f"[ALERTS] VWAP history error {sym} ({exchange}): {e}", is_scan=True)
        return None, None
    if hist_res is None or not isinstance(hist_res, pd.DataFrame) or hist_res.empty:
        return None, None
    df = hist_res
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    last_close = float(df['close'].iloc[-1])
    total_vol = df['volume'].sum()
    if total_vol == 0:
        # Options/illiquid: use TP-mean as VWAP proxy (critical for NFO)
        vwap = float(df['tp'].mean())
        algo_log(f"[ALERTS] {sym}({exchange}): Zero volume — TP-mean VWAP proxy ₹{vwap:.2f}, Close ₹{last_close:.2f}", is_scan=True)
        return vwap, last_close
    vwap = (df['tp'] * df['volume']).sum() / total_vol
    return vwap, last_close

def save_alerts():
    """Persist alerts to JSON."""
    with alerts_lock:
        try:
            tmp = ALERTS_FILE_PATH + '.tmp'
            with open(tmp, 'w') as f:
                json.dump({'alerts': alerts_list, 'last_id': alert_id_counter}, f, indent=2)
            os.replace(tmp, ALERTS_FILE_PATH)
        except Exception as e:
            print(f"[ALERTS] Save error: {e}")

def load_alerts():
    """Load alerts from JSON on startup."""
    global alerts_list, alert_id_counter
    if os.path.exists(ALERTS_FILE_PATH):
        try:
            with open(ALERTS_FILE_PATH, 'r') as f:
                data = json.load(f)
            alerts_list = data.get('alerts', [])
            alert_id_counter = data.get('last_id', 0)
            print(f"[ALERTS] Loaded {len(alerts_list)} alerts")
        except Exception as e:
            print(f"[ALERTS] Load error: {e}")

load_alerts()

def _check_crossover(prev_val, curr_val, ref_val, direction):
    """Check if a crossover happened. Returns True if crossover detected.
    direction: 'above' = price crosses above ref, 'below' = price crosses below ref, 'any' = either."""
    if prev_val is None or curr_val is None or ref_val is None:
        return False
    if direction == 'above':
        return prev_val <= ref_val and curr_val > ref_val
    elif direction == 'below':
        return prev_val >= ref_val and curr_val < ref_val
    elif direction == 'any':
        return (prev_val <= ref_val and curr_val > ref_val) or (prev_val >= ref_val and curr_val < ref_val)
    return False

def _async_vwap_eval(alert_id, sym, interval, direction, alert_exchange=None):
    """Background thread function to evaluate VWAP exclusively and quickly without blocking loop."""
    global alerts_list, triggered_alerts_log
    
    # 1. Fetch data (this is the blocking network call) — pass exchange for correct NFO/options data
    vwap, last_close = _get_vwap_for_alert(sym, interval, alert_exchange=alert_exchange)
    if vwap is None or last_close is None:
        return
        
    # 2. Acquire lock briefly to process state update
    with alerts_lock:
        alert = next((a for a in alerts_list if a['id'] == alert_id), None)
        if not alert or not alert.get('enabled', True) or alert.get('status') == 'triggered':
            return
            
        prev_close = alert.get('_prev_close', None)
        alert['_prev_close'] = last_close
        
        if prev_close is None:
            save_alerts()
            return
            
        if _check_crossover(prev_close, last_close, vwap, direction):
            cross_dir = 'above' if last_close > vwap else 'below'
            alert['status'] = 'triggered'
            alert['triggered_at'] = datetime.now().isoformat()
            alert['triggered_price'] = last_close
            alert['triggered_vwap'] = round(vwap, 2)
            triggered_alerts_log.insert(0, {
                'id': alert['id'], 'symbol': sym,
                'message': f"{sym} {interval} close crossed {cross_dir} VWAP ₹{vwap:.2f} (Close: ₹{last_close:.2f})",
                'time': alert['triggered_at'], 'type': 'vwap_crossover'
            })
            if len(triggered_alerts_log) > 50:
                triggered_alerts_log = triggered_alerts_log[:50]
            algo_log(f" VWAP ALERT: {sym} {interval} close crossed {cross_dir} VWAP ₹{vwap:.2f}")
            save_alerts()

def run_alerts_monitor():
    """Background thread: checks all enabled alerts every 2 seconds for high-speed triggering."""
    global alerts_list, triggered_alerts_log
    while True:
        try:
            time.sleep(2)
            now = datetime.now()
            # Only run during market hours (9:15 to 15:30)
            if now.hour < 9 or (now.hour == 9 and now.minute < 15) or now.hour >= 16:
                continue
                
            minutes_since_open = (now.hour * 60 + now.minute) - (9 * 60 + 15)

            with alerts_lock:
                state_changed = False
                for alert in alerts_list:
                    if not alert.get('enabled', True):
                        continue
                    if alert.get('status') == 'triggered':
                        continue

                    sym = alert['symbol'].upper().strip()
                    alert_type = alert.get('alert_type', 'price_crossover')

                    if alert_type == 'price_crossover':
                        ltp = _get_alert_ltp(sym)
                        if ltp <= 0:
                            continue
                        target_price = float(alert.get('target_price', 0))
                        direction = alert.get('direction', 'any')
                        prev_price = alert.get('_prev_price', None)
                        alert['_prev_price'] = ltp

                        if prev_price is None:
                            continue

                        if _check_crossover(prev_price, ltp, target_price, direction):
                            alert['status'] = 'triggered'
                            alert['triggered_at'] = now.isoformat()
                            alert['triggered_price'] = ltp
                            triggered_alerts_log.insert(0, {
                                'id': alert['id'], 'symbol': sym,
                                'message': f"{sym} crossed {'above' if ltp > target_price else 'below'} ₹{target_price:.2f} (LTP: ₹{ltp:.2f})",
                                'time': now.isoformat(), 'type': 'price_crossover'
                            })
                            if len(triggered_alerts_log) > 50:
                                triggered_alerts_log = triggered_alerts_log[:50]
                            algo_log(f" ALERT TRIGGERED: {sym} crossed ₹{target_price} (LTP: ₹{ltp:.2f})")
                            state_changed = True

                    elif alert_type == 'vwap_crossover':
                        interval = alert.get('timeframe', '3m')
                        direction = alert.get('direction', 'any')
                        alert_exchange = alert.get('exchange', 'NSE')  # Pass the alert's exchange
                        
                        try:
                            tf_minutes = int(interval.replace('m', ''))
                        except:
                            tf_minutes = 3
                            
                        # If current minute squarely aligns with the timeframe (e.g. 9:15+3=9:18)
                        if minutes_since_open >= 0 and minutes_since_open % tf_minutes == 0:
                            last_check = alert.get('_last_vwap_check_min', -1)
                            if last_check != now.minute:
                                alert['_last_vwap_check_min'] = now.minute
                                # Offload blocking API call securely into an async daemon thread
                                threading.Thread(
                                    target=_async_vwap_eval, 
                                    args=(alert['id'], sym, interval, direction, alert_exchange), 
                                    daemon=True
                                ).start()

                if state_changed:
                    save_alerts()
        except Exception as e:
            print(f"[ALERTS] Monitor error: {e}")

# ── ALERTS API ENDPOINTS ──────────────────────────────────────────
@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    with alerts_lock:
        safe = []
        for a in alerts_list:
            c = {k: v for k, v in a.items() if not k.startswith('_')}
            safe.append(c)
        sorted_alerts = sorted(safe, key=lambda x: x.get('triggered_at', ''), reverse=True)
        return jsonify({'alerts': sorted_alerts, 'triggered': triggered_alerts_log[:20]})

@app.route('/api/alerts', methods=['POST'])
def create_alert():
    global alert_id_counter
    data = request.json
    if not data or not data.get('symbol'):
        return jsonify({'error': 'Symbol is required'}), 400
    with alerts_lock:
        alert_id_counter += 1
        new_alert = {
            'id': alert_id_counter,
            'symbol': data['symbol'].upper().strip(),
            'exchange': data.get('exchange', 'NSE'),
            'alert_type': data.get('alert_type', 'price_crossover'),
            'target_price': float(data.get('target_price') or 0),
            'direction': data.get('direction', 'any'),
            'timeframe': data.get('timeframe', '3m'),
            'note': data.get('note', ''),
            'enabled': True,
            'status': 'active',
            'created_at': datetime.now().isoformat(),
        }
        alerts_list.append(new_alert)
        save_alerts()
    # Subscribe symbol for LTP
    if openalgo_client:
        try:
            sym = new_alert['symbol']
            ex = new_alert['exchange']
            if sym in INDEX_EXCHANGE_MAP:
                ex = INDEX_EXCHANGE_MAP[sym]
            openalgo_client.subscribe_quote([{'exchange': ex, 'symbol': sym}])
        except:
            pass
    algo_log(f" Alert #{alert_id_counter} created: {new_alert['symbol']} ({new_alert['alert_type']})")
    return jsonify({'status': 'success', 'alert_id': alert_id_counter}), 201

@app.route('/api/alerts/<int:alert_id>', methods=['PUT'])
def update_alert(alert_id):
    data = request.json
    with alerts_lock:
        alert = next((a for a in alerts_list if a['id'] == alert_id), None)
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        for key in ['symbol', 'alert_type', 'target_price', 'direction', 'timeframe', 'note', 'enabled']:
            if key in data:
                if key == 'target_price':
                    alert[key] = float(data[key])
                elif key == 'symbol':
                    alert[key] = str(data[key]).upper().strip()
                else:
                    alert[key] = data[key]
        # Reset state for re-evaluation
        if data.get('status') == 'active' or alert.get('status') == 'triggered':
            if 'status' in data and data['status'] == 'active':
                alert['status'] = 'active'
                alert.pop('triggered_at', None)
                alert.pop('triggered_price', None)
                alert.pop('_prev_price', None)
                alert.pop('_prev_close', None)
        save_alerts()
    return jsonify({'status': 'success'})

@app.route('/api/alerts/<int:alert_id>', methods=['DELETE'])
def delete_alert(alert_id):
    with alerts_lock:
        alert = next((a for a in alerts_list if a['id'] == alert_id), None)
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        alerts_list.remove(alert)
        save_alerts()
    algo_log(f" Alert #{alert_id} deleted")
    return jsonify({'status': 'success'})

@app.route('/api/alerts/bulk_delete', methods=['POST'])
def bulk_delete_alerts():
    """Delete multiple alerts at once by list of IDs."""
    data = request.json
    ids = data.get('ids', [])
    if not ids:
        return jsonify({'error': 'No alert IDs provided'}), 400
    removed_count = 0
    with alerts_lock:
        alerts_to_remove = [a for a in alerts_list if a['id'] in ids]
        for alert in alerts_to_remove:
            alerts_list.remove(alert)
            removed_count += 1
        if removed_count > 0:
            save_alerts()
    algo_log(f" Bulk deleted {removed_count} alert(s): {ids}")
    return jsonify({'status': 'success', 'deleted_count': removed_count})

@app.route('/api/alerts/<int:alert_id>/toggle', methods=['POST'])
def toggle_alert(alert_id):
    with alerts_lock:
        alert = next((a for a in alerts_list if a['id'] == alert_id), None)
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        alert['enabled'] = not alert.get('enabled', True)
        save_alerts()
    return jsonify({'status': 'success', 'enabled': alert['enabled']})

@app.route('/api/alerts/<int:alert_id>/reset', methods=['POST'])
def reset_alert(alert_id):
    with alerts_lock:
        alert = next((a for a in alerts_list if a['id'] == alert_id), None)
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        alert['status'] = 'active'
        alert['enabled'] = True
        alert.pop('triggered_at', None)
        alert.pop('triggered_price', None)
        alert.pop('_prev_price', None)
        alert.pop('_prev_close', None)
        save_alerts()
    algo_log(f" Alert #{alert_id} reset to active")
    return jsonify({'status': 'success'})

@app.route('/api/alerts/triggered', methods=['GET'])
def get_triggered_alerts():
    return jsonify({'triggered': triggered_alerts_log[:20]})

def subscribe_active_alerts():
    """Subscribe all active and pending alerts to WebSocket on startup."""
    if not openalgo_client:
        return
    subs = []
    with alerts_lock:
        for alert in alerts_list:
            if alert.get('enabled', True) and alert.get('status') != 'triggered':
                sym = alert['symbol'].upper().strip()
                ex = alert.get('exchange', 'NSE')
                if sym in INDEX_EXCHANGE_MAP:
                    ex = INDEX_EXCHANGE_MAP[sym]
                subs.append({'exchange': ex, 'symbol': sym})
    if subs:
        try:
            # We wait a moment for the openalgo client connection to stabilize
            time.sleep(2)
            openalgo_client.subscribe_quote(subs)
            algo_log(f" Subscribed {len(subs)} alerts to WebSocket.")
        except Exception as e:
            print(f"[ALERTS] Start subscription error: {e}")

# ====================================================================
# BACKEND AUDIO ALARM (Pygame)
# ====================================================================
try:
    import pygame
    pygame.mixer.init()
    has_pygame = True
except Exception as e:
    has_pygame = False
    print(f"[ALARM INIT] Pygame not available: {e}")

ALARM_SOUND_PATH = os.path.join(MASTER_DIR, 'templates', 'WS_Fail_Alert.mp3')
alarm_triggered = False

def _play_alarm_thread():
    try:
        print(f"[ALARM] Attempting to load audio file at: {ALARM_SOUND_PATH}")
        if os.path.exists(ALARM_SOUND_PATH):
            print("[ALARM] File exists! Checking mixer busy state...")
            if not pygame.mixer.music.get_busy():
                print("[ALARM] Loading and playing audio...")
                pygame.mixer.music.load(ALARM_SOUND_PATH)
                pygame.mixer.music.play()
            else:
                print("[ALARM] Mixer is already busy playing something else.")
        else:
            print("[ALARM ERROR] MP3 File DOES NOT EXIST at the specified path!")
    except Exception as e:
        print(f"[ALARM ERROR] Exception during playback: {e}")

def play_alarm_sound():
    global alarm_triggered
    if not has_pygame: 
        print("[ALARM] Cannot play: Pygame not initialized.")
        return
    if not alarm_triggered:
        print("[ALARM] Triggering Audio Playback thread! ")
        alarm_triggered = True
        threading.Thread(target=_play_alarm_thread, daemon=True).start()

def stop_alarm_sound():
    global alarm_triggered
    if not has_pygame: return
    if alarm_triggered or pygame.mixer.music.get_busy():
        try:
            print("[ALARM] Data ok -> Stopping audio.")
            pygame.mixer.music.stop()
            alarm_triggered = False
        except Exception as e:
            print(f"[ALARM] Stop error: {e}")

def run_backend_websocket_watchdog():
    """Background thread to natively play audio if WS data is stale."""
    monitor_symbols = ["RELIANCE", "JSWENERGY", "GODREJAGRO", "ICICIBANK", "TCS", "HDFCLIFE", "NIFTYBEES", "SAIL", "OIL", "ONGC", "DMART", "PETRONET", "MCX", "GAIL", "POWERGRID", "IGL"]
    while True:
        time.sleep(2)
        try:
            now = datetime.now()
            # Market hours check (9:15 to 15:31)
            time_val = now.hour + now.minute / 60.0
            if not (9.25 <= time_val <= 15.516):
                stop_alarm_sound()
                continue
                
            current_time = time.time()
            most_recent_monitor = 0
            for sym in monitor_symbols:
                t = ltp_last_update_time.get(sym, ltp_last_update_time.get(sym + "-EQ", 0))
                if t > most_recent_monitor:
                    most_recent_monitor = t
            
            # If no data at all yet, assume stale
            stale_seconds = (current_time - most_recent_monitor) if most_recent_monitor > 0 else (current_time - 0)
            delay_threshold = safe_int(get_setting('stale_data_alert_delay'), 180)
            
            # Extra safeguard: only alarm if we've actually connected and populated dict, or 5 mins past open
            if most_recent_monitor > 0 and stale_seconds > delay_threshold:
                play_alarm_sound()
            elif most_recent_monitor == 0 and time_val > (9.25 + 5/60.0):
                play_alarm_sound() # No data 5 mins after market open
            else:
                stop_alarm_sound()
                
        except Exception as e:
            print(f"[WATCHDOG ERROR] {e}")

# ====================================================================
# MAIN
# ====================================================================
if __name__ == '__main__':
    start_zmq_listener()
    subscribe_active_trades()
    start_algo_manager_async()
    # Start Alerts Monitor and WebSocket Subscription Background Tasks
    threading.Thread(target=run_alerts_monitor, daemon=True).start()
    threading.Thread(target=subscribe_active_alerts, daemon=True).start()
    threading.Thread(target=run_backend_websocket_watchdog, daemon=True).start()
    algo_log(f" OpenAlgo Journal Server started. Poll: {get_setting('poll_interval_seconds')}s | SL Chase: {get_setting('sl_market_chase_delay_seconds')}s | Alerts: ON")
    app.run(host='0.0.0.0', port=5006, debug=True, use_reloader=False, threaded=True)
