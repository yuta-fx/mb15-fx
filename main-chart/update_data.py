import yfinance as yf
import json
import time
import math
import os
import pandas as pd
from supabase import create_client, Client 
from datetime import datetime, timedelta, timezone 
import hashlib # 追加：データ変化の検知用

# --- 設定 ---
SYMBOL = "USDJPY=X"
SUPABASE_URL = "https://wohwsfzixcsahfhfpebn.supabase.co" 
SUPABASE_KEY = "sb_publishable_Jq5c9EMXK7tSyWUX11yMDA_Qos7xJfh" 
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) 

# --- 保存先パスの確定 ---
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

TIMEFRAMES = [
    {"label": "1m",  "interval": "1m",  "period": "1d",  "offset": 26, "rsi_in": 20, "rsi_out": 40},
    {"label": "5m",  "interval": "5m",  "period": "5d",  "offset": 26, "rsi_in": 20, "rsi_out": 70},
    {"label": "15m", "interval": "15m", "period": "5d",  "offset": 26, "rsi_in": 20, "rsi_out": 50},
    {"label": "30m", "interval": "30m", "period": "5d",  "offset": 26, "rsi_in": None, "rsi_out": None},
    {"label": "1h",  "interval": "1h",  "period": "1mo", "offset": 26, "rsi_in": None, "rsi_out": None},
    {"label": "4h",  "interval": "4h",  "period": "1mo", "offset": 26, "rsi_in": 20, "rsi_out": 50},
    {"label": "8h",  "interval": "1h",  "period": "2mo", "offset": 26, "rsi_in": None, "rsi_out": None},
    {"label": "1d",  "interval": "1d",  "period": "max", "offset": 26, "rsi_in": None, "rsi_out": None},
    {"label": "1w",  "interval": "1wk", "period": "max", "offset": 26, "rsi_in": None, "rsi_out": None}
]

# 点滅防止用：前回のデータハッシュを保持
last_hashes = {}

def get_filtered_economic_events():
    """今日と明日の重要指標(USD/JPY かつ 重要度2以上)を取得し、前後1時間の範囲を計算"""
    try:
        jst = timezone(timedelta(hours=9))
        now_jst = datetime.now(jst)
        today_str = now_jst.strftime("%Y-%m-%d")
        
        res = supabase.table("economic_calendar") \
            .select("*") \
            .gte("event_date", today_str) \
            .order("event_date") \
            .execute()
        
        filtered = []
        for item in res.data:
            if item['currency'] in ['USD', 'JPY'] and item['importance'] >= 2:
                time_str = item['event_time']
                date_str = item['event_date']
                
                if ":" in time_str:
                    try:
                        h, m = map(int, time_str.split(":"))
                        dt_str = f"{date_str} {h:02d}:{m:02d}:00"
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=jst)
                        ts = int(dt.timestamp())
                        
                        filtered.append({
                            "time": ts,
                            "start": ts - 3600,
                            "end": ts + 3600,
                            "name": f"[{item['currency']}] {item['event_name']}",
                            "importance": item['importance']
                        })
                    except:
                        continue
        return filtered
    except Exception as e:
        print(f"Calendar Fetch Error: {e}")
        return []

def send_mac_notification(title, message):
    os.system(f"osascript -e 'display notification \"{message}\" with title \"{title}\" sound name \"Glass\"'")

def clean_nan(data_list):
    return [d for d in data_list if not math.isnan(d.get('value', float('nan')))]

last_statuses = {tf['label']: "" for tf in TIMEFRAMES}
current_positions = {tf['label']: None for tf in TIMEFRAMES}
signals_history = {tf['label']: [] for tf in TIMEFRAMES}

print(f"マルチタイムフレームFX監視システム 起動中... (出力先: {OUTPUT_DIR})")

while True:
    all_trends = {}
    all_levels = {} 
    economic_events = get_filtered_economic_events()

    for tf in TIMEFRAMES:
        try:
            label, rsi_in, rsi_out = tf['label'], tf['rsi_in'], tf['rsi_out']
            df = yf.download(tickers=SYMBOL, period=tf['period'], interval=tf['interval'], progress=False)
            
            if df.empty: continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if label == "8h":
                df = df.resample('8H').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()

            close, high, low = df['Close'], df['High'], df['Low']
            df['MA_short'] = close.rolling(5).mean()
            df['MA_long'] = close.rolling(20).mean()
            df['bb_m'] = close.rolling(20).mean()
            df['std'] = close.rolling(20).std()
            df['bb_u2'] = df['bb_m'] + (df['std'] * 2)
            df['bb_l2'] = df['bb_m'] - (df['std'] * 2)
            df['sup'] = low.rolling(50).min()
            df['res'] = high.rolling(50).max()
            tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
            kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
            df['senkou_a'] = ((tenkan + kijun) / 2)
            df['senkou_b'] = (high.rolling(52).max() + low.rolling(52).min()) / 2
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            df['rsi'] = 100 - (100 / (1 + (gain / loss)))
            exp1, exp2 = close.ewm(span=12, adjust=False).mean(), close.ewm(span=26, adjust=False).mean()
            df['macd'], df['macd_sig'] = exp1 - exp2, (exp1 - exp2).ewm(span=9, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_sig']
            df['stoch_k'] = 100 * (close - low.rolling(9).min()) / (high.rolling(9).max() - low.rolling(9).min())
            df['stoch_d'] = df['stoch_k'].rolling(3).mean()
            tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
            df['atr'] = tr.rolling(14).mean()

            curr = df.iloc[-1]
            prev = df.iloc[-2]
            price = float(curr['Close'])
            all_levels[label] = {"sup": float(curr['sup']), "res": float(curr['res'])}

            candles, ma_s, ma_l, s_a, s_b, bb_u, bb_l, rsi_d = [], [], [], [], [], [], [], []
            macd_d, macd_s, macd_h, stoch_k, stoch_d = [], [], [], [], []
            all_times = set()
            interval_sec = (df.index[1] - df.index[0]).total_seconds() if len(df) > 1 else 60
            offset_sec = tf['offset'] * interval_sec

            for index, row in df.iterrows():
                t = int(index.timestamp()) + (9 * 60 * 60)
                all_times.add(t)
                candles.append({"time": t, "open": float(row['Open']), "high": float(row['High']), "low": float(row['Low']), "close": float(row['Close'])})
                ma_s.append({"time": t, "value": float(row['MA_short'])}); ma_l.append({"time": t, "value": float(row['MA_long'])})
                bb_u.append({"time": t, "value": float(row['bb_u2'])}); bb_l.append({"time": t, "value": float(row['bb_l2'])})
                rsi_d.append({"time": t, "value": float(row['rsi'])})
                macd_d.append({"time": t, "value": float(row['macd'])}); macd_s.append({"time": t, "value": float(row['macd_sig'])}); macd_h.append({"time": t, "value": float(row['macd_hist'])})
                stoch_k.append({"time": t, "value": float(row['stoch_k'])}); stoch_d.append({"time": t, "value": float(row['stoch_d'])})
                ft = int(t + offset_sec); s_a.append({"time": ft, "value": float(row['senkou_a'])}); s_b.append({"time": ft, "value": float(row['senkou_b'])})

            is_expanding = (curr['bb_u2'] - curr['bb_l2']) > (prev['bb_u2'] - prev['bb_l2'])
            trend_icon, trend_text = "→", "レンジ"
            if is_expanding:
                if price > curr['bb_u2']: trend_icon, trend_text = "↑", "強い上昇トレンド"
                elif price > curr['bb_m']: trend_icon, trend_text = "↗", "上昇トレンド"
                elif price < curr['bb_l2']: trend_icon, trend_text = "↓", "強い下降トレンド"
                elif price < curr['bb_m']: trend_icon, trend_text = "↘", "下降トレンド"
            all_trends[label] = {"icon": trend_icon, "text": trend_text, "price": price}

            status, active_type = "監視中", "NONE"
            t_now = int(df.index[-1].timestamp()) + (9 * 60 * 60)
            rsi_val = float(curr['rsi'])

            if rsi_in is not None:
                if current_positions[label] is None:
                    if rsi_val <= rsi_in:
                        status, current_positions[label] = f"✅ {label} BUY", "BUY"
                        signals_history[label].append({"time": t_now, "position": "belowBar", "color": "#26a69a", "shape": "arrowUp", "text": "BUY"})
                    elif rsi_val >= (100 - rsi_in):
                        status, current_positions[label] = f"🚨 {label} SELL", "SELL"
                        signals_history[label].append({"time": t_now, "position": "aboveBar", "color": "#ef5350", "shape": "arrowDown", "text": "SELL"})
                else:
                    active_type = current_positions[label]
                    if (active_type == "BUY" and rsi_val >= rsi_out) or (active_type == "SELL" and rsi_val <= (100 - rsi_out)):
                        status = f"💰 {label} {active_type}決済"
                        signals_history[label].append({"time": t_now, "position": "inBar", "color": "#ffffff", "shape": "circle", "text": "EXIT"})
                        current_positions[label], active_type = None, "NONE"

            if status != "監視中" and status != last_statuses[label]:
                send_mac_notification(f"サイン確定 [{label}]", status)
                last_statuses[label] = status

            output = {
                "candles": candles, "ma_short": clean_nan(ma_s), "ma_long": clean_nan(ma_l),
                "senkou_a": clean_nan(s_a), "senkou_b": clean_nan(s_b),
                "bb_upper2": clean_nan(bb_u), "bb_lower2": clean_nan(bb_l),
                "rsi": clean_nan(rsi_d), "macd": clean_nan(macd_d), "macd_signal": clean_nan(macd_s), "macd_hist": clean_nan(macd_h),
                "stoch_k": clean_nan(stoch_k), "stoch_d": clean_nan(stoch_d),
                "rsi_sync": [{"time": ts, "value": 50.0} for ts in sorted(list(all_times))],
                "macd_sync": [{"time": ts, "value": 0.0} for ts in sorted(list(all_times))],
                "status": status, "active_type": active_type, "current_price": price, "trend_icon": trend_icon,
                "atr": float(curr['atr']), "signals": signals_history[label],
                "economic_events": economic_events 
            }

            # 【点滅防止：ハッシュによる変更検知】
            json_str = json.dumps(output, sort_keys=True)
            new_hash = hashlib.md5(json_str.encode()).hexdigest()
            if last_hashes.get(label) != new_hash:
                with open(os.path.join(OUTPUT_DIR, f'data_{label}.json'), 'w') as f: 
                    f.write(json_str)
                last_hashes[label] = new_hash

        except Exception as e: print(f"Error {tf['label']}: {e}")
    
    with open(os.path.join(OUTPUT_DIR, 'all_trends.json'), 'w') as f: 
        json.dump(all_trends, f)
    with open(os.path.join(OUTPUT_DIR, 'all_levels.json'), 'w') as f: 
        json.dump(all_levels, f)
    
    time.sleep(3)