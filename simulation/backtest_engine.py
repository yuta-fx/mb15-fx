import yfinance as yf
import pandas as pd
import numpy as np
import json
import csv
import os
from datetime import datetime, timedelta
import warnings
import time
from collections import defaultdict

# SSL関連の警告を非表示にする
warnings.filterwarnings("ignore", category=UserWarning)

# --- パス設定：このスクリプトがある simulation フォルダの「1つ上」を基準にする ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 出力用ディレクトリ（結果は simulation フォルダ内に出す）
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- 設定 ---
SYMBOL = "USDJPY=X"
TIMEFRAMES = {
    "1m":  {"period": "7d",   "interval": "1m"},
    "5m":  {"period": "60d",  "interval": "5m"},
    "15m": {"period": "60d",  "interval": "15m"},
    "30m": {"period": "60d",  "interval": "30m"},
    "1h":  {"period": "730d", "interval": "1h"},
    "4h":  {"period": "730d", "interval": "1h"},
}

# 戦略パターンの定義
STRATEGIES = [
    (30, 70, 50, 50, "rsi_30_70_50", None),
    (30, 70, 50, 50, "rsi_30_70_50", "double"),
    (0, 0, 0, 0, "bb_trend", None),
    (0, 0, 0, 0, "bb_trend", "double"),
    (0, 0, 0, 0, "bb_reversal", None),
    (0, 0, 0, 0, "bb_reversal", "double"),
    (20, 80, 50, 50, "rsi_20_80_50", None),
    (20, 80, 50, 50, "rsi_20_80_50", "double"),
    (20, 80, 70, 30, "rsi_20_80_70", None),
    (20, 80, 60, 40, "rsi_20_80_60", None),
    (20, 80, 40, 60, "rsi_20_80_40", None)
]

# 過去指標データの読み込みロジック
EVENT_TIMES = []
past_data_full = []
json_path = os.path.join(BASE_DIR, 'historical_events.json')

if os.path.exists(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            past_data_full = json.load(f)
            EVENT_TIMES = [ev['time'] for ev in past_data_full]
            print(f"✅ {len(EVENT_TIMES)} 件の過去指標を読み込みました: {json_path}")
    except Exception as e:
        print(f"⚠️ {json_path} の読み込みに失敗しました: {e}")
else:
    print(f"❌ ファイルが見つかりません: {json_path}")

# データキャッシュ用
cached_dfs = {}

def is_event_time(current_time):
    curr = pd.to_datetime(current_time).replace(tzinfo=None)
    for event in EVENT_TIMES:
        ev = pd.to_datetime(event).replace(tzinfo=None)
        # エントリー禁止・強制決済ロジックも前後1時間に設定
        if ev - timedelta(hours=1) <= curr <= ev + timedelta(hours=1):
            return True
    return False

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def export_to_numbers_csv(all_flat_results):
    filename = os.path.join(OUTPUT_DIR, "numbers_summary.csv")
    header = ["戦略", "フィルター", "時間足", "勝率", "合計損益(pips)", "合計損益(日本円)", "トレード数", "PF"]
    LOT_MULTIPLIER = 100000
    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for res in all_flat_results:
            jpy_val = int(res['total_profit'] * LOT_MULTIPLIER)
            jpy_str = f"¥{jpy_val:,}" if jpy_val >= 0 else f"-¥{abs(jpy_val):,}"
            writer.writerow([res['strat'], res['filter'] or "None", f"{res['tf']}足", f"{res['win_rate']}%", round(res['total_profit'], 3), jpy_str, f"{res['trade_count']} 回", res['pf']])
    print(f"\n✅ Numbers用CSV '{filename}' を作成しました。")

def get_or_download_data(tf_label):
    if tf_label in cached_dfs:
        return cached_dfs[tf_label]
    
    conf = TIMEFRAMES[tf_label]
    df = pd.DataFrame()
    for i in range(3):
        try:
            df = yf.download(SYMBOL, period=conf['period'], interval=conf['interval'], progress=False, timeout=20)
            if not df.empty: break
        except Exception:
            time.sleep(2)
    
    if df.empty: return None
    
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    if df.index.tz is not None: df.index = df.index.tz_localize(None)

    if tf_label == "4h":
        df = df.resample('4h').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()

    close, high, low = df['Close'], df['High'], df['Low']
    df['ma_s'] = close.rolling(5).mean()
    df['ma_l'] = close.rolling(20).mean()
    df['bb_m'] = close.rolling(20).mean()
    df['std'] = close.rolling(20).std()
    df['bb_u'] = df['bb_m'] + (df['std'] * 2)
    df['bb_l'] = df['bb_m'] - (df['std'] * 2)
    df['rsi'] = calculate_rsi(close)

    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
    df['s_a'] = ((tenkan + kijun) / 2).shift(26)
    df['s_b'] = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    exp1 = close.ewm(span=12, adjust=False).mean()
    exp2 = close.ewm(span=26, adjust=False).mean()
    df['macd_line'] = exp1 - exp2
    df['macd_sig'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    df['macd_h'] = df['macd_line'] - df['macd_sig']
    
    cached_dfs[tf_label] = df
    return df

def run_simulation(tf_label, entry_low, entry_high, exit_buy, exit_sell, strat_id, filter_type=None):
    df = get_or_download_data(tf_label)
    if df is None: return None

    df_5m = get_or_download_data("5m") if filter_type == "double" else None
    df_15m = get_or_download_data("15m") if filter_type == "double" else None

    sim_df = df.iloc[78:].copy() 
    balance, trade_history, position = 0, [], None
    spread = 0.003 
    
    monthly_profits = defaultdict(float)

    for i in range(1, len(sim_df)):
        curr = sim_df.iloc[i]
        curr_time = curr.name
        t_str = curr_time.strftime('%Y-%m-%d %H:%M')
        month_key = curr_time.strftime('%Y-%m')
        price = float(curr['Close'])
        rsi_val = curr['rsi']
        bb_u, bb_l, bb_m = curr['bb_u'], curr['bb_l'], curr['bb_m']

        f_entry_ok = True
        if filter_type == "double":
            try:
                if tf_label == "1m":
                    rsi_5 = df_5m['rsi'].asof(curr_time)
                    rsi_15 = df_15m['rsi'].asof(curr_time)
                    if rsi_5 <= 30 or rsi_5 >= 70 or rsi_15 <= 30 or rsi_15 >= 70:
                        f_entry_ok = False
                elif tf_label == "5m":
                    rsi_15 = df_15m['rsi'].asof(curr_time)
                    if rsi_15 <= 30 or rsi_15 >= 70:
                        f_entry_ok = False
            except:
                f_entry_ok = False

        if is_event_time(curr_time):
            if position:
                p_diff = (price - position['entry']) - spread if position['type'] == 'BUY' else (position['entry'] - price) - spread
                balance += p_diff
                monthly_profits[month_key] += p_diff
                trade_history.append({"entry_time": position['entry_time'], "time": t_str, "type": "EXIT_EVENT", "entry": position['entry'], "exit": price, "profit": round(p_diff, 3), "balance": round(balance, 3)})
                position = None
            continue

        if position:
            is_exit = False
            if "rsi" in strat_id:
                is_exit = (position['type'] == 'BUY' and rsi_val >= exit_buy) or (position['type'] == 'SELL' and rsi_val <= exit_sell)
            elif strat_id == "bb_trend":
                is_exit = (position['type'] == 'BUY' and price <= bb_m) or (position['type'] == 'SELL' and price >= bb_m)
            elif strat_id == "bb_reversal":
                is_exit = (position['type'] == 'BUY' and price >= bb_m) or (position['type'] == 'SELL' and price <= bb_m)

            if is_exit:
                p_diff = (price - position['entry']) - spread if position['type'] == 'BUY' else (position['entry'] - price) - spread
                balance += p_diff
                monthly_profits[month_key] += p_diff
                trade_history.append({"entry_time": position['entry_time'], "time": t_str, "type": position['type'] + "_OUT", "entry": round(position['entry'], 3), "exit": round(price, 3), "profit": round(p_diff, 3), "balance": round(balance, 3)})
                position = None
        else:
            if f_entry_ok:
                if "rsi" in strat_id:
                    if rsi_val <= entry_low:
                        position = {"type": "BUY", "entry": price, "entry_time": t_str}
                    elif rsi_val >= entry_high:
                        position = {"type": "SELL", "entry": price, "entry_time": t_str}
                elif strat_id == "bb_trend":
                    if price >= bb_u:
                        position = {"type": "BUY", "entry": price, "entry_time": t_str}
                    elif price <= bb_l:
                        position = {"type": "SELL", "entry": price, "entry_time": t_str}
                elif strat_id == "bb_reversal":
                    if price <= bb_l:
                        position = {"type": "BUY", "entry": price, "entry_time": t_str}
                    elif price >= bb_u:
                        position = {"type": "SELL", "entry": price, "entry_time": t_str}

    out_df = df.ffill().fillna(0)
    indicator_data = {key: [] for key in ["ma_s", "ma_l", "bb_u", "bb_l", "s_a", "s_b", "rsi", "macd_h", "macd_line", "macd_sig"]}
    chart_candles = []
    
    # --- 経済指標データの整理（表示範囲を前後1時間に修正） ---
    relevant_events = []
    if not out_df.empty:
        chart_idx_no_tz = out_df.index.tz_localize(None) if out_df.index.tz is not None else out_df.index
        c_start = chart_idx_no_tz[0]
        c_end = chart_idx_no_tz[-1]
        
        for ev in past_data_full:
            try:
                ev_dt = pd.to_datetime(ev['time']).replace(tzinfo=None)
                # 塗りつぶしの開始と終了を前後1時間に設定
                display_start = ev_dt - timedelta(hours=1)
                display_end = ev_dt + timedelta(hours=1)

                if c_start <= ev_dt <= c_end:
                    closest_idx = chart_idx_no_tz.get_indexer([ev_dt], method='nearest')[0]
                    event_point_dt = chart_idx_no_tz[closest_idx]
                    
                    relevant_events.append({
                        "time": event_point_dt.strftime('%Y-%m-%d %H:%M'),
                        "name": ev['name'],
                        "start": display_start.strftime('%Y-%m-%d %H:%M'),
                        "end": display_end.strftime('%Y-%m-%d %H:%M')
                    })
            except:
                continue

    for idx, row in out_df.iterrows():
        t = idx.strftime('%Y-%m-%d %H:%M')
        chart_candles.append({"time": t, "open": float(row['Open']), "high": float(row['High']), "low": float(row['Low']), "close": float(row['Close'])})
        for key in indicator_data.keys(): indicator_data[key].append({"time": t, "value": float(row[key])})

    profits = [t['profit'] for t in trade_history]
    win_trades = [p for p in profits if p > 0]
    loss_trades = [p for p in profits if p <= 0]
    
    monthly_report = [{"month": k, "profit": round(v, 3)} for k, v in sorted(monthly_profits.items())]

    results = {
        "tf": tf_label, "strat": strat_id, "filter": filter_type,
        "win_rate": round(len(win_trades)/len(profits)*100, 1) if profits else 0,
        "total_profit": round(balance, 3),
        "trade_count": len(profits),
        "pf": round(sum(win_trades)/abs(sum(loss_trades)), 2) if loss_trades and sum(loss_trades) != 0 else "0.0",
        "monthly_report": monthly_report,
        "history": trade_history[::-1], "candles": chart_candles, "indicators": indicator_data, "events": relevant_events
    }
    
    strat_suffix = "" if strat_id == "rsi_30_70_50" else f"_{strat_id}"
    f_suffix = f"_{filter_type}" if filter_type else ""
    save_filename = os.path.join(OUTPUT_DIR, f'sim_results_{tf_label}{strat_suffix}{f_suffix}.json')
    
    with open(save_filename, 'w') as f: json.dump(results, f)
    return results

if __name__ == "__main__":
    flat_results_list = []
    print("📥 データダウンロード開始...")
    timeframes_list = ["1m", "5m", "15m", "30m", "1h", "4h"]
    for label in timeframes_list:
        get_or_download_data(label)
    print("✅ データの準備が完了しました。")

    for elow, ehigh, exbuy, exsell, s_id, f_type in STRATEGIES:
        f_name = f_type if f_type else "None"
        print(f"\n🚀 戦略実行中: {s_id} (Filter: {f_name})")
        for label in timeframes_list:
            res = run_simulation(label, elow, ehigh, exbuy, exsell, s_id, f_type)
            if res and res['trade_count'] > 0:
                flat_results_list.append(res)
                print(f"  ✅ [{label}] 完了 - 損益: {res['total_profit']} pips")

    print("\n" + "="*60)
    print("🏆 シミュレーション総合ランキング 🏆")
    print("="*60)

    top_profit = sorted(flat_results_list, key=lambda x: x['total_profit'], reverse=True)[:3]
    print("\n💰 【通算純利益 TOP3】")
    for i, r in enumerate(top_profit, 1):
        f_info = f"({r['filter']})" if r['filter'] else ""
        print(f"{i}位: {r['tf']}足 | {r['strat']}{f_info} | {r['total_profit']} pips (PF: {r['pf']})")

    export_to_numbers_csv(flat_results_list)
    print("\n✨ すべての計算とランキング抽出が完了しました。")