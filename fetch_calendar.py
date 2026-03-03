import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import io
import requests
import re
import os

# --- 設定 ---
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTd8pTpRBuVkToA9KIpFWBe1QdhuKsrs_6tk6TcjCBK8ZJoA6nrisFV0FVZcCDG4ic6ge3b-2latHo9/pub?output=csv"
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") 

if not SUPABASE_URL or not SUPABASE_KEY:
    print("エラー: GitHubのSecretsが設定されていません。")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print(f"Googleスプレッドシートを解析中... (現在時刻: {datetime.now()})")
    try:
        response = requests.get(CSV_URL)
        response.encoding = 'utf-8'
        # シートを読み込み。空行は無視
        df = pd.read_csv(io.StringIO(response.text), header=None).dropna(how='all')
        
        jst = timezone(timedelta(hours=9))
        today_jst = datetime.now(jst)
        current_year = today_jst.year
        
        # 今日から10日後までを対象（少し余裕を持たせます）
        start_date = today_jst.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=10)
        
        success_count = 0

        for i, row in df.iterrows():
            # 全てのセルを文字列としてリスト化
            vals = [str(v).strip() for v in row.tolist() if str(v).strip().lower() != 'nan' and str(v).strip() != '']
            if not vals: continue

            # --- 日付の自動判定 ---
            event_date_dt = None
            for v in vals:
                # 「3/4」「03/04」「3-4」などの形式を探す
                match = re.search(r'(\d{1,2})[/-](\d{1,2})', v)
                if match:
                    m, d = map(int, match.groups())
                    try:
                        # 2026年として作成
                        dt = datetime(current_year, m, d, tzinfo=jst)
                        
                        # 年末年始の補正（12月に1月のデータを見る場合など）
                        if today_jst.month == 12 and m == 1:
                            dt = dt.replace(year=current_year + 1)
                        elif today_jst.month == 1 and m == 12:
                            dt = dt.replace(year=current_year - 1)

                        # 指定範囲内かチェック
                        if start_date <= dt <= end_date:
                            event_date_dt = dt
                            break
                    except ValueError:
                        continue

            if event_date_dt is None:
                continue

            event_date_str = event_date_dt.strftime("%Y-%m-%d")

            # --- データの抽出 ---
            # 時間 (HH:MM)
            time_val = next((v for v in vals if re.search(r'\d{1,2}:\d{2}', v)), "00:00")
            # 重要度
            importance_raw = next((v for v in vals if "★" in v), "")
            # 通貨 (USD, JPYなど) ※曜日(SUN,MON等)を除外
            currency = next((v for v in vals if len(v) == 3 and v.isupper() and v not in ["SUN","MON","TUE","WED","THU","FRI","SAT","DAY"]), "USD")
            # 指標名 (長い文字を優先)
            long_vals = [v for v in vals if len(v) > 3 and ":" not in v and "/" not in v and "★" not in v and v != currency]
            event_name = long_vals[0] if long_vals else "経済指標"

            if currency not in ["USD", "JPY"]:
                continue

            try:
                event_data = {
                    "event_date": event_date_str,
                    "event_time": time_val,
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance_raw.count("★") if importance_raw else 1
                }

                supabase.table("economic_calendar").upsert(event_data).execute()
                print(f"✅ 同期成功: {event_date_str} {time_val} | {currency} | {event_name}")
                success_count += 1
            except Exception as e:
                print(f"保存エラー: {e}")

        print(f"\n✨ 完了！ {success_count} 件の指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()