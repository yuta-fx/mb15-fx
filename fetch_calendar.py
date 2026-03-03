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
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst)
    print(f"解析開始 (日本時間: {today_jst.strftime('%Y-%m-%d %H:%M:%S')})")

    try:
        response = requests.get(CSV_URL)
        response.encoding = 'utf-8'
        # シートを読み込み（列を特定せず全読み込み）
        df = pd.read_csv(io.StringIO(response.text), header=None).fillna("")
        
        current_year = today_jst.year
        start_date = today_jst.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=14)
        
        current_processing_date = None # 現在処理中の日付を保持
        success_count = 0

        for i, row in df.iterrows():
            vals = [str(v).strip() for v in row.tolist() if str(v).strip()]
            if not vals: continue
            
            row_text = " ".join(vals)

            # --- 1. 日付行かどうか判定 (例: "3/4 (水)") ---
            # A列(vals[0])に日付っぽいのがあるか確認
            date_match = re.search(r'(\d{1,2})[/-](\d{1,2})', vals[0])
            if date_match:
                m, d = map(int, date_match.groups())
                try:
                    dt = datetime(current_year, m, d, tzinfo=jst)
                    # 年越し補正
                    if today_jst.month == 12 and m == 1: dt = dt.replace(year=current_year + 1)
                    # この日付を「現在の日付」としてセット
                    current_processing_date = dt
                    continue # 日付行自体の処理はここまで
                except ValueError:
                    pass

            # --- 2. データ行の処理 ---
            # 日付が決まっていない、または範囲外ならスキップ
            if not current_processing_date or not (start_date <= current_processing_date <= end_date):
                continue

            # 通貨判定 (USD or JPY)
            currency = None
            if "USD" in row_text.upper(): currency = "USD"
            elif "JPY" in row_text.upper(): currency = "JPY"
            
            if not currency:
                continue

            # 時間 (HH:MM)
            time_match = re.search(r'(\d{1,2}:\d{2})', row_text)
            event_time = time_match.group(1) if time_match else "00:00"
            
            # 重要度 (星の数)
            importance = row_text.count("★")
            if importance == 0: importance = 1

            # 指標名 (通貨や時間、星を除いた部分を抽出)
            event_name = "経済指標"
            potential_names = [v for v in vals if len(v) > 3 and ":" not in v and "★" not in v and v.upper() not in ["USD", "JPY"]]
            if potential_names:
                event_name = potential_names[0]

            try:
                event_data = {
                    "event_date": current_processing_date.strftime("%Y-%m-%d"),
                    "event_time": event_time,
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance
                }

                supabase.table("economic_calendar").upsert(event_data).execute()
                print(f"✅ 同期: {current_processing_date.strftime('%m/%d')} {event_time} | {currency} | {event_name}")
                success_count += 1
            except Exception as e:
                print(f"保存エラー: {e}")

        print(f"\n✨ 完了！ {success_count} 件の指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()