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
    print("Googleスプレッドシートを解析中（1週間分を取得対象）...")
    try:
        response = requests.get(CSV_URL)
        response.encoding = 'utf-8'
        # headerを指定せず、全データを読み込んでから解析します
        df = pd.read_csv(io.StringIO(response.text))
        
        jst = timezone(timedelta(hours=9))
        today_jst = datetime.now(jst)
        current_year = today_jst.year
        
        # 取得範囲：今日から7日後まで
        start_date = today_jst.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=7)
        
        success_count = 0

        for i, row in df.iterrows():
            # 行内の全データを文字列リスト化（欠損値 nan は除外）
            vals = [str(v).strip() for v in row.tolist() if str(v).strip().lower() != 'nan']
            
            # --- 日付の自動判定 (M/D 形式を探す) ---
            event_date_dt = None
            for v in vals:
                # 3/4 や 03/04 などの形式を検索
                match = re.search(r'(\d{1,2})[/-](\d{1,2})', v)
                if match:
                    month, day = match.groups()
                    try:
                        dt = datetime(current_year, int(month), int(day), tzinfo=jst)
                        # 年越し補正
                        if today_jst.month == 12 and int(month) == 1:
                            dt = dt.replace(year=current_year + 1)
                        
                        # 範囲内（今日〜7日後）なら確定
                        if start_date <= dt <= end_date:
                            event_date_dt = dt
                            break
                    except ValueError:
                        continue

            if event_date_dt is None:
                continue

            event_date_str = event_date_dt.strftime("%Y-%m-%d")

            # --- 列の自動判定 ---
            # 時間 (HH:MM 形式)
            time_val = next((v for v in vals if re.search(r'\d{1,2}:\d{2}', v)), "00:00")
            # 重要度 (★)
            importance_raw = next((v for v in vals if "★" in v), "")
            # 指標名 (日付、時間、URL、★、3文字大文字通貨 以外の比較的長い文字)
            event_name = next((v for v in vals if len(v) > 5 and ":" not in v and "/" not in v and "★" not in v and "http" not in v), "経済指標")

            if event_name:
                try:
                    # 通貨 (USD, JPYなど大文字3文字)
                    currency = next((v for v in vals if len(v) == 3 and v.isupper() and v not in ["DAY", "SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]), "USD")
                    
                    # USDとJPYに限定（必要に応じて他通貨も追加可）
                    if currency not in ["USD", "JPY"]:
                        continue

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
                    if "23505" not in str(e):
                        print(f"保存エラー: {e}")
                    else:
                        success_count += 1

        print(f"\n✨ 完了！ 今後1週間以内の {success_count} 件の指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()