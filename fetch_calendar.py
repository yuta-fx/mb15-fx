import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import io
import requests
import re
import os  # 追加：環境変数を読み込むために必要

# --- 設定 (GitHubのSecretsから安全に読み込む設定) ---
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTd8pTpRBuVkToA9KIpFWBe1QdhuKsrs_6tk6TcjCBK8ZJoA6nrisFV0FVZcCDG4ic6ge3b-2latHo9/pub?output=csv"

# GitHubのSettings > Secrets > Actions で登録した名前を os.environ.get("...") に指定します
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") 

# 万が一Secretsが未設定の場合のチェック
if not SUPABASE_URL or not SUPABASE_KEY:
    print("エラー: GitHubのSecretsに SUPABASE_URL または SUPABASE_KEY が設定されていません。")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print("Googleスプレッドシートを解析中（1週間分を取得対象）...")
    try:
        response = requests.get(CSV_URL)
        response.encoding = 'utf-8'
        df = pd.read_csv(io.StringIO(response.text), header=None)
        
        jst = timezone(timedelta(hours=9))
        today_jst = datetime.now(jst)
        current_year = today_jst.year
        
        # 取得範囲の定義 (今日から7日後まで)
        start_date = today_jst.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=7)
        
        success_count = 0

        for i, row in df.iterrows():
            if i == 0: continue # タイトル行スキップ
            
            vals = [str(v).strip() for v in row.tolist()]
            
            # --- 日付の自動判定 ---
            event_date_dt = None
            for v in vals:
                match = re.search(r'(\d{1,2})[/-](\d{1,2})', v)
                if match:
                    month, day = match.groups()
                    dt = datetime(current_year, int(month), int(day), tzinfo=jst)
                    if today_jst.month == 12 and int(month) == 1:
                        dt = dt.replace(year=current_year + 1)
                    
                    if start_date <= dt <= end_date:
                        event_date_dt = dt
                    break

            if event_date_dt is None:
                continue

            event_date_str = event_date_dt.strftime("%Y-%m-%d")

            # --- 列の自動判定 ---
            time_val = next((v for v in vals if ":" in v and len(v) <= 5), "00:00")
            importance_raw = next((v for v in vals if "★" in v), "")
            long_vals = [v for v in vals if len(v) > 5 and ":" not in v and "http" not in v and "/" not in v]
            event_name = long_vals[0] if long_vals else ""

            if event_name and event_name != "nan":
                try:
                    currency = next((v for v in vals if len(v) == 3 and v.isupper()), "---")
                    
                    if currency not in ["USD", "JPY"]:
                        continue

                    event_data = {
                        "event_date": event_date_str,
                        "event_time": time_val,
                        "currency": currency,
                        "event_name": event_name,
                        "importance": importance_raw.count("★") if importance_raw else 1
                    }

                    # upsertを使うので、同じ指標名・同じ日付・同じ時刻なら上書きされます
                    supabase.table("economic_calendar").upsert(event_data).execute()
                    print(f"✅ 同期成功: {event_date_str} {time_val} | {currency} | {event_name}")
                    success_count += 1
                except Exception as e:
                    if "23505" not in str(e):
                        print(f"保存エラー: {e}")
                    else:
                        success_count += 1

        print(f"\n✨ 完了！ 今後1週間以内の {success_count} 件の重要指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()