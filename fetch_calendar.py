import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import requests
import io
import os

# --- 設定 ---
# 「今週」の全指標ページへ変更（これで0件を回避）
TARGET_URL = "https://fx.minkabu.jp/indicators?range=this_week"
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") 

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst)
    print(f"解析開始 (日本時間: {today_jst.strftime('%Y-%m-%d %H:%M:%S')})")

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(TARGET_URL, headers=headers)
        # HTML内に複数のテーブルがある可能性があるため、すべてチェック
        dfs = pd.read_html(io.StringIO(response.text))
        
        if not dfs:
            print("テーブルが見つかりませんでした。")
            return
            
        success_count = 0
        current_year = today_jst.year

        for df in dfs:
            print(f"テーブル走査中... 行数: {len(df)}")
            
            for i, row in df.iterrows():
                try:
                    cols = [str(c).replace('\n', '').strip() for c in row]
                    
                    # みんかぶの「今週」ページは列構造が異なる場合があるため柔軟に対応
                    # 日付(0), 時間(1), 国(2), 指標名(3), 重要度(4) の順を想定
                    date_val = cols[0]
                    time_val = cols[1]
                    event_name = cols[3]
                    importance_info = cols[4]

                    # 通貨判定
                    currency = ""
                    if any(x in event_name for x in ["米", "米国", "フェド", "雇用統計", "米・"]):
                        currency = "USD"
                    elif any(x in event_name for x in ["日", "日本", "日銀", "日・"]):
                        currency = "JPY"
                    
                    if not currency:
                        continue

                    # 日付の解析 (3/4(水) などの形式から抽出)
                    date_match = pd.Series([date_val]).str.extract(r'(\d{1,2})/(\d{1,2})')
                    if date_match.isnull().values.any():
                        continue
                    
                    m, d = int(date_match[0][0]), int(date_match[1][0])
                    event_date = datetime(current_year, m, d).strftime("%Y-%m-%d")

                    # 重要度の数値化
                    importance_level = 1
                    if "★★★" in importance_info or "3" in importance_info:
                        importance_level = 3
                    elif "★★" in importance_info or "2" in importance_info:
                        importance_level = 2

                    event_data = {
                        "event_date": event_date,
                        "event_time": time_val[:5], 
                        "currency": currency,
                        "event_name": event_name,
                        "importance": importance_level
                    }

                    supabase.table("economic_calendar").upsert(event_data).execute()
                    print(f"✅ 同期: {event_date} {time_val[:5]} | {currency} | {event_name}")
                    success_count += 1
                    
                except Exception:
                    continue

        print(f"\n✨ 完了！ 今週の指標 {success_count} 件を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()