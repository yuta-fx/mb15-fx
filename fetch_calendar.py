import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import requests
import io
import os

# --- 設定 ---
TARGET_URL = "https://fx.minkabu.jp/indicators"
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
        dfs = pd.read_html(io.StringIO(response.text))
        
        if not dfs:
            print("テーブルが見つかりませんでした。")
            return
            
        df = dfs[0] 
        print(f"取得行数: {len(df)}")
        
        success_count = 0

        for i, row in df.iterrows():
            try:
                # すべての列を文字列に変換し、余計な空白を徹底削除
                cols = [str(c).replace('\n', '').replace('\t', '').strip() for c in row]
                
                # デバッグ用：最初の3行だけ中身をログに出す（原因特定のため）
                if i < 3:
                    print(f"行{i}データ: {cols}")

                time_val = cols[0]
                country_info = cols[1]
                importance_info = cols[2]
                event_name = cols[3]

                # --- 判定ロジックを大幅に緩和 ---
                currency = ""
                if any(x in country_info for x in ["米", "US", "ドル"]):
                    currency = "USD"
                elif any(x in country_info for x in ["日", "JP", "円"]):
                    currency = "JPY"
                
                # 時間が「12:34」形式でなくても、数字が含まれていれば通す
                if not currency or not any(char.isdigit() for char in time_val):
                    continue

                # 重要度の判定
                importance_level = 1
                if any(x in importance_info for x in ["3", "★★★", "高", "重要"]):
                    importance_level = 3
                elif any(x in importance_info for x in ["2", "★★", "中"]):
                    importance_level = 2

                event_data = {
                    "event_date": today_jst.strftime("%Y-%m-%d"),
                    "event_time": time_val[:5], # "21:30"など先頭5文字
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance_level
                }

                supabase.table("economic_calendar").upsert(event_data).execute()
                print(f"✅ 同期: {time_val[:5]} | {currency} | {event_name}")
                success_count += 1
                
            except Exception as e:
                continue

        print(f"\n✨ 完了！ {success_count} 件の指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()