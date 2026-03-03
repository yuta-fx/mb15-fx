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
                
                time_val = cols[0]
                # 今回のログから、指標名は cols[2] に入っていることが判明
                event_name = cols[2]
                importance_info = cols[3] # 星の数

                # --- 指標名(event_name)から国を判定する ---
                currency = ""
                if any(x in event_name for x in ["米", "米国", "フェド", "雇用統計"]):
                    currency = "USD"
                elif any(x in event_name for x in ["日", "日本", "日銀"]):
                    currency = "JPY"
                # 他の国も必要ならここに追加（例：豪、欧、英など）
                
                # 日本と米国以外はスキップ（または必要に応じて変更）
                if not currency:
                    continue

                # 時間に数字が含まれているかチェック
                if not any(char.isdigit() for char in time_val):
                    continue

                # 重要度の判定（みんかぶの星や数値に対応）
                importance_level = 1
                if "★★★" in importance_info or "3" in importance_info:
                    importance_level = 3
                elif "★★" in importance_info or "2" in importance_info:
                    importance_level = 2

                event_data = {
                    "event_date": today_jst.strftime("%Y-%m-%d"),
                    "event_time": time_val[:5], 
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance_level
                }

                # Supabaseへ保存
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