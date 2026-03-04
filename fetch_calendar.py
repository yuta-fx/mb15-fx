import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import requests
import io
import os
import re

# --- 設定 ---
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
        # ページ全体のテーブルを取得
        dfs = pd.read_html(io.StringIO(response.text))
        
        success_count = 0
        current_year = today_jst.year

        for df in dfs:
            # テーブルの内容を1行ずつ解析
            for _, row in df.iterrows():
                try:
                    # 行のデータをすべて結合して1つのテキストにする
                    vals = [str(v).replace('\n', ' ').strip() for v in row.values]
                    full_text = " ".join(vals)

                    # 1. 指標名 (event_name) を取得（一番長いテキストを候補にする）
                    # みんかぶの構造上、指標名に「米・」「日・」が含まれる
                    potential_names = [v for v in vals if len(v) > 5 and ":" not in v and "/" not in v]
                    if not potential_names:
                        continue
                    event_name = potential_names[0]

                    # 2. 通貨判定 (USD/JPY) - 指標名から判定するのが最も確実
                    currency = ""
                    if any(x in event_name for x in ["米", "米国", "USD", "ドル"]):
                        currency = "USD"
                    elif any(x in event_name for x in ["日", "日本", "JPY", "円"]):
                        currency = "JPY"
                    
                    if not currency:
                        continue

                    # 3. 日付の抽出 (M/D 形式)
                    date_match = re.search(r'(\d{1,2})/(\d{1,2})', full_text)
                    if not date_match:
                        continue
                    m, d = map(int, date_match.groups())
                    event_date = datetime(current_year, m, d).strftime("%Y-%m-%d")

                    # 4. 時間の抽出 (HH:MM 形式)
                    time_match = re.search(r'(\d{1,2}:\d{2})', full_text)
                    event_time = time_match.group(1) if time_match else "00:00"

                    # 5. 重要度の数値化
                    importance = 1
                    if "★★★" in full_text or "3" in full_text: importance = 3
                    elif "★★" in full_text or "2" in full_text: importance = 2

                    # --- Supabaseの economic_calendar テーブルへ保存 ---
                    event_data = {
                        "event_date": event_date,
                        "event_time": event_time,
                        "currency": currency,
                        "event_name": event_name,
                        "importance": importance
                    }

                    # 重複を避けつつ保存 (Upsert)
                    supabase.table("economic_calendar").upsert(event_data).execute()
                    print(f"✅ 同期成功: {event_date} {event_time} | {currency} | {event_name[:15]}")
                    success_count += 1
                    
                except Exception as e:
                    # print(f"行解析エラー: {e}") # デバッグ用
                    continue

        print(f"\n✨ 完了！ economic_calendar に {success_count} 件の指標を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()