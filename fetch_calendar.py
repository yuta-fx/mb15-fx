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
            # データの次元を平坦化してループ
            for _, row in df.iterrows():
                try:
                    # すべての列を結合
                    row_data = [str(v).replace('\n', ' ').strip() for v in row.values]
                    full_text = " ".join(row_data)

                    # 1. 通貨判定 (USD/JPY)
                    currency = ""
                    if any(x in full_text for x in ["米", "米国", "USD", "ドル"]):
                        currency = "USD"
                    elif any(x in full_text for x in ["日", "日本", "JPY", "円"]):
                        currency = "JPY"
                    
                    if not currency:
                        continue

                    # 2. 日付の抽出 (M/D 形式)
                    date_match = re.search(r'(\d{1,2})/(\d{1,2})', full_text)
                    if not date_match:
                        continue
                    m, d = map(int, date_match.groups())
                    event_date = f"{current_year}-{m:02d}-{d:02d}"

                    # 3. 時間の抽出 (HH:MM 形式)
                    time_match = re.search(r'(\d{1,2}:\d{2})', full_text)
                    event_time = time_match.group(1) if time_match else "00:00"

                    # 4. 指標名
                    # 「指標名」が含まれる可能性が高い列を探す
                    potential_names = [v for v in row_data if len(v) > 4 and ":" not in v and "/" not in v]
                    event_name = potential_names[0] if potential_names else "経済指標"

                    # 5. 重要度 (星の数をカウント、または数値を探す)
                    importance = 1
                    if "★★★" in full_text or "3" in full_text:
                        importance = 3
                    elif "★★" in full_text or "2" in full_text:
                        importance = 2

                    # --- テーブルのカラム名に厳密に合わせる ---
                    event_data = {
                        "event_date": event_date,    # date型
                        "event_time": event_time,    # text型
                        "currency": currency,        # text型
                        "event_name": event_name,    # text型
                        "importance": int(importance) # int4型
                    }

                    # SupabaseへUpsert (重複は event_date, event_time, event_name で判断される)
                    supabase.table("economic_calendar").upsert(event_data).execute()
                    print(f"✅ 同期成功: {event_date} {event_time} | {currency} | {event_name[:15]}")
                    success_count += 1
                    
                except Exception as e:
                    continue

        print(f"\n✨ 完了！ economic_calendar に {success_count} 件同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()