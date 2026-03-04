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
        dfs = pd.read_html(io.StringIO(response.text))
        
        success_count = 0
        current_year = today_jst.year

        for df in dfs:
            for _, row in df.iterrows():
                try:
                    # セルデータをリスト化し、不要な改行を除去
                    vals = [str(v).replace('\n', ' ').strip() for v in row.values]
                    full_text = " ".join(vals)

                    # 1. 日付(M/D)と時間(HH:MM)が両方ある行だけを対象にする
                    date_match = re.search(r'(\d{1,2})/(\d{1,2})', full_text)
                    time_match = re.search(r'(\d{1,2}:\d{2})', full_text)
                    if not date_match or not time_match:
                        continue
                    
                    # 2. 通貨判定 (USDとJPYに厳選)
                    currency = ""
                    # 「米」や「USD」が含まれれば米国指標
                    if any(x in full_text for x in ["米", "米国", "USD", "ドル"]):
                        currency = "USD"
                    # 「日」や「JPY」が含まれれば日本指標
                    elif any(x in full_text for x in ["日", "日本", "JPY", "円"]):
                        currency = "JPY"
                    
                    # ドル円に関係ない国ならスキップ
                    if not currency:
                        continue

                    # 3. 日付と時間を整形
                    m, d = map(int, date_match.groups())
                    event_date = f"{current_year}-{m:02d}-{d:02d}"
                    event_time = time_match.group(1)

                    # 4. 指標名の特定 (数値や単位が含まれない最も長い文字列を探す)
                    event_name = "経済指標"
                    for v in vals:
                        # 5文字以上で、かつ数値や記号(pips, %, :)を含まないものを名称とみなす
                        if len(v) > 4 and not re.search(r'[+-]?\d|:|/|★', v):
                            event_name = v
                            break

                    # 5. 重要度
                    importance = 1
                    if "★★★" in full_text: importance = 3
                    elif "★★" in full_text: importance = 2

                    # --- 保存実行 ---
                    event_data = {
                        "event_date": event_date,
                        "event_time": event_time,
                        "currency": currency,
                        "event_name": event_name,
                        "importance": int(importance)
                    }

                    supabase.table("economic_calendar").upsert(event_data).execute()
                    print(f"✅ 同期: {event_date} {event_time} | {currency} | {event_name}")
                    success_count += 1
                    
                except Exception:
                    continue

        print(f"\n✨ 完了！ ドル円関連の指標 {success_count} 件を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()