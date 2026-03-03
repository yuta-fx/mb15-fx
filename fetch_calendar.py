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

if not SUPABASE_URL or not SUPABASE_KEY:
    print("エラー: GitHubのSecretsが設定されていません。")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst)
    print(f"解析開始 (日本時間: {today_jst.strftime('%Y-%m-%d %H:%M:%S')})")

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(TARGET_URL, headers=headers)
        
        # 警告回避のための StringIO
        dfs = pd.read_html(io.StringIO(response.text))
        if not dfs:
            print("テーブルが見つかりませんでした。")
            return
            
        df = dfs[0] 
        print(f"取得した行数: {len(df)}")
        
        success_count = 0

        for i, row in df.iterrows():
            try:
                # みんかぶの標準的な列構造を抽出
                # row[0]: 時間, row[1]: 国, row[2]: 重要度, row[3]: 指標名
                time_val = str(row[0]).strip()
                country_raw = str(row[1]).strip()
                importance_raw = str(row[2]).strip()
                event_name = str(row[3]).strip()

                # 通貨の判定（国名から通貨コードへ変換）
                currency = ""
                if "米" in country_raw or "米国" in country_raw or "USD" in country_raw:
                    currency = "USD"
                elif "日" in country_raw or "日本" in country_raw or "JPY" in country_raw:
                    currency = "JPY"
                
                # USD/JPY以外、または時間が不適切な行はスキップ
                if not currency or ":" not in time_val:
                    continue

                # 重要度の数値化 (みんかぶは星の数や文字で入る)
                importance_level = 1
                if "3" in importance_raw or "★★★" in importance_raw or "高" in importance_raw:
                    importance_level = 3
                elif "2" in importance_raw or "★★" in importance_raw or "中" in importance_raw:
                    importance_level = 2

                event_data = {
                    "event_date": today_jst.strftime("%Y-%m-%d"),
                    "event_time": time_val,
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance_level
                }

                # Supabaseへ保存
                supabase.table("economic_calendar").upsert(event_data).execute()
                print(f"✅ 同期成功: {time_val} | {currency} | {event_name}")
                success_count += 1
                
            except Exception as e:
                # 個別の行のエラーは無視して次に進む
                continue

        print(f"\n✨ 完了！ 本日の指標 {success_count} 件を同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()