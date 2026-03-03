import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import requests
import os

# --- 設定 ---
# スプレッドシートを介さず、みんかぶから直接テーブルを取得（0件問題を回避）
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
    print(f"解析開始 (直接スクレイピング: {today_jst.strftime('%Y-%m-%d %H:%M:%S')})")

    try:
        # 1. サイトからテーブルデータを直接読み込む
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(TARGET_URL, headers=headers)
        
        # HTML内のテーブルを抽出
        dfs = pd.read_html(response.text)
        if not dfs:
            print("テーブルが見つかりませんでした。")
            return
            
        df = dfs[0] 
        success_count = 0
        current_year = today_jst.year

        # 2. データの整形と保存
        for i, row in df.iterrows():
            try:
                # みんかぶのテーブル構造（時間, 通貨, 重要度, 指標名...）を解析
                time_val = str(row[0]).strip()
                currency = str(row[1]).strip()
                importance_raw = str(row[2]).strip()
                event_name = str(row[3]).strip()

                # 通貨フィルタ（USDとJPYのみ）
                if currency not in ["USD", "JPY"]:
                    continue
                
                # 時間が数値（HH:MM）でない行は飛ばす
                if ":" not in time_val:
                    continue

                # 重要度の数値化
                importance_level = 1
                if "3" in importance_raw or "★★★" in importance_raw: importance_level = 3
                elif "2" in importance_raw or "★★" in importance_raw: importance_level = 2

                # 保存用データ作成（日付は実行当日の日付を使用）
                event_data = {
                    "event_date": today_jst.strftime("%Y-%m-%d"),
                    "event_time": time_val,
                    "currency": currency,
                    "event_name": event_name,
                    "importance": importance_level
                }

                # 保存実行
                supabase.table("economic_calendar").upsert(event_data).execute()
                print(f"✅ 同期成功: {time_val} | {currency} | {event_name}")
                success_count += 1
                
            except Exception as e:
                continue

        print(f"\n✨ 完了！ 本日の指標 {success_count} 件を直接同期しました。")

    except Exception as e:
        print(f"致命的エラー: {e}")

if __name__ == "__main__":
    fetch_and_save()