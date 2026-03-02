import pandas as pd
import json
import os
from datetime import datetime, timedelta
from supabase import create_client, Client

# --- Supabase設定 ---
SUPABASE_URL = "https://wohwsfzixcsahfhfpebn.supabase.co"
SUPABASE_KEY = "sb_publishable_Jq5c9EMXK7tSyWUX11yMDA_Qos7xJfh"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- パス設定 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_PATH = os.path.join(BASE_DIR, 'historical_events.json')

def is_dst(dt):
    """米国夏時間(DST)判定"""
    year = dt.year
    dst_start = datetime(year, 3, 8) + timedelta(days=(6 - datetime(year, 3, 8).weekday()) % 7)
    dst_end = datetime(year, 11, 1) + timedelta(days=(6 - datetime(year, 11, 1).weekday()) % 7)
    return dst_start <= dt < dst_end

def fetch_past_events_and_sync():
    print("🚀 過去の重要指標を生成し、同期を開始します...")
    
    events_json = []      
    events_supabase = []  
    
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now()
    
    curr = start_date
    while curr <= end_date:
        event_time = "21:30" if is_dst(curr) else "22:30"
        date_str = curr.strftime("%Y-%m-%d")
        
        targets = []
        if curr.weekday() == 4 and 1 <= curr.day <= 7:
            targets.append("米雇用統計")
        if curr.day == 12:
            targets.append("米消費者物価指数(CPI)")
        if curr.day == 13:
            targets.append("米卸売物価指数(PPI)")

        for name in targets:
            events_json.append({
                "time": f"{date_str} {event_time}",
                "name": name,
                "importance": 3
            })
            
            events_supabase.append({
                "event_date": date_str,
                "event_time": event_time,
                "currency": "USD",
                "event_name": name,
                "importance": 3
            })
        curr += timedelta(days=1)

    # 1. ローカルJSON保存
    with open(SAVE_PATH, 'w', encoding='utf-8') as f:
        json.dump(events_json, f, ensure_ascii=False, indent=4)
    print(f"✅ ローカルJSON作成完了: {len(events_json)} 件")

    # 2. Supabaseへの個別保存（重複エラーを回避）
    print(f"📡 Supabaseへ {len(events_supabase)} 件のデータを同期中...")
    success = 0
    skip = 0
    
    for item in events_supabase:
        try:
            # upsert(on_conflict=...)を使って、重複時は何もしないように指定
            # もしテーブルに一意制約(event_date, event_time, event_name)がある場合に有効
            supabase.table("economic_calendar").upsert(item).execute()
            success += 1
        except Exception as e:
            # エラー内容に '23505' (重複) が含まれていればスキップ扱い
            if '23505' in str(e):
                skip += 1
            else:
                print(f"⚠️ 保存エラー: {item['event_name']} - {e}")
    
    print(f"✨ 同期完了！ (新規/更新: {success} 件, 重複スキップ: {skip} 件)")

if __name__ == "__main__":
    fetch_past_events_and_sync()