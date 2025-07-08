# -*- coding: utf-8 -*-
"""
自動匯入實價登錄 CSV 到 MySQL：houses 資料庫 / buydata 資料表
"""

import os
import pandas as pd
from sqlalchemy import create_engine

# ✅ SQL 連線資訊
db_user = 'abc123456'
db_pass = 'abcABC123'
db_host = 'localhost'
db_port = '3306'
db_name = 'houses'
table_name = 'buydata'

engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4")

# ✅ 資料夾路徑
csv_folder = "C://Project//land//"

# ✅ 欄位對應
column_mapping = {
    '編號': 'Number',
    '鄉鎮市區': 'country',
    '交易標的': 'Target',
    '土地位置建物門牌': 'location',
    '土地移轉總面積平方公尺': 'landarea',
    '都市土地使用分區': 'partition',
    '移轉層次': 'floor',
    '總樓層數': 'total_floor',
    '建物型態': 'site',
    '主要用途': 'use',
    '主要建材': 'buildmaterials',
    '建築完成年月': 'buildendday',
    '建物移轉總面積平方公尺': 'buildarea',
    '建物現況格局-廳': 'hall',
    '建物現況格局-房': 'room',
    '建物現況格局-衛': 'bath',
    '總價元': 'totalprice',
    '主建物面積': 'area',
    '附屬建物面積': 'sitearea',
    '陽台面積': 'balconyarea',
    '單價元平方公尺': 'areaprice',
    '電梯': 'elevator',
    '移轉編號': 'multinum',
    '交易年月日': 'buydatacol',
    '車位類別': 'parking',
    '車位總價元': 'parkingprice',
    '車位移轉總面積平方公尺': 'parkingarea',
    '建物現況格局-隔間': 'part',
    '備註': 'others'
}

# ✅ CSV 檔案開頭字母 → 城市對應
city_map = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市',
    'E': '高雄市', 'F': '台北縣', 'G': '宜蘭縣', 'H': '桃園縣',
    'J': '新竹縣', 'K': '苗栗縣', 'L': '台中縣', 'M': '南投縣',
    'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣', 'Q': '嘉義縣',
    'R': '台南縣', 'S': '高雄縣', 'T': '屏東縣', 'U': '花蓮縣',
    'V': '台東縣', 'W': '澎湖縣', 'X': '陽明山', 'Y': '金門縣', 'Z': '連江縣'
}

# ✅ 開始處理資料夾內所有 csv 檔案
for file in os.listdir(csv_folder):
    if file.endswith(".csv"):
        full_path = os.path.join(csv_folder, file)
        print(f"📂 正在處理：{full_path}")

        try:
            try:
                df = pd.read_csv(full_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(full_path, encoding='big5')

            # 原始欄位列印（協助 debug）
            print(f"🔍 原始欄位：{df.columns.tolist()}")

            # 欄位對應
            df = df.rename(columns=column_mapping)
            df = df[[v for v in column_mapping.values() if v in df.columns]]

            # 確保主鍵欄位存在
            if 'Number' not in df.columns:
                print(f"❌ 缺少欄位 'Number'，跳過 {file}")
                continue

            # 自動補上城市欄位
            prefix = file[0].upper()
            df['City'] = city_map.get(prefix, '未知城市')

            # 轉換布林值欄位
            if 'elevator' in df.columns:
                df['elevator'] = df['elevator'].apply(lambda x: 1 if str(x).strip() == '有' else 0)
            if 'part' in df.columns:
                df['part'] = df['part'].apply(lambda x: 1 if str(x).strip() == '有' else 0)

            # 寫入資料庫
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"✅ 匯入成功：{file}（{len(df)} 筆）")

        except Exception as e:
            print(f"❌ 發生錯誤：{file} → {e}")
