import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# 資料庫連線參數
db_user = 'abc123456'
db_pass = 'abcABC123'
db_host = 'localhost'
db_port = '3306'
db_name = 'houses'
table_name = 'buydata'

# 資料夾設定
root_folder = "C:\\Project\\land_data_cleaned\\買賣"
log_file_path = os.path.join(root_folder, 'imported_files.txt')

# 欄位清單
target_columns = [
    '縣市', '鄉鎮市區', '交易標的', '土地位置建物門牌', '土地移轉總面積平方公尺',
    '都市土地使用分區', '非都市土地使用分區', '非都市土地使用編定', '交易年月日',
    '交易筆棟數', '移轉層次', '總樓層數', '建物型態', '主要用途', '主要建材',
    '建築完成年月', '建物移轉總面積平方公尺', '建物現況格局-房', '建物現況格局-廳',
    '建物現況格局-衛', '建物現況格局-隔間', '有無管理組織', '總價元', '單價元平方公尺',
    '車位類別', '車位移轉總面積平方公尺', '車位總價元', '備註', '編號', '主建物面積',
    '附屬建物面積', '陽台面積', '電梯', '移轉編號'
]

# 建立連線
engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4")

# 載入已匯入的檔案清單（使用絕對路徑）
imported_files = set()
if os.path.exists(log_file_path):
    with open(log_file_path, 'r', encoding='utf-8') as f:
        imported_files = set(line.strip() for line in f if line.strip())

# 遍歷檔案
for season in os.listdir(root_folder):
    season_path = os.path.join(root_folder, season)
    if not os.path.isdir(season_path):
        continue

    for file in os.listdir(season_path):
        if not file.endswith('.csv'):
            continue

        file_path = os.path.abspath(os.path.join(season_path, file))  # 絕對路徑

        if file_path in imported_files:
            print(f"⏩ 跳過已處理：{file_path}")
            continue

        print(f"📄 處理：{file_path}")

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['建物現況格局-隔間'] = df['建物現況格局-隔間'].replace({'有': 1, '無': 0})
            df['電梯'] = df['電梯'].replace({'有': 1, '無': 0})
            df = df[target_columns]

            df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')

            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"{file_path}\n")

            print(f"✅ 匯入成功，共 {len(df)} 筆")

        except Exception as e:
            print(f"❌ 匯入失敗：{file_path} → {e}")
            raise
