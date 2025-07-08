import os
import pandas as pd
from sqlalchemy import create_engine
import chardet


# 資料庫連線參數
db_user = 'abc123456'
db_pass = 'abcABC123'
db_host = 'localhost'
db_port = '3306'
db_name = 'houses'
table_name = 'buydata'

# SQLAlchemy engine（MySQL）
engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4")

# CSV 資料夾路徑
csv_folder = "C:/Project/land/"

# 欄位對應
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
    '有無管理組織': 'elevator',
    '移轉編號': 'multinum',
    '交易年月': 'buydatacol',
    '車位類別': 'parking',
    '車位總價元': 'parkingprice',
    '車位移轉總面積平方公尺': 'parkingarea',
    '建物現況格局-隔間': 'part',
    '備註': 'others'
}

# 縣市代碼對照表
city_map = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市', 'E': '高雄市', 'F': '台北縣',
    'G': '宜蘭縣', 'H': '桃園縣', 'J': '新竹縣', 'K': '苗栗縣', 'L': '台中縣', 'M': '南投縣',
    'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣', 'Q': '嘉義縣', 'R': '台南縣', 'S': '高雄縣',
    'T': '屏東縣', 'U': '花蓮縣', 'V': '台東縣', 'W': '澎湖縣', 'X': '陽明山', 'Y': '金門縣', 'Z': '連江縣'
}




for file in os.listdir(csv_folder):
    if file.endswith(".csv"):
        full_path = os.path.join(csv_folder, file)
        print(f"📂 處理檔案：{full_path}")

        try:
            # ✅ 自動偵測檔案編碼
            with open(full_path, 'rb') as f:
                result = chardet.detect(f.read())
            encoding = result['encoding']
            print(f"📑 使用編碼：{encoding}")

            # ✅ 用正確編碼讀取 CSV
            df = pd.read_csv(full_path, encoding=encoding)
            df = df.rename(columns=column_mapping)
            df = df[[v for v in column_mapping.values() if v in df.columns]]

            # 加入縣市欄位
            prefix = file[0].upper()
            df['City'] = city_map.get(prefix, '未知城市')

            # 處理布林值欄位
            if 'elevator' in df.columns:
                df['elevator'] = df['elevator'].apply(lambda x: 1 if str(x).strip() == '有' else 0)
            if 'part' in df.columns:
                df['part'] = df['part'].apply(lambda x: 1 if str(x).strip() == '有' else 0)

            # 限制文字欄位長度避免塞爆資料庫
            limit_columns = {'floor': 100, 'site': 100, 'others': 500}
            for col, limit in limit_columns.items():
                if col in df.columns:
                    df[col] = df[col].astype(str).str[:limit]

            # ✅ 寫入資料庫
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"✅ 匯入完成：{file}（共 {len(df)} 筆）")

        except Exception as e:
            print(f"❌ 發生錯誤：{file} → {e}")

