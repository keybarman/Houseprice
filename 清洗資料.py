import os
import pandas as pd
from datetime import datetime

# 縣市代碼對照表
city_map = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市', 'E': '高雄市',
    'F': '台北縣', 'G': '宜蘭縣', 'H': '桃園縣', 'J': '新竹縣', 'K': '苗栗縣',
    'L': '台中縣', 'M': '南投縣', 'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣',
    'Q': '嘉義縣', 'R': '台南縣', 'S': '高雄縣', 'T': '屏東縣', 'U': '花蓮縣',
    'V': '台東縣', 'W': '澎湖縣', 'X': '陽明山', 'Y': '金門縣', 'Z': '連江縣'
}

# 資料夾設定
root_input_folder = r'C:\Project\land_data'
root_output_folder = r'C:\Project\land_data_cleaned'
default_date = datetime(2008, 1, 1)

def convert_minguo_date(val, with_day=True):
    try:
        val = str(int(val)).zfill(7 if with_day else 5)
        year = int(val[:3]) + 1911
        month = int(val[3:5])
        day = int(val[5:7]) if with_day else 1
        return datetime(year, month, day).date()
    except:
        return default_date.date()

# 開始遍歷資料夾
for category_folder in os.listdir(root_input_folder):
    category_path = os.path.join(root_input_folder, category_folder)
    if not os.path.isdir(category_path):
        continue

    for quarter_folder in os.listdir(category_path):
        quarter_path = os.path.join(category_path, quarter_folder)
        if not os.path.isdir(quarter_path):
            continue

        output_quarter_path = os.path.join(root_output_folder, category_folder, quarter_folder)
        os.makedirs(output_quarter_path, exist_ok=True)

        for file in os.listdir(quarter_path):
            if not file.endswith('.csv'):
                continue

            file_path = os.path.join(quarter_path, file)
            try:
                city_code = file.split('_')[0].upper()
                city = city_map.get(city_code, '未知')

                df = pd.read_csv(file_path, encoding='utf-8-sig', header=None, skip_blank_lines=True)
                df.columns = df.iloc[0]
                df = df.drop(index=[0, 1])
                df.insert(0, '縣市', city)

                # 處理日期欄位
                if '交易年月日' in df.columns:
                    df['交易年月日'] = df['交易年月日'].apply(lambda x: convert_minguo_date(x, with_day=True))
                if '建築完成年月' in df.columns:
                    df['建築完成年月'] = df['建築完成年月'].apply(lambda x: convert_minguo_date(x, with_day=False))

                # 儲存清洗後資料
                output_path = os.path.join(output_quarter_path, file)
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"✅ 成功：{category_folder}/{quarter_folder}/{file}")

            except Exception as e:
                print(f"❌ 錯誤：{category_folder}/{quarter_folder}/{file} → {e}")
