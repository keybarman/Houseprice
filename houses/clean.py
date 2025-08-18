import os
import pandas as pd
from datetime import datetime


def convert_roc_to_ad(date_str):
    if pd.isna(date_str):
        return ''
    date_str = str(date_str).strip()
    try:
        if "年" in date_str:
            parts = date_str.replace("年", "-").replace("月", "-").replace("日", "").split("-")
            year = int(parts[0]) + 1911
            month = int(parts[1])
            day = int(parts[2]) if len(parts) > 2 else 1
        elif len(date_str) == 7:
            year = int(date_str[:3]) + 1911
            month = int(date_str[3:5])
            day = int(date_str[5:7])
        elif len(date_str) == 6:
            year = int(date_str[:2]) + 1911
            month = int(date_str[2:4])
            day = int(date_str[4:6])
        else:
            return date_str
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except:
        return ''


city_map = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市', 'E': '高雄市',
    'F': '台北縣', 'G': '宜蘭縣', 'H': '桃園縣', 'I': '嘉義市', 'J': '新竹縣',
    'K': '苗栗縣', 'L': '台中縣', 'M': '南投縣', 'N': '彰化縣', 'O': '新竹市', 
    'P': '雲林縣', 'Q': '嘉義縣', 'R': '台南縣', 'S': '高雄縣', 'T': '屏東縣', 
    'U': '花蓮縣', 'V': '台東縣', 'W': '澎湖縣', 'X': '陽明山', 'Y': '金門縣', 
    'Z': '連江縣'
}


source_root = r'C:\Project\land_data'
target_root = r'C:\Project\land_data_cleaned'
success_log = []
error_log = []


for city in os.listdir(source_root):
    city_path = os.path.join(source_root, city)
    if not os.path.isdir(city_path): continue

    for quarter in os.listdir(city_path):
        quarter_path = os.path.join(city_path, quarter)
        if not os.path.isdir(quarter_path): continue

        output_folder = os.path.join(target_root, city, quarter)
        os.makedirs(output_folder, exist_ok=True)

        for filename in os.listdir(quarter_path):
            if not filename.endswith(".csv"):
                continue

            input_file = os.path.join(quarter_path, filename)
            output_file = os.path.join(output_folder, filename)

            try:
                df = pd.read_csv(input_file, header=None, skip_blank_lines=True, on_bad_lines='skip', encoding='utf-8', engine='python')
                df.columns = df.iloc[0]
                df = df.drop([0, 1])

                
                df.replace({'"': '*', ',': '*', '\n': ' '}, regex=True, inplace=True)

                
                for col in df.columns:
                    if '日期' in col or '年月日' in col or '年月' in col:
                        df[col] = df[col].apply(convert_roc_to_ad)

                
                city_code = filename[0].upper()
                city_name = city_map.get(city_code, '未知縣市')
                df.insert(0, '縣市', city_name)

                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                success_log.append(f"成功處理：{input_file}")
                print(f"成功處理：{input_file}")
            except Exception as e:
                error_log.append(f"失敗：{input_file} → {e}")
                print(f"失敗：{input_file} → {e}")


os.makedirs(target_root, exist_ok=True)
with open(os.path.join(target_root, "成功清單.txt"), "w", encoding="utf-8") as f:
    f.write("\n".join(success_log))

with open(os.path.join(target_root, "錯誤清單.txt"), "w", encoding="utf-8") as f:
    f.write("\n".join(error_log))

print("\n所有檔案清洗作業完成！")
