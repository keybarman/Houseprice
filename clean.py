import os
import pandas as pd
from datetime import datetime

# 原始資料夾與清洗後資料夾
source_root = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部-clean'
target_root = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部-cleannn'

# 錯誤日誌檔案
log_path = os.path.join(target_root, 'error_log.txt')
os.makedirs(target_root, exist_ok=True)  # 確保日誌檔資料夾存在

# 清空舊日誌
with open(log_path, 'w', encoding='utf-8') as log_file:
    log_file.write(f"🚨 清洗錯誤紀錄 - 建立於 {datetime.now()}\n\n")

print("📂 檢查原始資料夾內容...")
try:
    city_folders = os.listdir(source_root)
    print(f"✅ 找到 {len(city_folders)} 個縣市資料夾：{city_folders}")
except FileNotFoundError:
    print(f"❌ 找不到來源資料夾：{source_root}")
    exit()

for city_folder in city_folders:
    city_path = os.path.join(source_root, city_folder)
    if not os.path.isdir(city_path):
        print(f"⚠️ 跳過非資料夾項目：{city_path}")
        continue

    print(f"🏙️ 處理城市：{city_folder}")

    for quarter_folder in os.listdir(city_path):
        quarter_path = os.path.join(city_path, quarter_folder)
        if not os.path.isdir(quarter_path):
            print(f"⚠️ 跳過非季度資料夾：{quarter_path}")
            continue

        print(f"📅 處理季度：{quarter_folder}")

        # 建立對應的清洗後輸出資料夾
        output_folder = os.path.join(target_root, city_folder, quarter_folder)
        os.makedirs(output_folder, exist_ok=True)

        for filename in os.listdir(quarter_path):
            if not filename.endswith(".csv"):
                continue

            file_path = os.path.join(quarter_path, filename)
            print(f"🔍 處理中：{file_path}")

            try:
                # 讀取 CSV（無標題）
                df = pd.read_csv(file_path, header=None, skip_blank_lines=True)

                # 將第一列設為欄位名，刪除第二列
                df.columns = df.iloc[0]
                df = df.drop(index=[0, 1])

                # 儲存到新位置
                output_path = os.path.join(output_folder, filename)
                df.to_csv(output_path, index=False, encoding='utf-8-sig')

                print(f"✅ 已清洗並儲存：{output_path}")

            except Exception as e:
                error_msg = f"❌ 錯誤處理檔案：{file_path} → {e}"
                print(error_msg)
                with open(log_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(error_msg + '\n')

print(f"\n🎉 所有檔案處理完成！錯誤日誌位於：{log_path}")
