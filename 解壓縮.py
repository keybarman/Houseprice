import os
import zipfile
import re

# 壓縮檔所在資料夾
zip_folder = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部'
# 解壓縮後的根資料夾
output_root = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部-clean'

# 處理每個 zip 檔
for zip_file in os.listdir(zip_folder):
    if not zip_file.endswith(".zip"):
        continue

    # 解析季度資訊，例如 111S1.zip
    match = re.search(r"(\d{3}S[1-4])", zip_file)
    if not match:
        print(f"⚠️ 找不到季度資訊：{zip_file}")
        continue

    season = match.group(1)  # 例如 111S1
    zip_path = os.path.join(zip_folder, zip_file)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            if not member.endswith(".csv"):
                continue

            # 判斷分類
            if member.endswith("_a.csv"):
                category = "買賣"
            elif member.endswith("_b.csv"):
                category = "預售屋"
            else:
                category = "其他"

            # 建立對應子資料夾
            target_dir = os.path.join(output_root, category, season)
            os.makedirs(target_dir, exist_ok=True)

            # 解壓縮目標路徑
            target_path = os.path.join(target_dir, os.path.basename(member))

            # 解壓縮
            with zip_ref.open(member) as source, open(target_path, 'wb') as target:
                target.write(source.read())

            print(f"✅ 解壓：{zip_file} → {target_path}")