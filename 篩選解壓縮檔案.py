import os
import zipfile

# 壓縮檔所在的資料夾
zip_folder = r"C:\Project\zips"  # ← 你的 zip 檔案路徑
# 解壓後的根資料夾
output_root = r"C:\Project\unzip"

# 各類型的資料夾
sale_folder = os.path.join(output_root, "買賣")
presale_folder = os.path.join(output_root, "預售屋")
other_folder = os.path.join(output_root, "其他")

# 建立資料夾
os.makedirs(sale_folder, exist_ok=True)
os.makedirs(presale_folder, exist_ok=True)
os.makedirs(other_folder, exist_ok=True)

# 處理每個 zip 檔
for zip_file in os.listdir(zip_folder):
    if not zip_file.endswith(".zip"):
        continue

    zip_path = os.path.join(zip_folder, zip_file)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            # 確保是 csv 檔案
            if not member.endswith(".csv"):
                continue

            # 分類依據 CSV 檔名後綴
            if member.endswith("_a.csv"):
                target_dir = sale_folder
            elif member.endswith("_b.csv"):
                target_dir = presale_folder
            else:
                target_dir = other_folder

            # 解壓至對應資料夾
            zip_ref.extract(member, target_dir)
            print(f"✅ 解壓：{zip_file} → {target_dir}\\{member}")
