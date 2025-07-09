import os
import zipfile

# ✅ 壓縮檔所在的資料夾（請確認資料夾存在）
zip_folder = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部'

# ✅ 解壓後的根資料夾
output_root = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部-clean'

# ✅ 各類型的資料夾
sale_folder = os.path.join(output_root, "買賣")
presale_folder = os.path.join(output_root, "預售屋")
other_folder = os.path.join(output_root, "其他")

# ✅ 建立資料夾
os.makedirs(sale_folder, exist_ok=True)
os.makedirs(presale_folder, exist_ok=True)
os.makedirs(other_folder, exist_ok=True)

# ✅ 檢查壓縮檔資料夾是否存在
if not os.path.exists(zip_folder):
    print(f"❌ 錯誤：找不到壓縮檔路徑 {zip_folder}")
    exit(1)

# ✅ 處理每個 zip 檔
for zip_file in os.listdir(zip_folder):
    if not zip_file.endswith(".zip"):
        continue

    zip_path = os.path.join(zip_folder, zip_file)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                # 確保是 CSV 檔案
                if not member.endswith(".csv"):
                    continue

                # 分類依據 CSV 檔名後綴
                if member.endswith("_a.csv"):
                    target_dir = sale_folder
                elif member.endswith("_b.csv"):
                    target_dir = presale_folder
                else:
                    target_dir = other_folder

                # 解壓到指定資料夾（包含子資料夾處理）
                zip_ref.extract(member, target_dir)
                print(f"✅ 解壓：{zip_file} → {os.path.join(target_dir, member)}")
    except zipfile.BadZipFile:
        print(f"❌ 無法開啟 ZIP：{zip_file}，可能已損壞")
