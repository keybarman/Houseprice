import os
import zipfile

# 設定壓縮檔所在的資料夾
zip_folder = r'C:\\Users\\Tulacu-2021\\Desktop\\專題\\專題資料-內政部'  # <=== 請改成你的壓縮檔資料夾路徑
output_base = r'C:\\Users\\Tulacu-2021\\Desktop\\專題\\專題資料-內政部'  # <=== 解壓後的根資料夾

# 確保解壓目標根資料夾存在
os.makedirs(output_base, exist_ok=True)

# 遍歷所有 zip 檔案
for file in os.listdir(zip_folder):
    if file.endswith(".zip"):
        zip_path = os.path.join(zip_folder, file)
        folder_name = os.path.splitext(file)[0]  # 取得 zip 檔名，不含副檔名
        extract_path = os.path.join(output_base, folder_name)

        # 建立資料夾
        os.makedirs(extract_path, exist_ok=True)

        # 解壓縮
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print(f"✅ 解壓成功：{file} → {extract_path}")
        except zipfile.BadZipFile:
            print(f"❌ 壓縮檔錯誤：{file}")
