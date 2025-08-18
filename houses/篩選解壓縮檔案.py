import os
import zipfile


zip_folder = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部'


output_root = r'C:\Users\Tulacu-2021\Desktop\專題\專題資料-內政部-clean'


sale_folder = os.path.join(output_root, "買賣")
presale_folder = os.path.join(output_root, "預售屋")
other_folder = os.path.join(output_root, "其他")


os.makedirs(sale_folder, exist_ok=True)
os.makedirs(presale_folder, exist_ok=True)
os.makedirs(other_folder, exist_ok=True)


if not os.path.exists(zip_folder):
    print(f"錯誤：找不到壓縮檔路徑 {zip_folder}")
    exit(1)


for zip_file in os.listdir(zip_folder):
    if not zip_file.endswith(".zip"):
        continue

    zip_path = os.path.join(zip_folder, zip_file)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                if not member.endswith(".csv"):
                    continue


                if member.endswith("_a.csv"):
                    target_dir = sale_folder
                elif member.endswith("_b.csv"):
                    target_dir = presale_folder
                else:
                    target_dir = other_folder

                zip_ref.extract(member, target_dir)
                print(f"解壓：{zip_file} → {os.path.join(target_dir, member)}")
    except zipfile.BadZipFile:
        print(f"無法開啟 ZIP：{zip_file}，可能已損壞")
