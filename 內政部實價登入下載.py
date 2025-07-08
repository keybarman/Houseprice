import requests
import os

# 要下載的年度與季別（107=2018）
years = list(range(107, 115))  # 可依需要擴充
seasons = ['S1', 'S2', 'S3', 'S4']  # 四季

# 建立資料夾
save_dir = 'C:\\Users\\Tulacu-2021\\Desktop\\專題\\專題資料-內政部'
os.makedirs(save_dir, exist_ok=True)

# 開始下載
for year in years:
    for season in seasons:
        season_code = f'{year}{season}'
        url = f'http://plvr.land.moi.gov.tw/DownloadSeason?season={season_code}&type=zip&fileName=lvr_landcsv.zip'
        file_name = f'{season_code}.zip'
        save_path = os.path.join(save_dir, file_name)

        try:
            print(f'下載中：{season_code} ...')
            r = requests.get(url, stream=True, timeout=30)
            if r.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        f.write(chunk)
                print(f'✅ 已下載 {file_name}')
            else:
                print(f'❌ {season_code} 下載失敗（HTTP {r.status_code}）')
        except Exception as e:
            print(f'⚠️ {season_code} 發生錯誤：{e}')
