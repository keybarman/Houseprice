import os
import pandas as pd

# 設定你的主資料夾路徑
root_folder = r'C:\\Users\\Tulacu-2021\\Desktop\\Class\\Python try\\try\\1111各職業薪資資料'  # 例如 r'D:\1111_薪資資料'

# 初始化資料清單
all_data = []

# 遞迴走訪每個主職業資料夾
for main_cat in os.listdir(root_folder):
    main_path = os.path.join(root_folder, main_cat)
    if os.path.isdir(main_path):
        # 讀取主職業資料夾下的每個csv檔案
        for file in os.listdir(main_path):
            if file.lower().endswith('.csv'):
                file_path = os.path.join(main_path, file)
                try:
                    df = pd.read_csv(file_path)
                    # 自動加上主職業與細職業兩欄
                    df['主職業類別'] = main_cat
                    df['細職業名稱'] = os.path.splitext(file)[0]
                    # 若產業欄位不存在自動補一欄
                    if '產業' not in df.columns:
                        df['產業'] = '無產業'
                    # 補空值產業
                    df['產業'] = df['產業'].fillna('無產業')
                    all_data.append(df)
                except Exception as e:
                    print(f'讀取失敗: {file_path}，原因: {e}')

# 合併所有DataFrame
if all_data:
    total_df = pd.concat(all_data, ignore_index=True)
    # 輸出總表
    total_df.to_csv('all_職業薪資總表.csv', index=False, encoding='utf-8-sig')
    print('✔️ 合併完成！已輸出 all_職業薪資總表.csv')
else:
    print('❌ 找不到任何CSV檔案或讀取失敗')
