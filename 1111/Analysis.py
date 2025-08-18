import os
import pandas as pd


root_folder = r'C:\\Users\\Tulacu-2021\\Desktop\\Class\\Python try\\try\\1111各職業薪資資料'  


all_data = []


for main_cat in os.listdir(root_folder):
    main_path = os.path.join(root_folder, main_cat)
    if os.path.isdir(main_path):
        for file in os.listdir(main_path):
            if file.lower().endswith('.csv'):
                file_path = os.path.join(main_path, file)
                try:
                    df = pd.read_csv(file_path)
                    df['主職業類別'] = main_cat
                    df['細職業名稱'] = os.path.splitext(file)[0]
                    if '產業' not in df.columns:
                        df['產業'] = '無產業'
                    df['產業'] = df['產業'].fillna('無產業')
                    all_data.append(df)
                except Exception as e:
                    print(f'讀取失敗: {file_path}，原因: {e}')


if all_data:
    total_df = pd.concat(all_data, ignore_index=True)
    total_df.to_csv('all_職業薪資總表.csv', index=False, encoding='utf-8-sig')
    print('合併完成！已輸出 all_職業薪資總表.csv')
else:
    print('找不到任何CSV檔案或讀取失敗')
