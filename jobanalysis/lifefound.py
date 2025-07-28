import pandas as pd
import numpy as np

def expand_taiwan_province_data(input_file='歷年最低生活費.csv', output_file='擴展後_歷年最低生活費.csv'):
    """
    將台灣省的薪資數據套用到指定的縣市清單中
    支援ANSI編碼的CSV檔案（橫向資料結構）
    """
    
    # 台灣省的縣市名單
    taiwan_province_cities = [
        '基隆市', '宜蘭縣', '桃園縣', '新竹縣', '苗栗縣',
        '南投縣', '彰化縣', '新竹市', '雲林縣', '嘉義縣',
        '屏東縣', '花蓮縣', '臺東縣', '澎湖縣'
    ]

    try:
        # 讀取CSV檔案 (支援ANSI編碼)
        print("正在讀取CSV檔案...")
        try:
            # 先嘗試ANSI編碼 (cp950是繁體中文ANSI)
            df = pd.read_csv(input_file, encoding='cp950')
            print("使用ANSI (cp950)編碼讀取成功")
        except UnicodeDecodeError:
            try:
                # 如果失敗，嘗試UTF-8
                df = pd.read_csv(input_file, encoding='utf-8')
                print("使用UTF-8編碼讀取成功")
            except UnicodeDecodeError:
                # 最後嘗試GBK
                df = pd.read_csv(input_file, encoding='gbk')
                print("使用GBK編碼讀取成功")
        
        # 顯示原始資料資訊
        print(f"原始資料形狀: {df.shape}")
        print(f"欄位名稱: {list(df.columns)}")
        print("\n原始資料:")
        print(df)
        
        # 檢查是否有臺灣省欄位
        taiwan_province_column = None
        for col in df.columns:
            if '臺灣省' in col or '台灣省' in col:
                taiwan_province_column = col.strip()
                break
        
        if taiwan_province_column is None:
            print("警告: 找不到臺灣省欄位！")
            return None
        
        print(f"\n找到臺灣省欄位: '{taiwan_province_column}'")
        print(f"臺灣省的資料: {df[taiwan_province_column].tolist()}")
        
        # 建立新的DataFrame，包含原有資料
        new_df = df.copy()
        
        # 為每個台灣省縣市建立新欄位，使用相同的資料
        for city in taiwan_province_cities:
            new_df[city] = df[taiwan_province_column]
            print(f"新增欄位: {city}")
        
        # 移除原本的臺灣省欄位（可選）
        # new_df = new_df.drop(columns=[taiwan_province_column])
        
        # 重新排列欄位順序（年度欄位放第一個，其他按字母順序）
        year_column = df.columns[0]  # 第一欄是年度
        other_columns = [col for col in new_df.columns if col != year_column]
        other_columns.sort()  # 按字母順序排列
        
        final_columns = [year_column] + other_columns
        new_df = new_df[final_columns]
        
        # 儲存結果 (使用UTF-8 BOM以確保Excel正確顯示中文)
        new_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n處理完成！")
        print(f"原始欄位數量: {len(df.columns)}")
        print(f"新增縣市數量: {len(taiwan_province_cities)}")
        print(f"最終欄位數量: {len(new_df.columns)}")
        print(f"已儲存為: {output_file}")
        
        print(f"\n新增的縣市欄位:")
        for city in taiwan_province_cities:
            print(f"  ✓ {city}")
        
        print(f"\n最終資料預覽:")
        print(new_df.head())
        
        print(f"\n最終欄位名稱:")
        print(list(new_df.columns))
        
        return new_df
        
    except FileNotFoundError:
        print(f"錯誤: 找不到檔案 '{input_file}'")
        return None
    except Exception as e:
        print(f"處理過程中發生錯誤: {str(e)}")
        return None

def verify_expansion(df):
    """
    驗證擴展結果
    """
    if df is None:
        return
    
    print("\n=== 擴展結果驗證 ===")
    
    # 檢查新增的縣市欄位
    taiwan_cities = [
        '基隆市', '宜蘭縣', '桃園縣', '新竹縣', '苗栗縣',
        '南投縣', '彰化縣', '新竹市', '雲林縣', '嘉義縣',
        '屏東縣', '花蓮縣', '臺東縣', '澎湖縣'
    ]
    
    found_cities = []
    missing_cities = []
    
    for city in taiwan_cities:
        if city in df.columns:
            found_cities.append(city)
        else:
            missing_cities.append(city)
    
    print(f"成功新增的縣市欄位 ({len(found_cities)}個):")
    for city in found_cities:
        print(f"  ✓ {city}")
    
    if missing_cities:
        print(f"\n未成功新增的縣市 ({len(missing_cities)}個):")
        for city in missing_cities:
            print(f"  ✗ {city}")
    
    # 檢查資料一致性（以2025年為例）
    if len(found_cities) > 0 and '臺灣省 ' in df.columns:
        print(f"\n資料一致性檢查 (2025年):")
        taiwan_province_2025 = df[df.iloc[:, 0] == 2025]['臺灣省 '].values
        if len(taiwan_province_2025) > 0:
            expected_value = taiwan_province_2025[0]
            print(f"臺灣省 2025年數值: {expected_value}")
            
            # 檢查新增縣市的數值是否一致
            consistent_cities = []
            inconsistent_cities = []
            
            for city in found_cities[:3]:  # 只檢查前3個作為示例
                city_2025 = df[df.iloc[:, 0] == 2025][city].values
                if len(city_2025) > 0 and city_2025[0] == expected_value:
                    consistent_cities.append(city)
                else:
                    inconsistent_cities.append(city)
            
            if consistent_cities:
                print(f"資料一致的縣市: {consistent_cities}")
            if inconsistent_cities:
                print(f"資料不一致的縣市: {inconsistent_cities}")
                
# 執行擴展作業
if __name__ == "__main__":
    # 檔案路徑設定
    input_file = r'C:\\Project\\Houseprice\\jobanalysis\\未整理\\歷年最低生活費.CSV'
    output_file = r'C:\\Project\\Houseprice\\jobanalysis\\未整理\\歷年最低生活費clean.CSV'
    
    print("開始擴展台灣省資料到各縣市...")
    expanded_df = expand_taiwan_province_data(input_file, output_file)
    
    if expanded_df is not None:
        verify_expansion(expanded_df)
        print("\n擴展作業完成！")
    else:
        print("擴展作業失敗！")