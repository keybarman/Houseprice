import pandas as pd
from difflib import get_close_matches

# 讀取來源職務名稱（例如 1111）
df_source = pd.read_csv("C:\\Class\\Houseprice\\1111\\1111各職業薪資資料\\all_職業薪資總表.csv",  encoding='utf-8-sig')  # 欄位名稱假設為「職務名稱」

# 讀取 104 的標準職務類別名稱
df_target = pd.read_csv("C:\\Class\\Houseprice\\jobanalysis\\104joblist.csv", encoding='cp950')  # 欄位名稱為「職務類別」
standard_list = df_target['職務類別'].dropna().tolist()

# 建立對照結果清單
results = []
for name in df_source['職業類別'].dropna().unique():
    matches = get_close_matches(name, standard_list, n=1, cutoff=0.4)
    best_match = matches[0] if matches else '(無匹配)'
    results.append({
        '原始名稱': name,
        '最佳對應職務類別': best_match
    })

# 匯出為對照結果
df_result = pd.DataFrame(results)
df_result.to_csv("C:\\Class\\Houseprice\\jobanalysis\\模糊比對結果.csv", index=False, encoding='utf-8-sig')
print("比對完成，結果已儲存為 模糊比對結果.csv")