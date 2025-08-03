import pandas as pd
import math

# 輸入資料路徑
salary_path = "C:\\Project\\BI\\參考\\salary\\主要產業月薪總薪資.csv"
output_path = "C:\\Project\\BI\\參考\\salary\\主要產業月薪總薪資(漲幅).csv"

# 讀取資料（Big5 對應 cp950）
salary_df = pd.read_csv(salary_path, encoding='cp950')


# 可支配月薪
salary_df["月平均總薪資"] = (salary_df["月平均總薪資"] / 12).round(0).astype(int)

# 匯出處理後資料
salary_df.to_csv(output_path, index=False, encoding='cp950')

print("✅ 清洗完成，已匯出至：", output_path)
