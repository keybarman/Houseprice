import pandas as pd
import math


salary_path = "C:\\Project\\BI\\參考\\salary\\主要產業月薪總薪資.csv"
output_path = "C:\\Project\\BI\\參考\\salary\\主要產業月薪總薪資(漲幅).csv"


salary_df = pd.read_csv(salary_path, encoding='cp950')

salary_df["月平均總薪資"] = (salary_df["月平均總薪資"] / 12).round(0).astype(int)


salary_df.to_csv(output_path, index=False, encoding='cp950')

print("清洗完成，已匯出至：", output_path)