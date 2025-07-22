import pandas as pd

# 讀取資料
salary_regular_df = pd.read_csv('C:\\Users\\FM_pc\\Desktop\\DATA\\歷年受僱員工每人每月經常性薪資.csv', encoding='big5')
salary_total_df = pd.read_csv('C:\\Users\\FM_pc\\Desktop\\DATA\\歷年受僱員工每人每月總薪資.csv', encoding='big5')

# 篩選出包含每年每月的資料（6位數）
salary_regular_month_df = salary_regular_df[salary_regular_df['年月別'].apply(lambda x: len(str(x)) == 6)]
salary_total_month_df = salary_total_df[salary_total_df['年月別'].apply(lambda x: len(str(x)) == 6)]

# 儲存為 CSV 檔案
salary_regular_month_df.to_csv('包含每年每月_經常性薪資.csv', index=False)
salary_total_month_df.to_csv('包含每年每月_總薪資.csv', index=False)

print("包含每年每月的資料已儲存成功！")
