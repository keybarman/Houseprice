import pandas as pd


salary_regular_df = pd.read_csv('C:\\Users\\FM_pc\\Desktop\\DATA\\歷年受僱員工每人每月經常性薪資.csv', encoding='big5')
salary_total_df = pd.read_csv('C:\\Users\\FM_pc\\Desktop\\DATA\\歷年受僱員工每人每月總薪資.csv', encoding='big5')


salary_regular_year_df = salary_regular_df[salary_regular_df['年月別'].apply(lambda x: len(str(x)) == 4)]
salary_total_year_df = salary_total_df[salary_total_df['年月別'].apply(lambda x: len(str(x)) == 4)]


salary_regular_year_df.to_csv('只包含年份_經常性薪資.csv', index=False)
salary_total_year_df.to_csv('只包含年份_總薪資.csv', index=False)

print("只包含年份的資料已儲存成功！")
