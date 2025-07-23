from sqlalchemy import create_engine
import pandas as pd

# 建立資料（dim_month_df） ← 若你已經有就跳過
date_range = pd.date_range(start="2018-01-01", end="2025-12-31", freq="MS")
dim_month_df = pd.DataFrame({
    "年份": date_range.year,
    "月份": date_range.month,
    "年月": date_range.strftime("%Y-%m"),
    "年月名稱": date_range.strftime("%Y年%m月"),
    "季度": date_range.quarter.map(lambda q: f"Q{q}")
})

# 資料庫連線資訊（請改成你的）
db_user = 'abc123456'
db_pass = 'abcABC123'
db_host = 'localhost'
db_port = '3306'
db_name = 'houses'
table_name = 'buydata'

# 建立 SQLAlchemy 引擎
engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4")

# 匯入資料表（如果已存在會覆蓋）
dim_month_df.to_sql(name="dim_month", con=engine, if_exists="replace", index=False)
print("✅ dim_month 已成功匯入 MySQL！")
