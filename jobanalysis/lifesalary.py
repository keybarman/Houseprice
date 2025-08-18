import pandas as pd
import math

# 讀取薪資資料與勞健保級距資料（Big5 編碼為台灣常用）
salary_df = pd.read_csv("C:\\Project\\BI\\參考\\salary\\各縣市中位數月薪.csv",  encoding='cp950')
grade_df = pd.read_csv("C:\\Project\\BI\\參考\\salary\\勞健保級距.csv", encoding='cp950')

# 取得唯一且排序後的投保級距
grades = sorted(set(grade_df["薪資"].dropna().astype(int)))

# 所得稅級距與累進差額設定
tax_brackets = [
    {"min": 0, "max": 590000, "rate": 0.05, "deduct": 0},
    {"min": 590001, "max": 1330000, "rate": 0.12, "deduct": 41300},
    {"min": 1330001, "max": 2660000, "rate": 0.20, "deduct": 147700},
    {"min": 2660001, "max": 4980000, "rate": 0.30, "deduct": 413700},
    {"min": 4980001, "max": float("inf"), "rate": 0.40, "deduct": 911700},
]

# 計算所得稅額函數
def calculate_tax(salary_yearly):
    for bracket in tax_brackets:
        if bracket["min"] <= salary_yearly <= bracket["max"]:
            return salary_yearly * bracket["rate"] - bracket["deduct"]
    return 0

# 找出對應勞健保投保級距（不超過月薪的最大級距）
def find_insurance_grade(salary):
    eligible_grades = [g for g in grades if g <= salary]
    return max(eligible_grades) if eligible_grades else grades[0]

# 健保費：級距不得低於 26400，費率 1.55%，無條件進位
def calculate_health_insurance(grade):
    base = max(grade, 26400)
    return math.ceil(base * 0.0155)

# 勞保費：級距不得低於 11100，費率 2.4%，無條件進位
def calculate_labor_insurance(grade):
    base = max(grade, 11100)
    return math.ceil(base * 0.024)

salary_df["月薪資"]  = salary_df["中位數年薪"] / 12


salary_df["所得稅額"] = salary_df["中位數年薪"].apply(calculate_tax).round(0).astype(int)

salary_df["投保級距"] = salary_df["月薪資"].apply(find_insurance_grade)


salary_df["健保費"] = salary_df["投保級距"].apply(calculate_health_insurance)
salary_df["勞保費"] = salary_df["投保級距"].apply(calculate_labor_insurance)


salary_df["可支配年所得"] = (
    salary_df["中位數年薪"] - salary_df["所得稅額"] - salary_df["最低生活費"] - (salary_df["健保費"] + salary_df["勞保費"]) * 12
)
salary_df["可支配月薪"] = (salary_df["可支配年所得"] / 12).round(0).astype(int)


salary_df.to_csv("C:\\Project\\BI\\參考\\salary\\全國近五年所得比(中位數).csv", index=False, encoding='cp950')
