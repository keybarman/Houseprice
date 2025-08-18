import os
import time
import random
import requests
import csv

BASE_URL    = "https://guide.104.com.tw/salary/cat/"
API_URL     = "https://www.104.com.tw/jobbank/api/salary/salary-guide"
OUTPUT_DIR  = "104_salary_csvs_api"
MAX_MAIN    = 18
MAX_SUB     = 4


os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_salary_data(job_code):
    """
    呼叫 104 JSON API，回傳該 job_code 的薪資列表（list of dict）。
    """
    params = {
        "cat": job_code,
        "salaryType": "MONTHLY",
        "analyze": "workexp"
    }
    headers = {
        "Referer": BASE_URL + job_code,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)…",
        "X-Requested-With": "XMLHttpRequest"
    }
    resp = requests.get(API_URL, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data", {}).get("data", [])
    results = []
    for it in items:
        results.append({
            "職務代碼": job_code,
            "職務":     it.get("jobName", "").strip(),
            "月薪P25":  it.get("salaryStart", "").strip(),
            "月薪P75":  it.get("salaryEnd", "").strip(),
            "月均薪":   it.get("salaryMean", "").strip(),
            "職缺數":   str(it.get("count", "")).strip(),
        })
    return results

def main():
    codes = [
        f"2{m:03d}{s:03d}000"
        for m in range(1, MAX_MAIN + 1)
        for s in range(1, MAX_SUB + 1)
    ]
    print(f"共 {len(codes)} 個分類，開始呼叫 API…")

    for idx, code in enumerate(codes, 1):
        print(f"[{idx}/{len(codes)}] {code} → ", end="", flush=True)
        try:
            rows = fetch_salary_data(code)
            if not rows:
                print("無資料")
            else:
                out_path = os.path.join(OUTPUT_DIR, f"{code}.csv")
                with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=[
                        "職務代碼", "職務", "月薪P25", "月薪P75", "月均薪", "職缺數"
                    ])
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"存檔({len(rows)} 筆)")
        except Exception as e:
            print("錯誤:", e)

        time.sleep(random.uniform(0.5, 1.2))

    print("全部完成！CSV 全在：", OUTPUT_DIR)

if __name__ == "__main__":
    main()
