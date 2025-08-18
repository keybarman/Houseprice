from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

cat1_list = [
    (10, "管理幕僚_人資_行政"),
    (13, "行銷_企劃_專案管理"),
    (16, "機械機具_生產製程"), 
    (19, "營建_製圖_施作"), 
    (22, "影視傳媒_出版翻譯"), 
    (25, "學術研究_教育師資"), 
    (28, "生活服務_農林漁牧"), 
    (11, "金融保險_財會_稽核"),
    (14, "電腦系統_資訊_軟硬體"),
    (17, "操作_維修_技術服務"), 
    (20, "醫療_護理_保健"),
    (23, "美術設計"), 
    (26, "幼教才藝_補習進修"), 
    (29, "軍警消防_保全相關"), 
    (12, "業務_貿易_客服_門市"),
    (15, "光電IC_電子通訊"),
    (18, "採購_物流_品管檢測"), 
    (21, "生物科技_化學製藥"), 
    (24, "法務專利_顧問諮詢"), 
    (27, "美容美髮_餐飲旅遊"), 
]

chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1280,800')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')  
chrome_options.add_argument("--lang=zh-TW")
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')

from selenium.webdriver.chrome.service import Service
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)


count = 0

for cat1, folder in cat1_list:
    for cat2 in range(1, 10):
        for cat3 in range(1, 19):
            code = f"{cat1:02d}{cat2:02d}{cat3:02d}"
            url = f"https://www.jobsalary.com.tw/salarycompare.aspx?codeNo={code}"
            print(f"正在爬取 {url}")
            try:
                driver.get(url)
                time.sleep(random.uniform(1.5, 3.5))  
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                job_span = soup.select_one('div.descJobSalary span')
                job_category = job_span.text.strip() if job_span else None
                if not job_category:
                    print(f"查無職業類別，跳過 {code}")
                    continue
                table = soup.find('table', class_='salaryCompareTable')
                if table:
                    rows = table.find_all('tr')
                    data = []
                    for row in rows:
                        cols = [td.get_text(strip=True).replace('$', '').replace(',', '') for td in row.find_all(['td', 'th'])]
                        if cols:
                            data.append(cols)
                    if len(data) > 1:
                        df = pd.DataFrame(data[1:], columns=data[0])
                        df.insert(0, '職業類別', job_category)
                        os.makedirs(folder, exist_ok=True)
                        filename = f"{job_category.replace('/', '_').replace('╱', '_').replace(' ', '').replace('|', '')}.csv"
                        filepath = os.path.join(folder, filename)
                        df.to_csv(filepath, index=False, encoding='utf-8-sig')
                        print(f"已儲存: {filepath}")
                    else:
                        print(f"無表格資料，跳過 {code}")
                else:
                    print(f"找不到表格，跳過 {code}")
                count += 1

                if count % 20 == 0:
                    sleep_long = random.uniform(30, 90)
                    print(f"\n--- 已爬{count}頁，長休息 {sleep_long:.2f} 秒 ---\n")
                    time.sleep(sleep_long)
                else:
                    sleep_short = random.uniform(1, 10)
                    print(f"隨機休息 {sleep_short:.2f} 秒...\n")
                    time.sleep(sleep_short)
            except Exception as e:
                print(f"出現異常：{e}")
                continue

driver.quit()
