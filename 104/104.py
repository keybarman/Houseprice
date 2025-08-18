import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
from bs4 import BeautifulSoup

class Job104SalaryScraper:
    def __init__(self):
        self.setup_driver()
        self.all_salary_data = []
        
        
        self.job_categories = {
            '2001': ['經營／幕僚類人員', '人力資源類人員', '行政／總務／法務類人員', '財會／金融專業人員'],
            '2002': ['行政／總務／法務', '人力資源', '經營幕僚', '專案管理'],
            '2003': ['業務銷售', '門市／店員／專櫃人員', '業務助理', '電話行銷／客服'],
            '2004': ['客服', '門市服務', '業務開發', '通路行銷'],
            '2005': ['行銷企劃', '廣告企劃', '市場調查', '產品企劃'],
            '2006': ['公關', '活動企劃', '媒體企劃', '品牌企劃'],
            '2007': ['大眾傳播', '編輯／採訪', '藝術設計', '影音製作'],
            '2008': ['軟體設計工程師', '網路管理工程師', '系統分析師', 'MIS程式設計師'],
            '2009': ['電子工程師', '機械工程師', '品管工程師', '製程工程師'],
            '2010': ['研發工程師', '產品工程師', '測試工程師', '技術支援工程師'],
            '2011': ['營建工程師', '水電工程師', '建築設計師', '室內設計師'],
            '2012': ['醫護人員', '藥師', '醫檢師', '營養師'],
            '2013': ['教師', '補習班老師', '安親班老師', '家教'],
            '2014': ['服務業', '餐飲服務', '美容美髮', '清潔服務'],
            '2015': ['操作技術', '維修技術', '品管技術', '生產技術'],
            '2016': ['營造技術', '裝潢技術', '水電技術', '機械技術'],
            '2017': ['物流運輸', '倉儲管理', '配送人員', '司機'],
            '2018': ['其他職類', '農林漁牧', '軍警消防', '法律相關']
        }
    
    def setup_driver(self):
        """設置Chrome瀏覽器驅動"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        

        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-features=TranslateUI')
        chrome_options.add_argument('--disable-ipc-flooding-protection')
        chrome_options.add_argument('--log-level=3')  
        

        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        

        chromedriver_path = r"C:\chromedriver\chromedriver.exe"
        service = Service(chromedriver_path)
        
        try:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except:

            print("嘗試使用舊版Selenium語法...")
            self.driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
        
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    def random_delay(self, min_seconds=1, max_seconds=3):
        """隨機延遲以避免被偵測"""
        time.sleep(random.uniform(min_seconds, max_seconds))
    
    def scrape_salary_data(self, category_code, subcategory_index=1):
        """爬取特定類別的薪資資料"""

        full_code = f"{category_code}{subcategory_index:03d}000"
        url = f"https://guide.104.com.tw/salary/cat/{full_code}"
        
        print(f"正在爬取: {url}")
        
        try:
            self.driver.get(url)
            self.random_delay(2, 4)
            

            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "salary-table"))
            )

            salary_table = self.driver.find_element(By.CLASS_NAME, "salary-table")
            rows = salary_table.find_elements(By.TAG_NAME, "tr")
            
            category_data = []
            
            for row in rows[1:]:  
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        job_title = cells[0].text.strip()
                        p25_salary = cells[1].text.strip()
                        p75_salary = cells[2].text.strip()
                        avg_salary = cells[3].text.strip()
                        job_count = cells[4].text.strip()
                        
                        salary_info = {
                            '類別代碼': full_code,
                            '職務': job_title,
                            '月薪範圍P25': p25_salary,
                            '月薪範圍P75': p75_salary,
                            '月均薪': avg_salary,
                            '職缺': job_count
                        }
                        
                        category_data.append(salary_info)
                        
                except Exception as e:
                    print(f"解析行資料時發生錯誤: {e}")
                    continue
            
            print(f"成功爬取 {len(category_data)} 筆資料")
            return category_data
            
        except TimeoutException:
            print(f"頁面載入超時: {url}")
            return []
        except Exception as e:
            print(f"爬取過程發生錯誤: {e}")
            return []
    
    def scrape_all_categories(self):
        """爬取所有類別的薪資資料"""
        print("開始爬取所有職業類別的薪資資料...")
        
        for category_code, subcategories in self.job_categories.items():
            print(f"\n正在處理類別: {category_code}")
            
            
            for i in range(1, min(5, len(subcategories) + 1)):
                subcategory_data = self.scrape_salary_data(category_code, i)
                self.all_salary_data.extend(subcategory_data)
                self.random_delay(1, 2) 
        
        print(f"\n總共爬取了 {len(self.all_salary_data)} 筆薪資資料")
    
    def save_to_csv(self, filename="104_salary_data.csv"):
        """將資料儲存為CSV檔案"""
        if self.all_salary_data:
            df = pd.DataFrame(self.all_salary_data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"資料已儲存至: {filename}")
        else:
            print("沒有資料可儲存")
    
    def close(self):
        """關閉瀏覽器驅動"""
        if hasattr(self, 'driver'):
            self.driver.quit()

def main():
    """主函數"""
    scraper = Job104SalaryScraper()
    
    try:

        scraper.scrape_all_categories()
        

        scraper.save_to_csv()
        

        if scraper.all_salary_data:
            print("\n前10筆資料預覽:")
            df = pd.DataFrame(scraper.all_salary_data[:10])
            print(df.to_string(index=False))
            
    except KeyboardInterrupt:
        print("\n程式被使用者中斷")
    except Exception as e:
        print(f"程式執行過程中發生錯誤: {e}")
    finally:
        scraper.close()

if __name__ == "__main__":

    print("請先安裝必要套件:")
    print("pip install selenium pandas beautifulsoup4 requests")
    print("並下載ChromeDriver: https://chromedriver.chromium.org/")
    print("\n開始執行爬蟲...")
    
    main()