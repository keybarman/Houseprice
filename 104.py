# -*- coding: utf-8 -*-
import time
import pandas as pd
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# 匯入 Pillow 和 io 函式庫，用於新的截圖方法
from PIL import Image
import io

# --- 使用者設定區 ---
CHROME_DRIVER_PATH = "C:\\chromedriver\\chromedriver.exe"

# --- 程式設定 ---
MAX_MAIN_CATEGORIES = 18
MAX_SUB_CATEGORIES = 6
BASE_URL = "https://guide.104.com.tw/salary/cat/"
SCREENSHOT_DIR = "104_screenshots"
# --- 設定結束 ---

# 一個更安全的檔名清理函式
def sanitize_filename(filename):
    """移除檔名中的非法字元，避免存檔失敗。"""
    return re.sub(r'[\\/*?:"<>|]', '_', filename)

if "請填寫" in CHROME_DRIVER_PATH:
    print("錯誤：請務必在程式碼中設定 CHROME_DRIVER_PATH 的正確路徑！")
    exit()

try:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    # 【修改】加入更多選項，偽裝成真人瀏覽器並提高穩定性
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(service=Service(executable_path=CHROME_DRIVER_PATH), options=chrome_options)
    # 【修改】隱藏自動化特徵
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
        '''
    })
    print("WebDriver 啟動成功。")
except Exception as e:
    print(f"WebDriver 啟動失敗，請檢查 CHROME_DRIVER_PATH 是否正確。錯誤訊息：{e}")
    exit()

if not os.path.exists(SCREENSHOT_DIR):
    os.makedirs(SCREENSHOT_DIR)
    print(f"已建立截圖儲存資料夾: {SCREENSHOT_DIR}")

final_results = []

try:
    driver.set_page_load_timeout(30)
    
    try:
        print(f"正在前往主頁處理 Cookie...")
        driver.get("https://guide.104.com.tw/salary/")
    except TimeoutException:
        print("錯誤：載入主頁面時超時。請檢查您的網路連線或網站狀態。")
        driver.quit()
        exit()

    # 【修改】大幅延長等待時間
    wait = WebDriverWait(driver, 25)
    try:
        cookie_agree_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "js-cookie-agree")))
        cookie_agree_button.click()
        print("Cookie 同意按鈕已點擊。")
        time.sleep(1)
    except TimeoutException:
        print("未找到 Cookie 同意視窗，或已同意，繼續執行。")

    print("\n--- 開始自動生成代碼並進行探測 ---")
    
    for main_cat_num in range(1, MAX_MAIN_CATEGORIES + 1):
        print(f"\n正在探測主類別代碼: {main_cat_num:03d}...")
        
        for sub_cat_num in range(1, MAX_SUB_CATEGORIES + 1):
            job_code = f"2{main_cat_num:03d}{sub_cat_num:03d}000"
            target_url = BASE_URL + job_code
            
            try:
                driver.get(target_url)
                time.sleep(1)
                current_url = driver.current_url
                
                if target_url not in current_url:
                    print(f"  -> 警告: 頁面被強制跳轉至 {current_url}，視為無效代碼，結束探測主類別 {main_cat_num:03d}。")
                    break

                error_element = driver.find_elements(By.XPATH, "//*[contains(text(), '查無此職務類別')]")
                if error_element:
                    print(f"  -> 代碼 {job_code} 無效，結束探測主類別 {main_cat_num:03d}。")
                    break

                content_block = wait.until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "div.container.main"))
                )
                job_name_element = content_block.find_element(By.CSS_SELECTOR, "h1.h2")
                salary_element = content_block.find_element(By.CLASS_NAME, "salary-t")
                
                job_name = job_name_element.text
                salary = salary_element.text
                
                print(f"  -> 成功! [{job_name}] - {salary}")
                final_results.append({'職務類別': job_name, '平均月薪': salary, '職務代碼': job_code})
                
                try:
                    time.sleep(0.5)
                    sanitized_job_name = sanitize_filename(job_name)
                    screenshot_filename = f"{job_code}-{sanitized_job_name}.png"
                    screenshot_path = os.path.join(SCREENSHOT_DIR, screenshot_filename)
                    
                    png_data = content_block.screenshot_as_png
                    image = Image.open(io.BytesIO(png_data))
                    image.save(screenshot_path)
                    
                    print(f"     -> 截圖已儲存至: {screenshot_path}")
                except Exception as e:
                    print(f"     -> 警告: 儲存截圖失敗。錯誤訊息: {e}")
                
                time.sleep(0.5)

            except TimeoutException:
                print(f"  -> 警告: 頁面 {target_url} 等待資料時逾時，可能為無效頁面，結束探測主類別 {main_cat_num:03d}。")
                break
            except Exception as e:
                print(f"  -> 錯誤: 發生未預期錯誤: {e.__class__.__name__}，結束探測主類別 {main_cat_num:03d}。")
                break

finally:
    driver.quit()
    print("\n瀏覽器已關閉。")

if final_results:
    df = pd.DataFrame(final_results)
    output_filename = '104_salary_data_bruteforce.csv'
    df.to_csv(output_filename, index=False, encoding='utf-8-sig')

    print("\n==========================================")
    print(f"探測完成！資料已成功儲存至檔案: {output_filename}")
    print(f"共抓取到 {len(final_results)} 筆資料。")
    print("==========================================")
    print("\n資料預覽:")
    print(df.head())
else:
    print("\n程式執行完畢，但未抓取到任何資料。")
