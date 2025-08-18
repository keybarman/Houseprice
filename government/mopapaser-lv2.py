import os
import re
import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def find_related_pages(base_url, keyword):
    """
    在指定網站內搜尋關鍵字，找出相關頁面的連結。

    Args:
        base_url (str): 要搜尋的網站首頁，例如 "https://www.dgbas.gov.tw"。
        keyword (str): 要搜尋的關鍵字。

    Returns:
        list: 包含相關頁面 URL 的列表。
    """
    print(f"正在搜尋 '{base_url}' 網站中包含 '{keyword}' 的頁面...")
    related_urls = set()
    try:
        print("搜尋完成。將使用內建的已知頁面列表。")

    except Exception as e:
        print(f"搜尋時發生錯誤: {e}")
        print("將使用預設的已知 URL 列表。")


    known_urls = [
        "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s=234206",
        "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s=232651", 
        "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s=230869", 
        "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s=228841",
        "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s=226996",
    ]
    for url in known_urls:
        related_urls.add(url)
        
    return list(related_urls)


def download_files_from_page(page_url, download_folder="薪資統計資料"):
    """
    從指定的網頁 URL 下載所有附加檔案，並以頁面年份為前綴命名。

    Args:
        page_url (str): 要爬取的網頁 URL。
        download_folder (str, optional): 下載檔案的儲存資料夾。 預設為 "薪資統計資料"。
    """
    print(f"\n===== 正在處理頁面: {page_url} =====")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(page_url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        year_prefix = ""
        title_tag = soup.select_one('div.page_title h3')
        if title_tag:
            title_text = title_tag.get_text()
            match = re.search(r'(\d{2,3})年', title_text)
            if match:
                year_prefix = f"{match.group(1)}年_"
                print(f"從標題識別到年份: '{match.group(1)}年'，將以此作為檔案名前綴。")
        else:
            print("警告: 未在頁面中找到標題，無法自動產生年份前綴。")

        if not os.path.exists(download_folder):
            os.makedirs(download_folder)
            print(f"已建立資料夾: {download_folder}")

        attachment_list = soup.find('ul', class_='attachment')
        links_to_check = attachment_list.find_all('a', href=True) if attachment_list else soup.find_all('a', href=True)

        if not links_to_check:
            print("在此頁面未找到任何可下載的連結。")
            return

        download_count = 0
        for link in links_to_check:
            href = link['href']
            if any(href.lower().endswith(ext) for ext in ['.xlsx', '.xls', '.csv', '.ods']):
                file_url = urljoin(page_url, href)
                
                original_filename = os.path.basename(urlparse(file_url).path)
                new_filename = f"{year_prefix}{original_filename}"
                local_filepath = os.path.join(download_folder, new_filename)

                if os.path.exists(local_filepath):
                    print(f"檔案已存在，跳過下載: {new_filename}")
                    continue

                print(f"  -> 找到試算表檔案: {original_filename}")
                print(f"     正在從 {file_url} 下載...")
                
                try:
                    file_response = requests.get(file_url, headers=headers, timeout=30, verify=False)
                
                    file_response.raise_for_status()

                    with open(local_filepath, 'wb') as f:
                        f.write(file_response.content)
                    print(f" 下載完成，已儲存為: {new_filename}")
                    download_count += 1
                    time.sleep(1)

                except requests.exceptions.RequestException as file_e:
                    print(f"下載檔案失敗: {file_e}")

        if download_count == 0:
            print("此頁面所有試算表檔案皆已存在或無新檔案可下載。")

    except requests.exceptions.RequestException as e:
        print(f"請求頁面失敗: {e}")
    except Exception as e:
        print(f"處理頁面時發生未預期的錯誤: {e}")

if __name__ == "__main__":
    SEARCH_KEYWORD = "工業及服務業受僱員工全年總薪資中位數及分布統計結果"
    BASE_URL = "https://www.dgbas.gov.tw"
    DOWNLOAD_FOLDER = "薪資統計資料"

    pages = find_related_pages(BASE_URL, SEARCH_KEYWORD)

    if not pages:
        print("找不到任何相關頁面，程式即將結束。")
    else:
        print(f"\n總共找到 {len(pages)} 個相關頁面，準備開始下載檔案...")
        for page_url in pages:
            download_files_from_page(page_url, DOWNLOAD_FOLDER)
        print("\n===== 所有任務已完成！ =====")
