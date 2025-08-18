import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import sys
import os
from urllib.parse import urljoin, urlparse
import re
import concurrent.futures
from threading import Lock
import threading
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

results_lock = Lock()
print_lock = Lock()

class OptimizedScraper:
    def __init__(self, max_workers=20, batch_size=50):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.session = self.create_optimized_session()
        self.results = []
        self.processed_count = 0
        self.total_downloads = 0
        
       
        self.title_pattern = re.compile(r'工業及服務業受僱員工全年總薪資中位數及分布統計結果')
        self.year_pattern = re.compile(r'(\d{2,3})年')
        self.filename_pattern = re.compile(r'[<>:"/\\|?*\s]+')
        
    def create_optimized_session(self):
        """創建優化的requests session"""
        session = requests.Session()
        
        
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,
            pool_maxsize=50,
            pool_block=False
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session

    def print_progress(self, current, total, found_count, current_title=""):
        """顯示進度條和當前狀態（線程安全）"""
        with print_lock:
            progress = (current) / total * 100
            bar_length = 30
            filled_length = int(bar_length * progress / 100)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            
            sys.stdout.write('\r')
            sys.stdout.write(f'進度: [{bar}] {progress:.1f}% ({current}/{total}) | 已找到: {found_count} 筆')
            if current_title:
                sys.stdout.write(f' | 當前: {current_title[:50]}...')
            sys.stdout.flush()

    def is_target_title(self, title_text):
        """匹配所有年份的薪資統計結果（優化版）"""
        if not title_text:
            return False
        
        
        cleaned_title = ''.join(title_text.split())
        return bool(self.title_pattern.search(cleaned_title))

    def extract_year_from_title(self, title_text):
        """從標題中提取年份（優化版）"""
        match = self.year_pattern.search(title_text)
        return match.group(1) if match else "未知年份"

    def get_download_links_fast(self, soup, base_url):
        """快速版本的下載連結提取"""
        download_links = []
        
        
        selectors = [
            'a[href*="Upload"]',
            'a[href$=".pdf"]',
            'a[href$=".xlsx"]',
            'a[href$=".xls"]',
        ]
        
        seen_urls = set()
        
        for selector in selectors:
            try:
                links = soup.select(selector)
                for link in links[:5]:  
                    href = link.get('href')
                    if href and href not in seen_urls:
                        full_url = urljoin(base_url, href)
                        if full_url not in seen_urls:
                            seen_urls.add(full_url)
                            download_links.append({
                                'url': full_url,
                                'text': link.get_text(strip=True) or '下載連結',
                                'type': self.get_file_type(full_url)
                            })
            except Exception:
                continue
                
        return download_links

    def get_file_type(self, url):
        """從URL判斷檔案類型（優化版）"""
        url_lower = url.lower()
        if '.pdf' in url_lower:
            return 'PDF'
        elif '.xlsx' in url_lower or '.xls' in url_lower:
            return 'Excel'
        elif '.doc' in url_lower:
            return 'Word'
        elif '.zip' in url_lower:
            return 'ZIP'
        else:
            return '其他'

    def process_batch(self, sid_batch, base_url, download_folder):
        """批次處理多個頁面"""
        batch_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_sid = {
                executor.submit(self.process_single_page_fast, sid, base_url, download_folder): sid 
                for sid in sid_batch
            }
            
            for future in concurrent.futures.as_completed(future_to_sid):
                sid = future_to_sid[future]
                try:
                    success, result = future.result()
                    if success:
                        batch_results.append(result)
                except Exception as e:
                    pass
                
                self.processed_count += 1
                
                
                if self.processed_count % 10 == 0:
                    self.print_progress(self.processed_count, self.total_ids, len(self.results))
        
        return batch_results

    def process_single_page_fast(self, sid, base_url, download_folder):
        """快速處理單個頁面"""
        url = base_url + str(sid)
        
        try:
            
            resp = self.session.get(url, timeout=5)
            
            if resp.status_code != 200:
                return False, None
            
            
            try:
                soup = BeautifulSoup(resp.text, "lxml")
            except:
                soup = BeautifulSoup(resp.text, "html.parser")
            
           
            title = self.find_title_fast(soup)
            
            if not title:
                return False, None
                
            title_text = title.get_text(strip=True)
            
            if self.is_target_title(title_text):
                year = self.extract_year_from_title(title_text)
                
            
                download_links = self.get_download_links_fast(soup, url)
                
                result = {
                    "sid": sid, 
                    "title": title_text, 
                    "year": year,
                    "url": url,
                    "download_links": download_links,
                    "download_count": len(download_links),
                    "found_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
          
                with print_lock:
                    print(f"\n找到：ID {sid} ({year}年) - {len(download_links)} 個檔案")
                
                
                if download_links:
                    self.download_files_batch(download_links, year, download_folder)
                
                return True, result
                
        except Exception as e:
            return False, None
        
        return False, None

    def find_title_fast(self, soup):
        """快速尋找標題"""
        
        target_text = soup.find(string=lambda text: text and '工業及服務業受僱員工全年總薪資中位數及分布統計結果' in text)
        if target_text:
            return target_text.parent
        
   
        for tag in ['h1', 'h2', 'h3']:
            title = soup.find(tag)
            if title:
                return title
        
        return None

    def download_files_batch(self, download_links, year, download_folder):
        """批次下載檔案"""
        year_folder = os.path.join(download_folder, f"{year}年")
        os.makedirs(year_folder, exist_ok=True)
        
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for i, link in enumerate(download_links[:3]):  
                file_extension = os.path.splitext(urlparse(link['url']).path)[1] or '.pdf'
                safe_filename = self.create_safe_filename(link['text'], year)
                filename = os.path.join(year_folder, f"{safe_filename}_{i+1}{file_extension}")
                
                future = executor.submit(self.download_file, link['url'], filename)
                futures.append((future, filename, link['text']))
            
            for future, filename, link_text in futures:
                try:
                    success, error = future.result(timeout=10)
                    if success:
                        self.total_downloads += 1
                except:
                    pass

    def create_safe_filename(self, text, year=""):
        """創建安全的檔名（優化版）"""
        safe_chars = self.filename_pattern.sub('_', text)
        safe_chars = safe_chars[:50] 
        
        if year:
            return f"{year}年_{safe_chars}"
        return safe_chars

    def download_file(self, url, filename, max_retries=1):
        """下載檔案（優化版）"""
        try:
            response = self.session.get(url, stream=True, timeout=10)
            response.raise_for_status()
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return True, None
        except Exception as e:
            return False, str(e)

    def scrape_salary_news(self, start_id=230000, end_id=235000):
        """主要爬取函數（優化版）"""
        base_url = "https://www.dgbas.gov.tw/News_Content.aspx?n=3602&s="
        
        self.results = []
        self.processed_count = 0
        self.total_downloads = 0
        self.total_ids = end_id - start_id
        
       
        download_folder = f"薪資統計下載_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(download_folder, exist_ok=True)
        
        print(f"開始高速爬取薪資新聞稿...")
        print(f"範圍: {start_id} - {end_id} (共 {self.total_ids} 個ID)")
        print(f"批次大小: {self.batch_size}, 最大線程數: {self.max_workers}")
        print(f"下載資料夾: {download_folder}")
        print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        start_time = time.time()
        
        
        all_ids = list(range(start_id, end_id))
        
        for i in range(0, len(all_ids), self.batch_size):
            batch = all_ids[i:i + self.batch_size]
            batch_results = self.process_batch(batch, base_url, download_folder)
            
            with results_lock:
                self.results.extend(batch_results)
            
            
            if i % (self.batch_size * 4) == 0:
                elapsed = time.time() - start_time
                speed = self.processed_count / elapsed if elapsed > 0 else 0
                with print_lock:
                    print(f"\n已處理 {self.processed_count}/{self.total_ids} | 速度: {speed:.1f} 頁/秒 | 找到: {len(self.results)} 筆")
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 80)
        print(f"爬取完成！")
        print(f"總共處理: {self.processed_count} 個ID")
        print(f"找到符合條件: {len(self.results)} 筆")
        print(f"總下載檔案: {self.total_downloads} 個")
        print(f"總耗時: {elapsed_time:.2f} 秒")
        print(f"平均速度: {self.processed_count/elapsed_time:.2f} 頁/秒")
        print(f"下載資料夾: {download_folder}")
        
        return self.results, download_folder

def save_results(results, download_folder):
    """儲存結果到CSV"""
    if not results:
        print("沒有找到符合條件的資料")
        return
    
    
    csv_data = []
    for result in results:
        base_info = {
            'sid': result['sid'],
            'year': result['year'],
            'title': result['title'],
            'url': result['url'],
            'download_count': result['download_count'],
            'found_time': result['found_time']
        }
        
        if result['download_links']:
            for i, link in enumerate(result['download_links']):
                row = base_info.copy()
                row.update({
                    'download_url': link['url'],
                    'download_text': link['text'],
                    'file_type': link['type']
                })
                csv_data.append(row)
        else:
            csv_data.append(base_info)
    
    df = pd.DataFrame(csv_data)
    filename = os.path.join(download_folder, "薪資統計清單.csv")
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    
    print(f"儲存清單到: {filename}")
    print("\n找到的資料:")
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['year']}年 - {result['download_count']} 個檔案")

if __name__ == "__main__":
    try:
        
        scraper = OptimizedScraper(max_workers=20, batch_size=100)
        
        
        results, download_folder = scraper.scrape_salary_news(start_id=230000, end_id=235000)
        
        
        save_results(results, download_folder)
        
    except KeyboardInterrupt:
        print("\n\n程式被使用者中斷")
    except Exception as e:
        print(f"\n\n程式執行錯誤: {e}")
        import traceback
        traceback.print_exc()