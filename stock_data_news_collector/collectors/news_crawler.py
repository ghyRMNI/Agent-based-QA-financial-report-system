"""
iWenCai News Crawler using Selenium
"""

import pandas as pd
import time
import urllib.parse
import re
from datetime import datetime, timedelta
import calendar
import os
from typing import Optional, List, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_BASE_DIR, NEWS_DATA_DIR, LOG_DIR, ERROR_SCREENSHOTS_DIR, HEADLESS_MODE, BROWSER_WINDOW_SIZE, USER_AGENT
from utils.file_utils import ensure_directory_exists

class NewsCrawler:
    """iWenCai News Crawler - Monthly Time Range Split Version"""
    
    def __init__(self, visible: bool = not HEADLESS_MODE):
        self.driver = None
        self.visible = visible
        self.wait = None
        self.screenshot_dir = os.path.join(LOG_DIR, ERROR_SCREENSHOTS_DIR)
        # 不在这里创建目录，只有在实际需要保存截图时才创建
    
    def setup_driver(self):
        """Setup browser driver"""
        print("Starting Edge browser...")
        edge_options = Options()
        
        if not self.visible:
            edge_options.add_argument('--headless')
        
        # Anti-detection configuration
        edge_options.add_argument('--disable-blink-features=AutomationControlled')
        edge_options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        edge_options.add_experimental_option('useAutomationExtension', False)
        edge_options.add_argument('--no-sandbox')
        edge_options.add_argument('--disable-dev-shm-usage')
        edge_options.add_argument(f'--window-size={BROWSER_WINDOW_SIZE}')
        edge_options.add_argument(f'--user-agent={USER_AGENT}')
        
        # 禁用浏览器日志输出
        edge_options.add_argument('--log-level=3')  # 只显示致命错误
        edge_options.add_argument('--silent')
        
        # 使用指定的EdgeDriver路径
        try:
            from selenium.webdriver.edge.service import Service
            import subprocess
            
            driver_path = r"D:\edgeDriver\msedgedriver.exe"
            # 禁用EdgeDriver日志输出
            service = Service(
                executable_path=driver_path,
                log_output=subprocess.DEVNULL
            )
            self.driver = webdriver.Edge(service=service, options=edge_options)
            print("Browser started successfully (Edge)")
        except Exception as e:
            print(f"错误: 无法启动 Edge 浏览器")
            print(f"原因: {e}")
            print(f"\n请确保:")
            print(f"1. EdgeDriver 路径正确: {driver_path}")
            print(f"2. Edge 浏览器已安装")
            print(f"3. EdgeDriver 版本与 Edge 浏览器版本匹配")
            raise
        
        # Execute anti-detection script
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        self.wait = WebDriverWait(self.driver, 20)

    def take_screenshot(self, error_type: str):
        """Take screenshot when error occurs"""
        try:
            # 只有在实际需要保存截图时才创建目录
            ensure_directory_exists(self.screenshot_dir)
            screenshot_path = os.path.join(self.screenshot_dir, f"{error_type}.png")
            self.driver.save_screenshot(screenshot_path)
            print(f"Screenshot saved: {screenshot_path}")
        except Exception as e:
            print(f"Failed to take screenshot: {e}")
            # 如果保存失败，清理可能创建的空目录
            self._cleanup_empty_dir(self.screenshot_dir)

    def crawl_news_by_monthly_ranges(self, company_name: str, start_date: str, end_date: str, pages_per_month: int = 1) -> Optional[pd.DataFrame]:
        """
        Crawl news by splitting time range into months
        
        Args:
            company_name: Company name
            start_date: Start date "YYYY-MM-DD"
            end_date: End date "YYYY-MM-DD"
            pages_per_month: Pages to crawl per month
            
        Returns:
            DataFrame containing news data or None if failed
        """
        print(f"Crawling news for {company_name} from {start_date} to {end_date}...")
        
        if self.driver is None:
            self.setup_driver()
        
        try:
            # Validate date format
            if not self.validate_dates(start_date, end_date):
                return None
            
            # Generate monthly time ranges
            monthly_ranges = self.generate_monthly_ranges(start_date, end_date)
            print(f"Generated {len(monthly_ranges)} monthly time ranges")
            
            # Crawl data for each time range
            all_news_data = []
            for i, (month_start, month_end) in enumerate(monthly_ranges):
                # Crawl current month data
                month_news = self.crawl_single_month(
                    company_name, month_start, month_end, pages_per_month
                )
                
                if month_news is not None and not month_news.empty:
                    all_news_data.append(month_news)
                    print(f"Month {i+1}: {len(month_news)} news")
                else:
                    print(f"Month {i+1}: 0 news")
                
                # Add delay between requests
                if i < len(monthly_ranges) - 1:
                    time.sleep(2)
            
            # Combine all data
            if all_news_data:
                combined_data = pd.concat(all_news_data, ignore_index=True)
                # Remove duplicates
                combined_data = combined_data.drop_duplicates(subset=['Title', 'Time'], keep='first')
                print(f"Total: {len(combined_data)} news")
                return combined_data
            else:
                print("No news data found")
                return None
            
        except Exception as e:
            print(f"Crawling failed: {e}")
            self.take_screenshot("crawling_error")
            return None

    def crawl_single_month(self, company_name: str, start_date: str, end_date: str, max_pages: int = 1) -> Optional[pd.DataFrame]:
        """
        Crawl news for single time range
        
        Args:
            company_name: Company name
            start_date: Start date
            end_date: End date
            max_pages: Maximum pages to crawl
            
        Returns:
            DataFrame containing news data or None if failed
        """
        try:
            # Build base URL
            base_url = self.construct_base_url(company_name, start_date, end_date)
            
            # Crawl multiple pages
            all_news_data = []
            for page in range(1, max_pages + 1):
                page_url = f"{base_url}&page={page}"
                
                self.driver.get(page_url)
                
                # Wait for page load
                time.sleep(4)
                
                # Check if no data
                if self.check_no_data_page():
                    break
                
                # Extract current page news data
                page_news_data = self.extract_news_data(company_name, page)
                
                if page_news_data is not None and not page_news_data.empty:
                    # Add time range info
                    page_news_data['TimeRange'] = f"{start_date} to {end_date}"
                    all_news_data.append(page_news_data)
                else:
                    break
                
                # Check if last page
                if page < max_pages and not self.has_next_page_simple():
                    break
            
            # Combine current time range data
            if all_news_data:
                return pd.concat(all_news_data, ignore_index=True)
            else:
                return None
                
        except Exception as e:
            print(f"Error crawling single month: {e}")
            self.take_screenshot("single_month_error")
            return None

    def generate_monthly_ranges(self, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """
        Generate monthly time ranges
        
        Args:
            start_date: Start date "YYYY-MM-DD"
            end_date: End date "YYYY-MM-DD"
            
        Returns:
            List of (start_date, end_date) tuples
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        monthly_ranges = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            # Calculate first day of current month
            month_start = current_dt.replace(day=1)
            
            # Calculate last day of current month
            _, last_day = calendar.monthrange(current_dt.year, current_dt.month)
            month_end = current_dt.replace(day=last_day)
            
            # If month end exceeds specified end date, use specified end date
            if month_end > end_dt:
                month_end = end_dt
            
            # If month start is before specified start date, use specified start date
            if month_start < start_dt:
                month_start = start_dt
            
            # Add to result list
            monthly_ranges.append((
                month_start.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d")
            ))
            
            # Move to first day of next month
            if current_dt.month == 12:
                current_dt = current_dt.replace(year=current_dt.year + 1, month=1, day=1)
            else:
                current_dt = current_dt.replace(month=current_dt.month + 1, day=1)
        
        return monthly_ranges

    def has_next_page_simple(self) -> bool:
        """Simple next page detection"""
        try:
            # Find next page button
            next_selectors = [
                "//a[contains(text(), '下一页')]",
                "//a[contains(@class, 'next')]",
                "//button[contains(text(), '下一页')]",
                "//span[contains(text(), '下一页')]"
            ]
            
            for selector in next_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    if element.is_displayed() and element.is_enabled():
                        return True
                except NoSuchElementException:
                    continue
                    
            return False
            
        except Exception:
            return False

    def construct_base_url(self, company_name: str, start_date: str, end_date: str) -> str:
        """Build base URL"""
        encoded_company = urllib.parse.quote(company_name)
        time_range = f"{start_date}~{end_date}"
        
        base_url = (f"https://www.iwencai.com/unifiedwap/inforesult?"
                   f"w={encoded_company}&"
                   f"querytype=news&"
                   f"search1=&"
                   f"search2=&"
                   f"search3={time_range}&"
                   f"search4=&"
                   f"search5=")
        
        return base_url

    def validate_dates(self, start_date: str, end_date: str) -> bool:
        """Validate date format and logic"""
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            if start_dt > end_dt:
                print("Start date cannot be later than end date")
                return False
            
            today = datetime.now()
            if end_dt > today:
                print("Note: End date is in future, may have no data")
            
            return True
            
        except ValueError as e:
            print(f"Date format error: {e}, please use YYYY-MM-DD format")
            return False

    def check_no_data_page(self) -> bool:
        """Check if page shows no data"""
        try:
            no_data_selectors = [
                "//*[contains(text(), '抱歉，没有找到相关的内容')]",
                "//*[contains(text(), '没有找到相关结果')]",
                "//*[contains(text(), '暂无数据')]",
                "//*[contains(text(), 'No results found')]"
            ]
            
            for selector in no_data_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    if element.is_displayed():
                        return True
                except NoSuchElementException:
                    continue
            
            return False
            
        except Exception:
            return False

    def extract_news_data(self, company_name: str, page_number: int = 1) -> Optional[pd.DataFrame]:
        """Extract news data"""
        try:
            # Wait for page load
            time.sleep(2)
            
            # Get page text
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            lines = [line.strip() for line in body_text.split('\n') if line.strip()]
            
            # Find lines containing company name
            company_lines = []
            for line in lines:
                if company_name in line and len(line) > 20:
                    company_lines.append(line)
            
            if company_lines:
                news_data = []
                for i, line in enumerate(company_lines[:50]):
                    # Extract time info
                    news_time = self.extract_time_from_text(line)
                    
                    # Extract title
                    title = self.extract_title_from_text(line, company_name)
                    
                    # Extract source
                    source = self.extract_source_from_text(line)
                    
                    news_data.append({
                        'Title': title,
                        'Link': self.driver.current_url,
                        'Source': source,
                        'Time': news_time,
                        'Summary': line[:150],
                        'Company': company_name,
                        'Page': page_number,
                        'CrawlTime': time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                if news_data:
                    df = pd.DataFrame(news_data)
                    return df
                    
        except Exception as e:
            print(f"Error extracting news data: {e}")
        
        return None

    def extract_time_from_text(self, text: str) -> str:
        """Extract time info from text"""
        time_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{4}\.\d{2}\.\d{2}', # YYYY.MM.DD
            r'\d{4}/\d{2}/\d{2}',   # YYYY/MM/DD
            r'\d{2}-\d{2} \d{2}:\d{2}', # MM-DD HH:MM
            r'\d{4}年\d{1,2}月\d{1,2}日' # Chinese date
        ]
        
        for pattern in time_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
        
        return "Unknown time"

    def extract_title_from_text(self, text: str, company_name: str) -> str:
        """Extract title from text"""
        if text.startswith(company_name):
            title = text[:100] + "..." if len(text) > 100 else text
        else:
            index = text.find(company_name)
            if index >= 0:
                title = text[index:index+100] + "..." if len(text[index:]) > 100 else text[index:]
            else:
                title = text[:100] + "..." if len(text) > 100 else text
        
        return title

    def extract_source_from_text(self, text: str) -> str:
        """Extract source from text"""
        source = "iWenCai"
        source_patterns = [
            r'来源[：:]\s*([^\s\n]+)',
            r'发布[：:]\s*([^\s\n]+)',
            r'供稿[：:]\s*([^\s\n]+)'
        ]
        
        for pattern in source_patterns:
            match = re.search(pattern, text)
            if match:
                source = match.group(1)
                break
        
        return source

    def save_news_data(self, news_data: pd.DataFrame, stock_code: str, timestamp: str) -> str:
        """Save news data to file"""
        news_dir = os.path.join(OUTPUT_BASE_DIR, NEWS_DATA_DIR)
        ensure_directory_exists(news_dir)
        
        filename = os.path.join(news_dir, f"{stock_code}_news_{timestamp}.csv")
        try:
            news_data.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"News data saved to: {filename}")
            return filename
        except Exception as e:
            print(f"Failed to save news data: {e}")
            return ""

    def close(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
            print("Browser closed")
        
        # 清理可能创建的空目录
        self._cleanup_empty_dir(self.screenshot_dir)
        # 如果logger目录也是空的，也清理
        log_dir = LOG_DIR
        if os.path.exists(log_dir) and os.path.isdir(log_dir):
            try:
                if not os.listdir(log_dir):
                    os.rmdir(log_dir)
            except OSError:
                pass
    
    def _cleanup_empty_dir(self, dir_path: str):
        """
        清理空目录
        
        Args:
            dir_path: 目录路径
        """
        try:
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    # 如果父目录也是空的，也删除
                    parent_dir = os.path.dirname(dir_path)
                    if parent_dir and os.path.exists(parent_dir) and os.path.isdir(parent_dir):
                        if not os.listdir(parent_dir):
                            os.rmdir(parent_dir)
        except OSError:
            # 忽略删除失败的情况
            pass