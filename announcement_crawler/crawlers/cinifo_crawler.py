import requests
import json
import time
import random
from crawlers.cninfo_db import CninfoAnnouncementDB
from crawlers.driverController import DriverController
import os
from selenium.webdriver.common.by import By


class Cninfo:
    """
    Main crawler class for downloading announcements from cninfo.com.cn
    Handles API queries, file downloads, and database operations
    """

    # Default headers for API requests
    DEFAULT_HEADERS = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "https://www.cninfo.com.cn",
        "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    # API endpoint for querying announcements
    QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"

    def __init__(self, db_path=None):
        """
        Initialize the Cninfo crawler instance

        Attributes:
            db: Database instance for storing announcement records
            searchKey: Search keyword for filtering announcements
            plate: Stock plate/category for filtering
            stock: Stock code to search for
            column: Column/category type
            download_path: Directory path for downloaded files
        """
        # 如果没有指定db_path，使用None（将在下面禁用数据库去重功能）
        self.db = CninfoAnnouncementDB(db_path) if db_path else None
        self.searchKey = ""
        self.plate = ""
        self.stock = ""
        self.column = ""
        self.download_path = ""

    def set_download_dir(self, download_dir):
        """
        Set the download directory for announcement files

        Args:
            download_dir (str): Path to the download directory
        """
        self.download_path = download_dir

    def edit_payload(self, stock_code, column, searchKey, category, plate=None):
        """
        Configure search parameters for announcement queries

        Args:
            stock_code (str): Stock code to search for
            column (str): Column/category type
            searchKey (str): Search keyword
            category (str): Announcement category
            plate (str, optional): Stock plate/category
        """
        self.stock = stock_code
        self.column = column
        self.searchKey = searchKey
        self.plate = plate
        self.category = category

    def query_get(self, start_date, end_date):
        """
        Query total number of pages for announcements in date range

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format

        Returns:
            int: Total number of pages, or None if query fails
        """
        # payload
        payload = {
            "pageNum": "1",
            "pageSize": "30",
            "column": self.column,
            "tabName": "fulltext",
            "plate": self.plate,  # "",
            "stock": self.stock,
            "searchkey": self.searchKey,  # "",
            "secid": "",
            "category": self.category,
            "trade": "",
            "seDate": f"{start_date}~{end_date}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }

        try:
            response = requests.post(
                url=self.QUERY_URL, headers=self.DEFAULT_HEADERS, data=payload, timeout=30
            )

            if response.status_code == 200:
                # 优先尝试使用response.json()，它会自动处理解压
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    # 如果json()失败，尝试手动解压
                    try:
                        import gzip
                        decompressed = gzip.decompress(response.content)
                        data = json.loads(decompressed.decode('utf-8'))
                        print("提示: 使用gzip手动解压成功")
                    except:
                        # 最后尝试brotli
                        try:
                            import brotli
                            decompressed = brotli.decompress(response.content)
                            data = json.loads(decompressed.decode('utf-8'))
                            print("提示: 使用brotli解压成功")
                        except:
                            print("警告: 无法解析API响应")
                            # 检查响应内容类型
                            content_preview = ""
                            try:
                                if hasattr(response, 'text'):
                                    content_preview = response.text[:200]
                                else:
                                    content_preview = response.content[:200].decode('utf-8', errors='ignore')
                            except:
                                content_preview = str(response.content[:200])
                            
                            # 检查是否是HTML错误页面
                            if content_preview.strip().startswith('<!') or '<html' in content_preview.lower():
                                print("错误: API返回了HTML页面而非JSON数据")
                                print("可能的原因:")
                                print("  1. API服务器错误或维护中")
                                print("  2. 请求被拒绝（IP限制、频率限制等）")
                                print("  3. 该股票代码或交易所类型不被支持（港股可能不支持）")
                                print(f"响应预览: {content_preview[:100]}...")
                            else:
                                print(f"响应内容前200字符: {content_preview}")
                            
                            return 0
                
                total_record = data.get("totalRecordNum", 0)
                total_announcement = data.get("totalAnnouncement", 0)
                total_page = data.get("totalpages", 0)
                
                print(f"total records: {total_record}")
                print(f"total announcements: {total_announcement}")
                print(f"total pages: {total_page}")

                if total_record > 0:
                    # Handle case where API returns 0 pages but has data
                    if total_page == 0:
                        total_page = 1
                else:
                    print("该日期范围内未找到年报数据")

                return total_page
            else:
                print(f"请求失败，状态码：{response.status_code}")
                print(f"响应内容: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            print("请求超时，请检查网络连接")
            return None
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None

    def query_record(self, date):
        """
        Query total number of announcements for a specific date

        Args:
            date (str): Query date in YYYY-MM-DD format

        Returns:
            int: Total number of announcements, or None if query fails
        """
        # payload
        payload = {
            "pageNum": "1",
            "pageSize": "30",
            "column": self.column,
            "tabName": "fulltext",
            "plate": "",
            "stock": self.stock,
            "searchkey": "",
            "secid": "",
            "category": self.category,
            "trade": "",
            "seDate": f"{date}~{date}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }

        response = requests.post(
            url=self.QUERY_URL, headers=self.DEFAULT_HEADERS, data=payload
        )

        if response.status_code == 200:
            # 优先尝试使用response.json()，它会自动处理解压
            try:
                data = response.json()
            except json.JSONDecodeError:
                # 如果json()失败，尝试手动解压
                try:
                    import gzip
                    decompressed = gzip.decompress(response.content)
                    data = json.loads(decompressed.decode('utf-8'))
                except:
                    # 最后尝试brotli
                    try:
                        import brotli
                        decompressed = brotli.decompress(response.content)
                        data = json.loads(decompressed.decode('utf-8'))
                    except:
                        print("警告: 无法解析API响应")
                        return None
            
            total_record = data["totalRecordNum"]
            total_announcement = data["totalAnnouncement"]
            total_page = data["totalpages"]
            print(f"total records: {total_record}")
            print(f"total announcements: {total_announcement}")
            print(f"total pages: {total_page}")
            return total_record
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    def query_all(self, start_date, end_date, total_page, max_save_cnt=100, max_fail=5):
        """
        Download all announcements within the specified date range

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            total_page (int): Total number of pages to process
            max_save_cnt (int): Maximum number of files to save (default: 100)
            max_fail (int): Maximum number of allowed failures (default: 5)
        """
        # payload
        total_save_cnt = 0
        total_fail_cnt = 0
        # for i in range(1, 2):
        for i in range(1, total_page + 1):
            if total_fail_cnt >= max_fail:
                print("program has failed to much")
                break
            if total_save_cnt >= max_save_cnt:
                print(f"program have save enough files: {total_save_cnt} files")
            time.sleep(random.randint(1, 2))
            payload = {
                "pageNum": f"{i}",
                "pageSize": "30",
                "column": self.column,
                "tabName": "fulltext",
                "plate": self.plate,  # "",
                "stock": self.stock,
                "searchkey": self.searchKey,  # "",
                "secid": "",
                "secid": "",
                "category": self.category,
                "trade": "",
                "seDate": f"{start_date}~{end_date}",
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true",
            }

            response = requests.post(
                url=self.QUERY_URL, headers=self.DEFAULT_HEADERS, data=payload
            )

            if response.status_code == 200:
                # 优先尝试使用response.json()，它会自动处理解压
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    # 如果json()失败，尝试手动解压
                    try:
                        import gzip
                        decompressed = gzip.decompress(response.content)
                        data = json.loads(decompressed.decode('utf-8'))
                    except:
                        # 最后尝试brotli
                        try:
                            import brotli
                            decompressed = brotli.decompress(response.content)
                            data = json.loads(decompressed.decode('utf-8'))
                        except:
                            print(f"警告: 第{i}页无法解析API响应")
                            total_fail_cnt += 1
                            continue
                
                success, page_save_cnt = self.save_page(
                    data,
                    download_dir=self.download_path,
                )
                total_save_cnt += page_save_cnt
                if success == False:
                    total_fail_cnt += 1
                print(f"page {i} have download {page_save_cnt} files")

        print(f"total download files cnt: {total_save_cnt}")

    def query(self, start_date, end_date):
        """
        Query and download announcements within the specified date range

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """
        total_page = self.query_get(start_date, end_date)
        if total_page > 0:
            self.query_all(start_date, end_date, total_page)
        else:
            print("no data has found")

    def save_file(
        self,
        url,
        download_dir="outputs/announcements",
        max_attempt=1,
    ):
        """
        Download a single announcement file

        Args:
            url (str): URL of the file to download
            download_dir (str): Directory to save the file (default: "outputs/announcements")
            max_attempt (int): Maximum number of download attempts (default: 1)
        """
        dc = None
        download_status = False
        try:
            dc = DriverController(download_dir=download_dir)
            if not dc.driver:
                dc.start_browser()
            dc.driver.get(url)

            save_dir = download_dir

            for attempt in range(max_attempt):
                try:
                    # Record the status of PDF files before download
                    original_pdf_files = set(
                        f
                        for f in os.listdir(save_dir)
                        if f.endswith(".pdf")
                        and os.path.isfile(os.path.join(save_dir, f))
                    )

                    # download click
                    time.sleep(0.5)
                    download_link = dc._wait_and_highlight(
                        By.XPATH, "//button[contains(.,'公告下载')]"
                    )
                    dc._reliable_click(download_link)
                    dc.logger.info("file start downloading ...")

                    # Monitor download progress - only check PDF files
                    for wait_time in range(60):  # Max wait 30 seconds
                        time.sleep(0.5)

                        # Get current PDF files
                        current_pdf_files = set(
                            f
                            for f in os.listdir(save_dir)
                            if f.endswith(".pdf")
                            and os.path.isfile(os.path.join(save_dir, f))
                        )
                        new_pdf_files = current_pdf_files - original_pdf_files

                        # Check if new PDF files have been downloaded
                        if new_pdf_files:
                            # Get the newest PDF file
                            newest_pdf = max(
                                new_pdf_files,
                                key=lambda f: os.path.getmtime(
                                    os.path.join(save_dir, f)
                                ),
                            )
                            pdf_path = os.path.join(save_dir, newest_pdf)

                            # Check if file size is stable
                            try:
                                size1 = os.path.getsize(pdf_path)
                                time.sleep(1)
                                size2 = os.path.getsize(pdf_path)

                                if (
                                    size1 == size2 and size1 > 1024
                                ):  # File > 1KB and stable
                                    download_status = True
                                    dc.logger.info(f"文件下载成功: {newest_pdf}")
                                    break
                            except OSError:
                                # File might be writing, continue waiting
                                continue

                    if download_status:
                        break
                    else:
                        dc.logger.warning(
                            f"Download timeout or file incomplete, attempt {attempt+1}/{max_attempt}"
                        )

                except Exception as e:
                    dc.logger.error(
                        f"Download attempt {attempt+1} failed with error: {str(e)}"
                    )
                    dc._take_screenshot("download_error")

        except Exception as e:
            dc.logger.error(f"Download failed: {str(e)}")
        finally:
            try:
                dc.close()
            except Exception as e:
                dc.logger.error(f"Error closing browser: {str(e)}")

        return download_status

    def save_page(
        self,
        data,
        download_dir="outputs/announcements",
        max_fail=1,
    ):
        """
        Process and save announcements from a single page of results

        Args:
            data (dict): Announcement data from API response
            download_dir (str): Directory to save downloaded files
            max_fail (int): Maximum number of allowed failures per page

        Returns:
            tuple: (success_status, number_of_files_saved)
        """
        # Ensure download directory exists
        os.makedirs(download_dir, exist_ok=True)

        page_save_cnt = 0
        try:
            announcements = data.get("announcements")
            
            if not announcements:  # Handle null and empty list cases
                return False, page_save_cnt

            # Process valid data
            max_fail = int(max_fail) if str(max_fail).isdigit() else 1
            fail_cnt = 0
            # base url
            base_url = "https://www.cninfo.com.cn/new/disclosure/detail?"

            for announcement in announcements:
                if fail_cnt >= max_fail:
                    break

                announcement_id = announcement.get("announcementId")
                secName = announcement.get("secName")
                announcementTitle = announcement.get("announcementTitle")
                
                if not announcement or not announcement_id:
                    continue

                # Check for duplicates in database
                if self.db and self.db.record_exists(announcement_id):
                    continue

                # Create filename
                check_file_name = f"{secName}：{announcementTitle}.pdf"
                check_file_path = os.path.join(download_dir, check_file_name)

                # Check if file already exists
                file_exists = False
                for f in os.listdir(download_dir):
                    if f.startswith(f"{secName}：{announcementTitle}") and f.endswith(
                        ".pdf"
                    ):
                        file_exists = True
                        check_file_name = f
                        check_file_path = os.path.join(download_dir, check_file_name)
                        break

                # If file exists but no record in database, add record
                if file_exists:
                    if self.db:
                        record = {
                            "secCode": announcement.get("secCode"),
                            "secName": secName,
                            "announcementId": announcement_id,
                            "announcementTitle": announcementTitle,
                            "downloadUrl": f"{base_url}announcementId={announcement_id}",
                            "pageColumn": announcement.get("pageColumn"),
                            "announcementTime": (
                                announcement.get("adjunctUrl", "").split("/")[1]
                                if announcement.get("adjunctUrl")
                                else ""
                            ),
                        }
                        self.db.save_record(record)
                    page_save_cnt += 1
                    continue

                # Download file
                final_url = f"{base_url}announcementId={announcement_id}"
                success = self.save_file(final_url, download_dir)

                if success:
                    if self.db:
                        record = {
                            "secCode": announcement.get("secCode"),
                            "secName": secName,
                            "announcementId": announcement_id,
                            "announcementTitle": announcementTitle,
                            "downloadUrl": final_url,
                            "pageColumn": announcement.get("pageColumn"),
                            "announcementTime": (
                                announcement.get("adjunctUrl", "").split("/")[1]
                                if announcement.get("adjunctUrl")
                                else ""
                            ),
                        }
                        self.db.save_record(record)
                    page_save_cnt += 1
                else:
                    fail_cnt += 1

            return True, page_save_cnt

        except Exception as e:
            print(f"save failed: {e}")
            return False, page_save_cnt
