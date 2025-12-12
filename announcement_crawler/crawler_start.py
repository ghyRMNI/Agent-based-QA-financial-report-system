import csv
import logging
import logging.config
import os
import sys
from typing import Optional

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers import cinifo_crawler


def load_all_stocks():
    """
    Load stock data from the all_stocks.csv file

    Returns:
        list: List of dictionaries containing stock information with keys:
            - column: Stock exchange column
            - code: Stock code
            - orgId: Organization ID
            - pinyin: Company name in Pinyin
            - zwjc: Company name in Chinese
    """
    csv_path = os.path.join(
        os.path.dirname(__file__), "crawlers", "stock_data", "all_stocks.csv"
    )
    stocks = []

    with open(
        csv_path, "r", encoding="utf-8-sig"
    ) as file:  # Use utf-8-sig to remove BOM
        reader = csv.DictReader(file)
        # print(f"CSVcolumns: {reader.fieldnames}")  # Debug information
        for row in reader:
            stocks.append(
                {
                    "column": row["column"],
                    "code": row["code"],
                    "orgId": row["orgId"],
                    "pinyin": row["pinyin"],
                    "zwjc": row["zwjc"],
                }
            )

    return stocks


# Load stock data at module initialization
all_stocks = load_all_stocks()


def find_stock_info(code: Optional[str] = None, company_name: Optional[str] = None):
    """
    Find stock information by code or company name in the all_stocks list

    Args:
        code: Stock code to search for
        company_name: Company name or Pinyin abbreviation to search for

    Returns:
        dict: Dictionary containing stock information if found, None otherwise
    """
    for stock in all_stocks:
        if code and stock["code"] == code:
            return stock
        if company_name and (
            stock["pinyin"] == company_name or stock["zwjc"] == company_name
        ):
            return stock
    return None


def start_crawling(
    start_date: str,
    end_date: str,
    stock_code: str,
    column: str,
    searchKey: str,
    company_name: str,
    download_dir: Optional[str] = None,
):
    """
    run cninfo announcement crawling for a specific company

    input:
        start_date: str,  # format: 'YYYY-MM-DD'
        end_date: str,  # format: 'YYYY-MM-DD'
        stock_code: str,
        column: str,
        searchKey: str,
        company_name: str, # used for setting download directory
        download_dir: Optional[str] = None,
    """
    # 不使用数据库（db_path=None），每次都重新下载
    announcementDownloader = cinifo_crawler.Cninfo(db_path=None)

    # Set company-specific download directory
    if download_dir is None:
        # 使用统一输出目录下的announcements子目录
        download_dir = os.path.join("outputs", "announcements", company_name)
    
    os.makedirs(download_dir, exist_ok=True)
    announcementDownloader.set_download_dir(download_dir)

    # Configure category for annual reports
    category = None
    if column == "szse":
        category = "category_ndbg_szsh"  # shenzhen & shanghai annual report
    
    announcementDownloader.edit_payload(
        stock_code=stock_code,
        column=column,
        searchKey=searchKey,
        category=category,
    )
    announcementDownloader.query(
        start_date=start_date,
        end_date=end_date,
    )

    output_file_dir = download_dir
    return output_file_dir


def input_crawling(
    start_date: str,
    end_date: str,
    searchKey: str,
    code: Optional[str] = None,
    company_name: Optional[str] = None,
    download_dir: Optional[str] = None,
):
    """
    Run cninfo announcement crawling for a specific company based on code or company name

    Args:
        start_date: Start date 'YYYY-MM-DD' (required)
        end_date: End date 'YYYY-MM-DD' (required)
        searchKey: Search keyword
        code: Stock code
        company_name: Company name or Pinyin abbreviation
        download_dir: Download directory
    """
    # Validate input parameters
    if not start_date or not end_date:
        raise ValueError("start_date and end_date must be provided")

    if not code and not company_name:
        raise ValueError("Either code or company_name must be provided")

    # Find stock information
    stock_info = find_stock_info(code, company_name)
    if not stock_info:
        raise ValueError(
            f"Stock information not found for: code={code}, company_name={company_name}"
        )

    # Extract column information from stock data
    column = stock_info["column"]

    # Construct stock_code in required format
    stock_code = f"{stock_info['code']},{stock_info['orgId']}"

    # Use Pinyin as company name for directory naming
    actual_company_name = company_name if company_name else stock_info["pinyin"]

    # Print crawling information
    print(
        f"Starting to crawl announcements for {stock_info['zwjc']}({stock_info['code']}) ..."
    )
    print(f"Exchange: {column}")
    print(f"stock_code: {stock_code}")
    print(f"company_name: {actual_company_name}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Search keyword: {searchKey}")

    # Execute the crawling process
    output_file_dir = start_crawling(
        start_date=start_date,
        end_date=end_date,
        stock_code=stock_code,
        column=column,
        searchKey=searchKey,
        company_name=actual_company_name,
        download_dir=download_dir,
    )

    return output_file_dir


if __name__ == "__main__":
    """
    Main execution block with example usage patterns
    """
    # Example usage 1: Using stock code (dates must be provided)
    input_crawling(
        start_date="2020-01-01", end_date="2025-09-28", searchKey="年报", code="00700"
    )

    # Example usage 2: Using company Pinyin name (dates must be provided)
    output_file_dir = input_crawling(
        start_date="2020-01-01",
        end_date="2025-09-28",
        searchKey="年报",
        company_name="浦发银行",
    )

    print(f"Announcement files have been downloaded to: {output_file_dir}")
