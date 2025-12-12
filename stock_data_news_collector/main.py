"""
Main Startup File for Stock Data and News Collector
支持命令行参数: python main.py 603043 2024-01-01 2024-12-31
"""

import os
import sys
import argparse
from typing import Optional, Tuple, Dict
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from collectors.stock_data_collector import StockDataCollector
from collectors.news_crawler import NewsCrawler
from config import DEFAULT_YEARS, DEFAULT_NEWS_PAGES
from utils.file_utils import ensure_directory_exists

def input_collection(
    start_date: str,
    end_date: str,
    stock_code: str,
    include_news: bool = True,
    news_pages: int = DEFAULT_NEWS_PAGES,
    years: int = DEFAULT_YEARS
) -> Tuple[Dict, Dict, str]:
    """
    Main collection function
    
    Args:
        start_date: Start date in format "YYYY-MM-DD"
        end_date: End date in format "YYYY-MM-DD"
        stock_code: Stock code (e.g., "603043")
        include_news: Whether to include news crawling
        news_pages: Number of news pages to crawl per month
        years: Number of years for stock data (if dates not provided)
        
    Returns:
        Tuple of (stock_data, news_data, output_directory)
    """
    print("Stock Data and News Collection System")
    print("=" * 60)
    
    # Validate inputs
    if not start_date or not end_date:
        raise ValueError("Start date and end date are required")
    
    if not stock_code:
        raise ValueError("Stock code is required")
    
    # Create collectors
    stock_collector = StockDataCollector(stock_code=stock_code)
    news_crawler = None
    news_data = {}
    
    try:
        # Collect stock data
        print(f"\nCollecting stock data for {stock_collector.stock_name}({stock_code})...")
        stock_data, stock_report = stock_collector.collect_stock_data(
            start_date=start_date,
            end_date=end_date,
            years=years
        )
        
        # Collect news data if requested
        if include_news:
            print(f"\nCollecting news data for {stock_collector.stock_name}({stock_code})...")
            news_crawler = NewsCrawler(visible=False)
            news_df = news_crawler.crawl_news_by_monthly_ranges(
                company_name=stock_collector.stock_name,
                start_date=start_date,
                end_date=end_date,
                pages_per_month=news_pages
            )
            
            if news_df is not None and not news_df.empty:
                # Save news data
                news_filename = news_crawler.save_news_data(news_df, stock_code, stock_collector.timestamp)
                news_data = {
                    'data': news_df,
                    'filename': news_filename,
                    'records': len(news_df)
                }
                print(f"Successfully collected {len(news_df)} news items")
            else:
                print("No news data collected")
                news_data = {'data': None, 'filename': '', 'records': 0}
        
        output_dir = stock_collector.data_dir
        print(f"\nCollection completed!")
        print(f"Output directory: {output_dir}")
        
        return stock_data, news_data, output_dir
        
    except Exception as e:
        print(f"Collection failed: {e}")
        raise
    finally:
        # Clean up resources
        if news_crawler:
            news_crawler.close()

def main():
    """Main execution function with command line arguments"""
    parser = argparse.ArgumentParser(description='Stock Data and News Collection System')
    parser.add_argument('stock_code', help='Stock code (e.g., 603043)')
    parser.add_argument('start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--no-news', action='store_true', help='Skip news collection')
    parser.add_argument('--news-pages', type=int, default=DEFAULT_NEWS_PAGES, 
                       help=f'Number of news pages per month (default: {DEFAULT_NEWS_PAGES})')
    parser.add_argument('--output', '-o', help='Output directory')
    
    args = parser.parse_args()
    
    print("Stock Data and News Collection System")
    print("=" * 60)
    print(f"Stock Code: {args.stock_code}")
    print(f"Date Range: {args.start_date} to {args.end_date}")
    print(f"News Collection: {'Disabled' if args.no_news else 'Enabled'}")
    if not args.no_news:
        print(f"News Pages per Month: {args.news_pages}")
    print("=" * 60)
    
    try:
        # Start collection
        stock_data, news_data, output_dir = input_collection(
            start_date=args.start_date,
            end_date=args.end_date,
            stock_code=args.stock_code,
            include_news=not args.no_news,
            news_pages=args.news_pages
        )
        
        # Display summary
        print("\nCollection Summary:")
        print("-" * 40)
        
        if 'Price Data' in stock_data and stock_data['Price Data'] is not None:
            price_data = stock_data['Price Data']
            print(f"Price Data: {len(price_data)} trading days")
            if len(price_data) > 0:
                latest = price_data.iloc[0]
                print(f"   Latest: {latest['date']} - Close: {latest['close']:.2f}")
        
        if 'Financial Data' in stock_data and stock_data['Financial Data']:
            financial_count = len(stock_data['Financial Data'])
            print(f"Financial Data: {financial_count} datasets")
        
        if news_data and news_data.get('records', 0) > 0:
            print(f"News Data: {news_data['records']} items")
            if news_data.get('filename'):
                print(f"   Saved to: {news_data['filename']}")
        
        print(f"\nAll data saved to: {output_dir}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
        sys.exit(1)

if __name__ == "__main__":
    main()