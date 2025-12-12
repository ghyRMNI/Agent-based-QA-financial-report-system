"""
Configuration settings for Stock Data and News Collector
"""

# Path configurations
OUTPUT_BASE_DIR = "outputs"
STOCK_DATA_DIR = "stock_data"
NEWS_DATA_DIR = "news_data"
LOG_DIR = "logger"
ERROR_SCREENSHOTS_DIR = "error_screenshots"

# Collection settings
DEFAULT_YEARS = 2
DEFAULT_NEWS_PAGES = 1
MAX_RETRIES = 3
RETRY_DELAY = 2

# Browser settings
HEADLESS_MODE = True
BROWSER_WINDOW_SIZE = "1400,900"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

# Stock data settings
TECHNICAL_INDICATORS = {
    'MA5': 5,
    'MA10': 10,
    'MA20': 20,
    'MA60': 60,
    'RSI_PERIOD': 14,
    'MACD_FAST': 12,
    'MACD_SLOW': 26,
    'MACD_SIGNAL': 9,
    'BOLLINGER_PERIOD': 20
}