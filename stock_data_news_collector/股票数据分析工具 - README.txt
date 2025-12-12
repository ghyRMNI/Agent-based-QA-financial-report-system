股票数据分析工具 - README
项目简介
股票数据分析工具是一个集成了股票数据获取和新闻爬取功能的Python工具。通过简单的命令行接口，用户可以快速获取指定股票的历史价格数据、财务数据和相关新闻。

快速开始
环境要求
Python 3.7+

Chrome浏览器

ChromeDriver

配置环境：
pip install akshare pandas selenium

配置ChromeDriver

下载与您Chrome浏览器版本匹配的ChromeDriver

将ChromeDriver添加到系统PATH中，或放置在项目目录下

使用方法
命令行方式
基本用法：

bash
python main.py <股票代码> <开始日期> <结束日期>
示例：

bash
# 获取广州酒家2020-2024年的股票数据和新闻
python main.py 603043 2024-01-01 2024-12-31

# 获取平安银行数据，不包含新闻
python main.py 000001 2020-01-01 2024-12-31 --no-news

完整参数：

bash
python main.py <股票代码> <开始日期> <结束日期> [--no-news]
股票代码: 必填，如 "603043"、"000001"

开始日期: 必填，格式 "YYYY-MM-DD"

结束日期: 必填，格式 "YYYY-MM-DD"


--no-news: 不收集新闻数据

Python API方式
python
from main import StockAnalysisTool

# 创建分析工具
tool = StockAnalysisTool(
    stock_code="603043",
    start_date="2020-01-01",
    end_date="2024-12-31",
    output_dir="my_analysis_data"
)

# 收集所有数据
results = tool.collect_all_data(include_news=True)

print(f"分析完成! 数据保存在: {results['stock_data']['directory']}")
功能特性
股票数据获取
- 股票基本信息（名称、行业、上市日期等）

- 历史价格数据（开盘、收盘、最高、最低、成交量等）

- 技术指标计算（MA、RSI、MACD、布林带等）

- 财务数据获取（多种数据源尝试）

- 市场对比数据

新闻数据爬取
- 基于Selenium的新闻爬虫

- 问财新闻数据

- 自动重试机制

- 无头浏览器模式

数据输出
- CSV格式数据文件

- JSON格式详细报告

- 文本格式汇总报告

- 中文编码支持

项目结构
stock_data_news_collector
├── collectors/                             # Core collection modules
│   ├── __init__.py
│   ├── stock_data_collector.py            # AkShare stock data collection
│   ├── news_crawler.py                    # Selenium news crawler
│   └── data/
│       └── all_stocks.csv                 # Stock code mapping database
├── outputs/                               # Collected data storage
│   ├── stock_data/                        # Stock data files
│   └── news_data/                         # News data files
├── logger/
│   └── error_screenshots/                 # Error screenshots (auto-generated)
├── utils/                                 # Utility functions
│   ├── __init__.py
│   └── file_utils.py
├── config.py                              # Configuration settings
├── main.py                                # Main startup file
└── README.md                              # Documentation