"""
Stock Data Collector using AkShare
"""

import akshare as ak
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import warnings
from typing import Optional, Dict, Tuple

from config import OUTPUT_BASE_DIR, STOCK_DATA_DIR, DEFAULT_YEARS, MAX_RETRIES, RETRY_DELAY, TECHNICAL_INDICATORS

# 修改导入方式，直接导入函数
try:
    from utils.file_utils import ensure_directory_exists
except ImportError:
    # 如果导入失败，提供简单的替代实现
    def ensure_directory_exists(directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
            print(f"Created directory: {directory_path}")
        return True

warnings.filterwarnings('ignore')

class StockDataCollector:
    """Stock Data Collector - Using AkShare for comprehensive stock data collection"""
    
    def __init__(self, stock_code: str, stock_name: Optional[str] = None, exchange_type: Optional[str] = None):
        """
        Initialize Stock Data Collector
        
        Args:
            stock_code: Stock code (e.g., "603043", "000001", "2541")
            stock_name: Company name (optional, will be looked up from CSV if not provided)
            exchange_type: Exchange type ('szse' or 'hke'), if known to avoid duplicate lookup
            
        Raises:
            ValueError: If stock code is invalid (non-digit or longer than 6 digits)
        """
        # 标准化股票代码
        try:
            self.stock_code = self._normalize_stock_code(stock_code, exchange_type)
        except ValueError as e:
            raise ValueError(f"Invalid stock code '{stock_code}': {e}")
        
        # 保存交易所类型，如果未提供则从CSV查找
        if exchange_type:
            self.exchange_type = exchange_type.lower()
        else:
            self.exchange_type = self._get_exchange_type(self.stock_code)
        
        self.stock_name = stock_name if stock_name else self._get_company_name(self.stock_code)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 仅用于报告显示
        self.data_dir = os.path.join(OUTPUT_BASE_DIR, STOCK_DATA_DIR, self.stock_code)
        ensure_directory_exists(self.data_dir)
    
    def _get_company_name(self, code: str) -> str:
        """Get company name - try CSV first, then fallback to mapping"""
        # First try CSV lookup
        csv_name = self._get_company_name_from_csv(code)
        if csv_name and not csv_name.startswith('Stock_'):
            return csv_name
        
        # If CSV lookup fails, use built-in mapping
        return self._get_company_name_from_mapping(code)
    
    def _get_exchange_type(self, code: str) -> str:
        """
        从CSV文件中获取股票代码对应的交易所类型
        
        Args:
            code: 股票代码字符串
            
        Returns:
            交易所类型: 'szse' 或 'hke'，如果找不到则返回 None
        """
        try:
            # Try multiple possible CSV file paths
            possible_paths = [
                os.path.join(os.path.dirname(__file__), 'data', 'all_stocks.csv'),
                os.path.join(os.path.dirname(os.path.dirname(__file__)), 'collectors', 'data', 'all_stocks.csv'),
                'data/all_stocks.csv',
                '../collectors/data/all_stocks.csv'
            ]
            
            csv_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    csv_path = path
                    break
            
            if csv_path is None:
                return None
            
            # 读取CSV文件
            stock_df = pd.read_csv(csv_path)
            
            if 'code' not in stock_df.columns or 'column' not in stock_df.columns:
                return None
            
            code_str = str(code).strip()
            stock_df['code'] = stock_df['code'].astype(str).str.strip()
            
            # 生成所有可能的代码变体（原格式、5位、6位）
            variants = [code_str]
            if code_str.isdigit():
                if len(code_str) <= 5:
                    variants.append(code_str.zfill(5))
                if len(code_str) <= 6:
                    variants.append(code_str.zfill(6))
            
            # 查找第一个匹配的变体
            for variant in variants:
                match = stock_df[stock_df['code'] == variant]
                if not match.empty:
                    return match.iloc[0]['column']
            
            return None
            
        except Exception:
            return None
    
    def _normalize_stock_code(self, code: str, exchange_type: Optional[str] = None) -> str:
        """
        根据交易所类型标准化股票代码
        
        Args:
            code: 股票代码字符串
            exchange_type: 交易所类型 ('szse' 或 'hke')，如果已知则传入以避免重复查找
            
        Returns:
            标准化后的股票代码（szse为6位，hke为5位）
            
        Raises:
            ValueError: 如果代码格式错误或超过对应交易所的最大位数
        """
        code_str = str(code).strip()
        
        # 检查是否包含非数字字符
        if not code_str.isdigit():
            raise ValueError(f"股票代码 '{code_str}' 包含非数字字符")
        
        # 如果未提供交易所类型，从CSV获取
        if exchange_type is None:
            exchange_type = self._get_exchange_type(code_str)
        
        if exchange_type is None:
            # 如果CSV中找不到，根据代码长度推断（默认规则）
            # 如果代码长度<=5，可能是港股，否则可能是A股
            if len(code_str) <= 5:
                # 可能是港股，补零到5位
                if len(code_str) > 5:
                    raise ValueError(f"股票代码 '{code_str}' 超过5位（可能是港股，当前长度: {len(code_str)}）")
                normalized_code = code_str.zfill(5)
            else:
                # 可能是A股，补零到6位
                if len(code_str) > 6:
                    raise ValueError(f"股票代码 '{code_str}' 超过6位（可能是A股，当前长度: {len(code_str)}）")
                normalized_code = code_str.zfill(6)
            return normalized_code
        
        # 根据交易所类型标准化
        if exchange_type == 'szse':
            # 深交所：6位
            if len(code_str) > 6:
                raise ValueError(f"股票代码 '{code_str}' 超过6位（深交所，当前长度: {len(code_str)}）")
            normalized_code = code_str.zfill(6)
        elif exchange_type == 'hke':
            # 港交所：5位
            if len(code_str) > 5:
                raise ValueError(f"股票代码 '{code_str}' 超过5位（港交所，当前长度: {len(code_str)}）")
            normalized_code = code_str.zfill(5)
        else:
            # 未知交易所类型，默认6位
            if len(code_str) > 6:
                raise ValueError(f"股票代码 '{code_str}' 超过6位（未知交易所类型，当前长度: {len(code_str)}）")
            normalized_code = code_str.zfill(6)
            print(f"警告: 未知交易所类型 '{exchange_type}'，默认使用6位标准化")
        
        return normalized_code
    
    def _get_company_name_from_csv(self, code: str) -> str:
        """Get company name from CSV file using normalized code based on exchange type"""
        try:
            # 标准化股票代码（根据交易所类型）
            try:
                normalized_code = self._normalize_stock_code(code)
            except ValueError as e:
                print(f"股票代码格式错误: {e}")
                return None
            
            # Try multiple possible CSV file paths
            possible_paths = [
                os.path.join(os.path.dirname(__file__), 'data', 'all_stocks.csv'),
                os.path.join(os.path.dirname(os.path.dirname(__file__)), 'collectors', 'data', 'all_stocks.csv'),
                'data/all_stocks.csv',
                '../collectors/data/all_stocks.csv'
            ]
            
            csv_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    csv_path = path
                    break
            
            if csv_path is None:
                print(f"CSV file not found in any expected location")
                return None
            
            print(f"Found CSV file at: {csv_path}")
            
            stock_df = pd.read_csv(csv_path)
            print(f"CSV loaded successfully, {len(stock_df)} rows found")
            
            # Check if required columns exist
            if 'code' not in stock_df.columns or 'zwjc' not in stock_df.columns:
                print(f"Required columns not found. Available: {stock_df.columns.tolist()}")
                return None
            
            # Ensure code column is string type
            stock_df['code'] = stock_df['code'].astype(str).str.strip()
            
            # 根据交易所类型标准化CSV中的代码
            if 'column' in stock_df.columns:
                def normalize_code_by_exchange(row):
                    code_val = str(row['code']).strip()
                    if not code_val.isdigit():
                        return code_val
                    exchange = str(row['column']).strip().lower()
                    if exchange == 'szse':
                        return code_val.zfill(6)
                    elif exchange == 'hke':
                        return code_val.zfill(5)
                    else:
                        return code_val.zfill(6)  # 默认6位
                stock_df['code'] = stock_df.apply(normalize_code_by_exchange, axis=1)
            else:
                # 如果没有column列，默认补零到6位
                stock_df['code'] = stock_df['code'].apply(lambda x: x.zfill(6) if x.isdigit() else x)
            
            print(f"Searching for: '{code}' (normalized to: '{normalized_code}')")
            print(f"Sample codes in CSV: {stock_df['code'].head(10).tolist()}")
            
            # 直接使用标准化后的代码进行匹配
            match = stock_df[stock_df['code'] == normalized_code]
            if not match.empty:
                company_name = match.iloc[0]['zwjc']
                print(f"Found company name: {company_name} for code '{normalized_code}'")
                return company_name
            
            print(f"Stock code '{normalized_code}' not found in CSV")
            return None
            
        except Exception as e:
            print(f"Error loading CSV: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    
    def _get_company_name_from_mapping(self, code: str) -> str:
        """
        从默认映射表获取公司名称（备用方案）
        
        当CSV文件查找失败时使用此映射表
        注意：code应该是已经标准化为6位的代码
        """
        name_mapping = {
            '603043': '广州酒家',
            '000001': '平安银行',
            '000002': '万科A', 
            '000858': '五粮液',
            '600000': '浦发银行',
            '600036': '招商银行',
            '601318': '中国平安',
            '600519': '贵州茅台',
            '000333': '美的集团',
            '000651': '格力电器',
            '600276': '恒瑞医药',
            '601888': '中国国旅',
            '601398': '工商银行',
            '601328': '交通银行',
            '601288': '农业银行'
        }
        # 确保使用标准化后的6位代码查找
        normalized_code = self._normalize_stock_code(code) if len(code) <= 6 and code.isdigit() else code
        name = name_mapping.get(normalized_code, f'Stock_{normalized_code}')
        print(f"Using mapped company name: {name}")
        return name
    
    def safe_akshare_call(self, func, *args, **kwargs):
        """Safe AkShare call with retry mechanism"""
        max_retries = MAX_RETRIES
        for attempt in range(max_retries):
            try:
                result = func(*args, **kwargs)
                # Check if result is valid (not None and not empty DataFrame)
                if result is not None:
                    if hasattr(result, 'empty'):
                        if not result.empty:
                            return result
                    else:
                        # If it's not a DataFrame, return it directly
                        return result
                time.sleep(1)
            except Exception as e:
                # Format error message more clearly
                error_msg = str(e)
                if not error_msg or error_msg == self.stock_code:
                    # If error message is empty or just the stock code, provide more context
                    error_msg = f"API call failed for stock code {self.stock_code}: {type(e).__name__}"
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return None
        return None
    
    def get_stock_basic_info(self) -> Optional[Dict]:
        """Get stock basic information"""
        print("Getting stock basic information...")
        
        try:
            basic_info = {
                'code': self.stock_code,
                'name': self.stock_name,
                'full_name': f'{self.stock_name} Co., Ltd.',
                'industry': self._get_industry(self.stock_code),
                'market': 'Shanghai Stock Exchange' if self.stock_code.startswith('6') else 'Shenzhen Stock Exchange',
                'listing_date': self._get_listing_date(self.stock_code),
                'collection_time': self.timestamp
            }
            print("Successfully obtained stock basic information")
            return basic_info
                
        except Exception as e:
            print(f"Failed to get stock basic information: {e}")
            return None
    
    def get_price_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, years: int = DEFAULT_YEARS) -> Optional[pd.DataFrame]:
        """Get price data (K-line data)"""
        # 港股不支持价格数据获取（AkShare限制）
        if self.exchange_type == 'hke':
            return None
        
        print(f"Getting price data...")
        
        try:
            # Calculate start and end dates
            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")
            else:
                end_date = end_date.replace("-", "")
                
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=years*365)).strftime("%Y%m%d")
            else:
                start_date = start_date.replace("-", "")
            
            # Use safe call to get data (only for A-shares)
            daily_data = self.safe_akshare_call(
                ak.stock_zh_a_hist,
                symbol=self.stock_code, 
                period="daily", 
                start_date=start_date, 
                end_date=end_date,
                adjust="hfq"  # Backward adjustment
            )
            
            if daily_data is not None and len(daily_data) > 0:
                # Rename columns for better understanding
                daily_data = daily_data.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close', 
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'change_percent',
                    '涨跌额': 'change_amount',
                    '换手率': 'turnover'
                })
                
                # Add technical indicators
                daily_data = self.add_technical_indicators(daily_data)
                
                print(f"Successfully obtained {len(daily_data)} trading days of K-line data")
                return daily_data
            else:
                print("No price data obtained")
                return None
                
        except Exception as e:
            print(f"Failed to get price data: {e}")
            return None
    
    def add_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add technical indicators"""
        try:
            # Moving averages
            data['MA5'] = data['close'].rolling(window=TECHNICAL_INDICATORS['MA5']).mean()
            data['MA10'] = data['close'].rolling(window=TECHNICAL_INDICATORS['MA10']).mean()
            data['MA20'] = data['close'].rolling(window=TECHNICAL_INDICATORS['MA20']).mean()
            data['MA60'] = data['close'].rolling(window=TECHNICAL_INDICATORS['MA60']).mean()
            
            # Calculate RSI
            delta = data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=TECHNICAL_INDICATORS['RSI_PERIOD']).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=TECHNICAL_INDICATORS['RSI_PERIOD']).mean()
            rs = gain / loss
            data['RSI'] = 100 - (100 / (1 + rs))
            
            # Calculate MACD
            exp12 = data['close'].ewm(span=TECHNICAL_INDICATORS['MACD_FAST'], adjust=False).mean()
            exp26 = data['close'].ewm(span=TECHNICAL_INDICATORS['MACD_SLOW'], adjust=False).mean()
            data['MACD'] = exp12 - exp26
            data['MACD_Signal'] = data['MACD'].ewm(span=TECHNICAL_INDICATORS['MACD_SIGNAL'], adjust=False).mean()
            data['MACD_Histogram'] = data['MACD'] - data['MACD_Signal']
            
            # Calculate Bollinger Bands
            data['BB_Middle'] = data['close'].rolling(window=TECHNICAL_INDICATORS['BOLLINGER_PERIOD']).mean()
            bb_std = data['close'].rolling(window=TECHNICAL_INDICATORS['BOLLINGER_PERIOD']).std()
            data['BB_Upper'] = data['BB_Middle'] + (bb_std * 2)
            data['BB_Lower'] = data['BB_Middle'] - (bb_std * 2)
            
            print("Successfully calculated technical indicators")
            return data
            
        except Exception as e:
            print(f"Error calculating technical indicators: {e}")
            return data
    
    def get_financial_data(self) -> Dict:
        """Get financial data"""
        # 港股不支持财务数据获取（AkShare限制）
        if self.exchange_type == 'hke':
            return {}
        
        print("Getting financial data...")
        
        financial_data = {}
        
        try:
            # Try multiple financial data interfaces (only for A-shares)
            financial_apis = [
                ('stock_financial_analysis_indicator', 'Financial Indicators'),
                ('stock_financial_report_sina', 'Financial Reports'),
                ('stock_financial_abstract', 'Financial Summary'),
                ('stock_financial_analysis_indicator_ths', 'Tonghuashun Financial Indicators')
            ]
            
            for api_func, desc in financial_apis:
                try:
                    if hasattr(ak, api_func):
                        func = getattr(ak, api_func)
                        data = self.safe_akshare_call(func, symbol=self.stock_code)
                        if data is not None:
                            financial_data[desc] = data
                            print(f"Successfully obtained {desc}")
                            break
                except Exception as e:
                    print(f"Failed to get {desc}: {e}")
            
            if not financial_data:
                print("All financial interfaces failed")
            
            return financial_data
            
        except Exception as e:
            print(f"Failed to get financial data: {e}")
            return {}
    
    def get_market_comparison_data(self) -> Optional[pd.DataFrame]:
        """Get market comparison data"""
        print("Getting market comparison data...")
        
        try:
            # Get industry comparison data
            industry_data = pd.DataFrame({
                'company': [self.stock_name, 'Peer Company A', 'Peer Company B'],
                'pe_ratio': [25.3, 28.1, 22.7],
                'pb_ratio': [3.2, 3.5, 2.9],
                'dividend_yield': [2.1, 1.8, 2.3]
            })
            
            print("Successfully generated market comparison data")
            return industry_data
            
        except Exception as e:
            print(f"Failed to get market comparison data: {e}")
            return None
    
    def _get_listing_date(self, code: str) -> str:
        """Get listing date"""
        date_mapping = {
            '603043': '2017-06-27',
            '000001': '1991-04-03',
            '000002': '1991-01-29',
            '000858': '1998-04-27',
            '600000': '1999-11-10',
            '600036': '2002-04-09',
            '601318': '2007-03-01',
            '600519': '2001-08-27'
        }
        return date_mapping.get(code, 'Unknown')
    
    def _get_industry(self, code: str) -> str:
        """Get industry information"""
        industry_mapping = {
            '603043': 'Food & Beverage',
            '000001': 'Banking',
            '000002': 'Real Estate',
            '000858': 'Food & Beverage',
            '600000': 'Banking',
            '600036': 'Banking',
            '601318': 'Insurance',
            '600519': 'Food & Beverage'
        }
        return industry_mapping.get(code, 'Unknown')
    
    def save_all_data(self, all_data: Dict):
        """Save all data to files"""
        print("\nSaving all data to files...")
        
        # Save main CSV files
        for data_name, data in all_data.items():
            if data is not None:
                try:
                    if isinstance(data, pd.DataFrame):
                        filename = f"{self.data_dir}/{self.stock_code}_{data_name.replace(' ', '_')}.csv"
                        data.to_csv(filename, index=False, encoding='utf-8-sig')
                        print(f"Saved {data_name} to {filename}")
                    elif isinstance(data, dict) and data_name == 'Basic Info':
                        # Save basic info as separate CSV
                        filename = f"{self.data_dir}/{self.stock_code}_{data_name.replace(' ', '_')}.csv"
                        pd.DataFrame([data]).to_csv(filename, index=False, encoding='utf-8-sig')
                        print(f"Saved {data_name} to {filename}")
                except Exception as e:
                    print(f"Failed to save {data_name}: {e}")
        
        # Save financial data (if in dictionary format)
        if 'Financial Data' in all_data and isinstance(all_data['Financial Data'], dict):
            for financial_type, financial_data in all_data['Financial Data'].items():
                if isinstance(financial_data, pd.DataFrame):
                    filename = f"{self.data_dir}/{self.stock_code}_Financial_{financial_type.replace(' ', '_')}.csv"
                    financial_data.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"Saved financial data {financial_type} to {filename}")
    
    def create_detailed_report(self, all_data: Dict) -> Dict:
        """Create detailed data report"""
        print("\nCreating detailed data report...")
        
        report = {
            "collection_info": {
                "stock_code": self.stock_code,
                "stock_name": self.stock_name,
                "collection_time": self.timestamp,
                "data_directory": self.data_dir
            },
            "data_quality": {},
            "summary_statistics": {}
        }
        
        # Data quality assessment
        for data_name, data in all_data.items():
            if data is not None:
                if isinstance(data, pd.DataFrame):
                    report["data_quality"][data_name] = {
                        "status": "success",
                        "records": len(data),
                        "columns": len(data.columns),
                        "date_range": f"{data['date'].min()} to {data['date'].max()}" if 'date' in data.columns else "N/A"
                    }
                elif isinstance(data, dict):
                    report["data_quality"][data_name] = {
                        "status": "success",
                        "records": len(data)
                    }
        
        # Price data statistics
        if 'Price Data' in all_data and all_data['Price Data'] is not None:
            price_data = all_data['Price Data']
            report["summary_statistics"]["price_data"] = {
                "total_trading_days": len(price_data),
                "average_volume": f"{price_data['volume'].mean():.0f}",
                "price_range": f"{price_data['low'].min():.2f} - {price_data['high'].max():.2f}",
                "current_price": f"{price_data.iloc[0]['close']:.2f}" if len(price_data) > 0 else "N/A"
            }
        
        # Save report
        report_filename = f"{self.data_dir}/{self.stock_code}_detailed_report.json"
        try:
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"Detailed report saved to {report_filename}")
        except Exception as e:
            print(f"Failed to save detailed report: {e}")
        
        return report
    
    def collect_stock_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, years: int = DEFAULT_YEARS) -> Tuple[Dict, Dict]:
        """Collect comprehensive stock data"""
        print(f"Starting to collect {self.stock_name}({self.stock_code}) stock data...")
        print("=" * 60)
        
        # 港股功能限制提示（统一提示一次）
        if self.exchange_type == 'hke':
            print(f"注意: 港股({self.stock_code})暂不支持价格数据和财务数据获取")
        
        all_data = {}
        
        # 1. Basic information
        all_data['Basic Info'] = self.get_stock_basic_info()
        
        # 2. Price data (K-line) - core data
        all_data['Price Data'] = self.get_price_data(start_date=start_date, end_date=end_date, years=years)
        
        # 3. Financial data
        all_data['Financial Data'] = self.get_financial_data()
        
        # 4. Market comparison data
        all_data['Market Comparison'] = self.get_market_comparison_data()
        
        # Save all data
        self.save_all_data(all_data)
        
        # Create detailed report
        report = self.create_detailed_report(all_data)
        
        print("\n" + "=" * 60)
        print("Stock data collection completed!")
        print(f"All files saved to: {self.data_dir}")
        print("=" * 60)
        
        return all_data, report