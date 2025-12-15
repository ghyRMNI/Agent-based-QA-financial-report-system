"""
统一数据收集主程序
Unified Data Collection Main Program

整合所有数据收集模块：
- 股票数据收集 (Stock Data)
- 新闻数据收集 (News Data)
- 公告爬取 (Announcements)
- PDF提取 (PDF Extraction)

使用方法：
    python main_pipeline.py
    然后按照提示输入公司信息
"""

import os
import sys
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import traceback

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from output_manager import UnifiedOutputManager


class UnifiedDataCollector:
    """统一数据收集器"""
    
    def __init__(self, company_name: str, stock_code: str, 
                 start_date: str, end_date: str, exchange_type: str = None):
        """
        初始化数据收集器
        
        Args:
            company_name: 公司名称
            stock_code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            exchange_type: 交易所类型 ('szse' 或 'hke')，如果已知则传入以避免重复查找
        """
        self.company_name = company_name
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.exchange_type = exchange_type  # 保存交易所类型
        
        # 创建输出管理器
        self.output_manager = UnifiedOutputManager(company_name)
        
        # 收集结果
        self.results = {
            'stock_data': {'success': False},
            'news_data': {'success': False},
            'announcements': {'success': False},
            'pdf_extracts': {'success': False}
        }
        
        print("\n" + "=" * 80)
        print("统一数据收集系统 | Unified Data Collection System")
        print("=" * 80)
        
        # 只显示用户实际输入的信息
        # 如果用户输入的是股票代码，company_name会是股票代码（占位符），stock_code会是实际代码
        # 如果用户输入的是公司名，company_name是公司名，stock_code是None
        if stock_code and company_name == stock_code:
            # 用户输入的是股票代码
            print(f"股票代码: {stock_code}")
        elif company_name and not stock_code:
            # 用户输入的是公司名称
            print(f"公司名称: {company_name}")
        elif stock_code and company_name and company_name != stock_code:
            # 两者都有且不同，都显示
            print(f"公司名称: {company_name}")
            print(f"股票代码: {stock_code}")
        else:
            # 默认显示公司名称（如果有）
            if company_name:
                print(f"公司名称: {company_name}")
        
        print(f"日期范围: {start_date} 至 {end_date}")
        print(f"输出目录: {self.output_manager.get_root_dir()}")
        print("=" * 80 + "\n")
    
    def check_existing_data(self) -> dict:
        """
        检查是否已存在当前公司的数据
        
        Returns:
            dict: 包含检查结果的字典
                - exists: 是否存在数据目录
                - has_news: 是否有新闻数据
                - has_announcements: 是否有公告数据
                - has_pdfs: 是否有PDF文件
        """
        output_root = self.output_manager.get_root_dir()
        
        result = {
            'exists': False,
            'has_news': False,
            'has_announcements': False,
            'has_pdfs': False
        }
        
        # 检查输出目录是否存在
        if not os.path.exists(output_root):
            return result
        
        result['exists'] = True
        
        # 检查新闻数据
        news_dir = self.output_manager.get_subdir('news_data', create_if_missing=False)
        if os.path.exists(news_dir):
            news_files = [f for f in os.listdir(news_dir) if f.endswith('.csv')]
            result['has_news'] = len(news_files) > 0
        
        # 检查公告数据
        announcements_dir = self.output_manager.get_subdir('announcements', create_if_missing=False)
        if os.path.exists(announcements_dir):
            pdf_files = [f for f in os.listdir(announcements_dir) if f.endswith('.pdf')]
            result['has_announcements'] = len(pdf_files) > 0
            result['has_pdfs'] = len(pdf_files) > 0
        
        return result
    
    def collect_stock_data(self) -> bool:
        """收集股票数据和新闻数据"""
        print("\n" + "-" * 80)
        print("步骤 1/4: 收集股票数据和新闻")
        print("-" * 80)
        
        try:
            # 导入股票数据收集模块
            sys.path.append(os.path.join(os.path.dirname(__file__), 'stock_data_news_collector'))
            from stock_data_news_collector.collectors.stock_data_collector import StockDataCollector
            from stock_data_news_collector.collectors.news_crawler import NewsCrawler
            
            # 如果没有股票代码，尝试从公司名查找
            actual_stock_code = self.stock_code
            if not actual_stock_code:
                # 导入查找函数
                sys.path.append(os.path.join(os.path.dirname(__file__), 'announcement_crawler'))
                from announcement_crawler.crawler_start import find_stock_info
                
                stock_info = find_stock_info(company_name=self.company_name)
                if stock_info:
                    actual_stock_code = stock_info['code']
                    print(f"根据公司名 '{self.company_name}' 找到股票代码: {actual_stock_code}")
                else:
                    print(f"错误: 无法找到公司 '{self.company_name}' 的股票代码")
                    return False
            
            # 创建股票数据收集器，传递交易所类型以避免重复查找和警告
            stock_collector = StockDataCollector(
                stock_code=actual_stock_code,
                exchange_type=self.exchange_type
            )
            
            # 更新实际的股票代码和公司名
            self.stock_code = actual_stock_code
            if not self.company_name or self.company_name == actual_stock_code:
                self.company_name = stock_collector.stock_name
            
            print(f"识别到公司: {stock_collector.stock_name} ({actual_stock_code})")
            
            # 收集股票数据
            print("正在收集股票数据...")
            stock_data, stock_report = stock_collector.collect_stock_data(
                start_date=self.start_date,
                end_date=self.end_date,
                years=2
            )
            
            # 移动股票数据到统一目录
            stock_output_dir = self.output_manager.get_subdir('stock_data')
            if os.path.exists(stock_collector.data_dir):
                # 复制所有文件到统一目录
                for item in os.listdir(stock_collector.data_dir):
                    src = os.path.join(stock_collector.data_dir, item)
                    dst = os.path.join(stock_output_dir, item)
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)
                    elif os.path.isdir(src):
                        if os.path.exists(dst):
                            shutil.rmtree(dst)
                        shutil.copytree(src, dst)
                
                # 删除原始输出目录（避免产生多个文件夹）
                try:
                    parent_dir = os.path.dirname(stock_collector.data_dir)
                    if os.path.exists(stock_collector.data_dir):
                        shutil.rmtree(stock_collector.data_dir)
                        print(f"已清理临时目录: {stock_collector.data_dir}")
                    
                    # 如果父目录为空，也删除
                    if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                        os.rmdir(parent_dir)
                        # 再往上一级
                        grandparent_dir = os.path.dirname(parent_dir)
                        if os.path.exists(grandparent_dir) and not os.listdir(grandparent_dir):
                            os.rmdir(grandparent_dir)
                except Exception as e:
                    print(f"清理临时目录时出错（可忽略）: {e}")
            
            # 统计股票数据
            trading_days = 0
            if 'Price Data' in stock_data and stock_data['Price Data'] is not None:
                trading_days = len(stock_data['Price Data'])
            
            financial_datasets = 0
            if 'Financial Data' in stock_data and stock_data['Financial Data']:
                financial_datasets = len(stock_data['Financial Data'])
            
            self.results['stock_data'] = {
                'success': True,
                'trading_days': trading_days,
                'financial_datasets': financial_datasets,
                'output_dir': stock_output_dir
            }
            
            print(f"股票数据收集成功!")
            print(f"   交易日数量: {trading_days}")
            print(f"   财务数据集: {financial_datasets}")
            
            # 收集新闻数据
            print("\n正在收集新闻数据...")
            print("提示: 正在启动Edge浏览器，首次启动可能需要较长时间...")
            
            news_crawler = None
            try:
                news_crawler = NewsCrawler(visible=False)
                print("浏览器启动成功，开始爬取新闻...")
                
                news_df = news_crawler.crawl_news_by_monthly_ranges(
                    company_name=stock_collector.stock_name,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    pages_per_month=1
                )
                
                if news_df is not None and not news_df.empty:
                    # 保存新闻数据到统一目录
                    news_output_dir = self.output_manager.get_subdir('news_data')
                    news_filename = os.path.join(
                        news_output_dir,
                        f"{self.stock_code}_news.csv"
                    )
                    news_df.to_csv(news_filename, index=False, encoding='utf-8-sig')
                    
                    self.results['news_data'] = {
                        'success': True,
                        'news_count': len(news_df),
                        'output_file': news_filename
                    }
                    
                    print(f"新闻数据收集成功!")
                    print(f"   新闻条数: {len(news_df)}")
                else:
                    print("未找到新闻数据")
                    self.results['news_data'] = {
                        'success': False,
                        'error': '未找到新闻数据'
                    }
            except KeyboardInterrupt:
                print("\n用户中断了新闻收集")
                self.results['news_data'] = {
                    'success': False,
                    'error': '用户中断'
                }
                raise
            except Exception as e:
                print(f"新闻收集失败: {e}")
                print("提示: 如果持续失败，可能是Edge浏览器或网络问题")
                traceback.print_exc()
                self.results['news_data'] = {
                    'success': False,
                    'error': str(e)
                }
            finally:
                if news_crawler:
                    try:
                        news_crawler.close()
                        print("浏览器已关闭")
                    except:
                        pass
            
            return True
            
        except Exception as e:
            print(f"股票数据收集失败: {e}")
            traceback.print_exc()
            self.results['stock_data'] = {
                'success': False,
                'error': str(e)
            }
            return False
    
    def collect_announcements(self) -> bool:
        """收集公告数据"""
        print("\n" + "-" * 80)
        print("步骤 2/4: 收集公告数据")
        print("-" * 80)
        
        try:
            # 导入公告爬虫模块
            sys.path.append(os.path.join(os.path.dirname(__file__), 'announcement_crawler'))
            from announcement_crawler.crawler_start import input_crawling
            
            # 设置下载目录
            download_dir = self.output_manager.get_subdir('announcements')
            
            print(f"正在下载年报公告...")
            output_dir = input_crawling(
                start_date=self.start_date,
                end_date=self.end_date,
                searchKey="年报",
                code=self.stock_code,
                download_dir=download_dir
            )
            
            # 统计下载的公告数量
            announcement_count = 0
            if os.path.exists(output_dir):
                announcement_count = len([f for f in os.listdir(output_dir) 
                                        if f.endswith('.pdf')])
            
            # 根据实际下载的文件数量判断是否成功
            if announcement_count > 0:
                self.results['announcements'] = {
                    'success': True,
                    'announcement_count': announcement_count,
                    'output_dir': output_dir
                }
                print(f"公告收集成功!")
                print(f"   公告数量: {announcement_count}")
                return True
            else:
                # 没有下载到文件，视为失败
                self.results['announcements'] = {
                    'success': False,
                    'announcement_count': 0,
                    'output_dir': output_dir,
                    'error': '未找到或下载任何公告文件'
                }
                print(f"公告收集完成，但未找到数据")
                print(f"   公告数量: 0")
                print(f"\n可能的原因:")
                print(f"   1. 该股票在指定日期范围内没有年报公告")
                print(f"   2. API无法解析响应（可能是网络问题或API限制）")
                if self.exchange_type == 'hke':
                    print(f"   3. 注意: cninfo.com.cn 主要支持A股，港股数据可能不完整")
                return False
            
        except KeyboardInterrupt:
            print("\n用户中断了公告收集")
            self.results['announcements'] = {
                'success': False,
                'error': '用户中断'
            }
            raise
        except Exception as e:
            print(f"公告收集失败: {e}")
            print("\n可能的原因:")
            print("1. 该公司在指定年份没有年报")
            print("2. API暂时不可用或限流")
            print("3. 网络连接问题")
            traceback.print_exc()
            self.results['announcements'] = {
                'success': False,
                'error': str(e)
            }
            return False
    
    def extract_pdfs(self) -> bool:
        """提取PDF内容"""
        print("\n" + "-" * 80)
        print("步骤 3/4: 提取PDF内容")
        print("-" * 80)
        
        try:
            # 检查是否有PDF文件需要提取
            announcement_dir = self.output_manager.get_subdir('announcements')
            pdf_files = []
            
            if os.path.exists(announcement_dir):
                for root, dirs, files in os.walk(announcement_dir):
                    for file in files:
                        if file.endswith('.pdf'):
                            pdf_files.append(os.path.join(root, file))
            
            if not pdf_files:
                print("未找到PDF文件，跳过提取步骤")
                self.results['pdf_extracts'] = {
                    'success': False,
                    'error': '未找到PDF文件'
                }
                return False
            
            print(f"找到 {len(pdf_files)} 个PDF文件")
            
            # 导入PDF提取模块
            pdf_extractor_path = os.path.join(
                os.path.dirname(__file__), 
                'pdf-extractor-cli-main'
            )
            sys.path.append(pdf_extractor_path)
            
            # 抑制所有PDF处理相关的警告和错误输出
            import warnings
            import logging
            import io
            warnings.filterwarnings('ignore')  # 抑制所有warnings
            
            # 抑制pdfplumber和fitz的错误输出（重定向stderr）
            old_stderr = sys.stderr
            sys.stderr = io.StringIO()  # 临时重定向stderr
            
            from pdf_extractor.text_extractor import TextExtractor  # type: ignore
            from pdf_extractor.hk_table_extractor import HKTableExtractor  # type: ignore
            import logging
            
            logger = logging.getLogger(__name__)
            text_extractor = TextExtractor(logger)
            table_extractor = HKTableExtractor(logger)
            
            # 为每个PDF创建提取目录
            pdf_output_dir = self.output_manager.get_subdir('pdf_extracts')
            processed_count = 0
            
            for pdf_file in pdf_files:
                try:
                    pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
                    
                    print(f"   处理: {pdf_name}...")
                    
                    # 提取文本和表格，直接传入pdf_output_dir
                    # extract_text和extract_tables内部会通过get_pdf_output_dirs创建子目录
                    try:
                        text_extractor.extract_text(pdf_file, None, pdf_output_dir)
                        print(f"      文本提取完成")
                    except Exception as e:
                        print(f"      文本提取失败: {e}")
                    
                    # 提取表格
                    try:
                        table_extractor.extract_tables(pdf_file, None, pdf_output_dir, 'csv')
                        print(f"      表格提取完成")
                    except Exception as e:
                        print(f"      表格提取失败: {e}")
                    
                    processed_count += 1
                    
                except Exception as e:
                    print(f"   处理 {pdf_name} 失败: {e}")
                    continue
            
            self.results['pdf_extracts'] = {
                'success': True,
                'files_processed': processed_count,
                'output_dir': pdf_output_dir
            }
            
            print(f"PDF提取完成!")
            print(f"   成功处理: {processed_count}/{len(pdf_files)} 个文件")
            
            # 恢复stderr
            sys.stderr = old_stderr
            
            # 清理csv文件夹（只保留csv_selected）
            print("\n清理临时CSV文件...")
            self._cleanup_csv_folders(pdf_output_dir)
            
            return True
            
        except Exception as e:
            # 恢复stderr（如果出错的话）
            try:
                sys.stderr = old_stderr
            except:
                pass
            
            print(f"PDF提取失败: {e}")
            traceback.print_exc()
            self.results['pdf_extracts'] = {
                'success': False,
                'error': str(e)
            }
            return False
    
    def _cleanup_csv_folders(self, pdf_extracts_dir: str):
        """
        清理csv文件夹，只保留csv_selected
        
        Args:
            pdf_extracts_dir: pdf_extracts目录路径
        """
        try:
            for root, dirs, files in os.walk(pdf_extracts_dir):
                # 查找csv文件夹（不包括csv_selected）
                if os.path.basename(root) == 'csv':
                    # 检查是否有对应的csv_selected
                    parent_dir = os.path.dirname(root)
                    csv_selected_dir = os.path.join(parent_dir, 'csv_selected')
                    
                    if os.path.exists(csv_selected_dir):
                        # 删除csv文件夹
                        import shutil
                        shutil.rmtree(root)
                        print(f"   已删除临时文件夹: {os.path.relpath(root, pdf_extracts_dir)}")
        except Exception as e:
            print(f"   清理CSV文件夹时出错（可忽略）: {e}")
    
    def generate_summary(self) -> str:
        """生成汇总报告"""
        print("\n" + "-" * 80)
        print("步骤 4/4: 生成汇总报告")
        print("-" * 80)
        
        summary_file = self.output_manager.create_summary_report(self.results)
        
        print(f"汇总报告已生成: {summary_file}")
        
        return summary_file
    
    def integrate_data(self):
        """整合股票数据和新闻数据"""
        print("\n" + "-" * 80)
        print("步骤 5/6: 整合股票和新闻数据")
        print("-" * 80)
        
        try:
            # 导入整合模块
            from integrate_stock_news_data import integrate_data
            
            output_dir = self.output_manager.get_root_dir()
            integrate_data(output_dir)
            
            print("股票和新闻数据整合完成!")
            return True
            
        except Exception as e:
            print(f"数据整合失败: {e}")
            traceback.print_exc()
            return False
    
    def extract_financial_tables_from_pdf(self):
        """从PDF提取的CSV中找出财务报表文件并清理，同时提取txt文件中的有用信息"""
        print("\n" + "-" * 80)
        print("步骤 6/7: 提取主要财务报表和txt有用信息")
        print("-" * 80)
        
        try:
            # 导入财务报表提取模块
            from extract_main_financial_statements import extract_main_statements
            from extract_text_data import extract_text_data_from_pdf_extracts
            
            output_dir = self.output_manager.get_root_dir()
            pdf_extracts_dir = os.path.join(output_dir, 'pdf_extracts')
            
            # 提取主要财务报表（根据关键词）
            print("\n提取主要财务报表...")
            main_statements = extract_main_statements(pdf_extracts_dir, output_dir)
            
            if main_statements and any(files for files in main_statements.values()):
                total_files = sum(len(files) for files in main_statements.values())
                print(f"\n主要财务报表提取完成! (共 {total_files} 个文件)")
            else:
                print("\n未找到主要财务报表")
            
            # 提取txt文件中的有用信息（关键词）
            print("\n开始提取txt文件中的关键词信息...")
            extract_text_data_from_pdf_extracts(pdf_extracts_dir, output_dir)
            
            return True
            
        except Exception as e:
            print(f"PDF财务报表提取失败: {e}")
            traceback.print_exc()
            return False
    
    def merge_financial_statements(self):
        """合并主要财务报表到一个CSV文件"""
        print("\n" + "-" * 80)
        print("步骤 7/7: 合并财务报表")
        print("-" * 80)
        
        try:
            # 导入合并模块
            from merge_financial_statements import merge_financial_statements
            
            output_dir = self.output_manager.get_root_dir()
            statements_dir = os.path.join(output_dir, 'main_financial_statements')
            
            # 检查是否存在财务报表目录
            if not os.path.exists(statements_dir):
                print("未找到财务报表目录，跳过合并步骤")
                return False
            
            # 合并财务报表
            print("\n开始合并财务报表...")
            output_file = merge_financial_statements(statements_dir, output_dir)
            
            if output_file:
                print(f"\n财务报表合并完成!")
                return True
            else:
                print("\n财务报表合并失败")
                return False
            
        except Exception as e:
            print(f"财务报表合并失败: {e}")
            traceback.print_exc()
            return False
    
    def run_all(self):
        """运行所有收集任务"""
        start_time = datetime.now()
        
        # 检查是否已存在数据
        existing_data = self.check_existing_data()
        skip_collection = False
        
        if existing_data['exists'] and existing_data['has_news'] and existing_data['has_announcements']:
            print("\n" + "=" * 80)
            print("检测到已存在数据")
            print("=" * 80)
            print(f"公司: {self.company_name}")
            print(f"输出目录: {self.output_manager.get_root_dir()}")
            print(f"  ✓ 新闻数据已存在")
            print(f"  ✓ 公告数据已存在")
            print("=" * 80)
            
            # 询问用户是否跳过数据收集
            user_choice = input("\n是否跳过数据收集，直接进行PDF分析？(y/n，默认y): ").strip().lower()
            
            if user_choice == '' or user_choice == 'y':
                skip_collection = True
                print("\n跳过数据收集步骤，直接进入PDF提取分析...\n")
                
                # 标记为成功（使用已有数据）
                self.results['stock_data']['success'] = True
                self.results['news_data']['success'] = True
                self.results['announcements']['success'] = True
                
                # 直接进入PDF提取
                self.extract_pdfs()
            else:
                print("\n将重新收集数据（已有数据将被覆盖）...\n")
        
        if not skip_collection:
            # 执行各个收集任务
            self.collect_stock_data()
            self.collect_announcements()
            self.extract_pdfs()
        
        summary_file = self.generate_summary()
        
        # 整合数据（如果至少有一个模块成功）
        has_success = any(result['success'] for result in self.results.values())
        if has_success:
            self.integrate_data()
            # 提取PDF财务报表（如果有PDF提取成功）
            if self.results['pdf_extracts']['success']:
                self.extract_financial_tables_from_pdf()
                # 合并财务报表
                self.merge_financial_statements()
        
        # 计算总耗时
        end_time = datetime.now()
        duration = end_time - start_time
        
        # 清理所有空的子目录
        self.output_manager.cleanup_empty_dirs()
        
        # 检查是否所有模块都失败
        all_failed = all(not result['success'] for result in self.results.values())
        
        if all_failed:
            # 所有模块都失败，删除空的输出目录
            output_root = self.output_manager.get_root_dir()
            if os.path.exists(output_root):
                try:
                    shutil.rmtree(output_root)
                    print("\n所有数据收集都失败，已清理空输出目录")
                except Exception as e:
                    print(f"\n清理空目录时出错: {e}")
        
        # 打印最终总结
        print("\n" + "=" * 80)
        print("数据收集完成!")
        print("=" * 80)
        print(f"总耗时: {duration}")
        
        if not all_failed:
            print(f"输出目录: {self.output_manager.get_root_dir()}")
            print(f"汇总报告: {summary_file}")
        else:
            print("注意: 所有数据收集都失败，未生成输出文件")
        
        print("\n收集结果:")
        print(f"  股票数据: {'成功' if self.results['stock_data']['success'] else '失败'}")
        print(f"  新闻数据: {'成功' if self.results['news_data']['success'] else '失败'}")
        print(f"  公告数据: {'成功' if self.results['announcements']['success'] else '失败'}")
        print(f"  PDF提取: {'成功' if self.results['pdf_extracts']['success'] else '失败'}")
        print("=" * 80 + "\n")

        return self.output_manager.get_root_dir()


def check_duplicate_stock_code(code: str) -> list:
    """
    检查股票代码是否在两个交易所都存在
    
    Args:
        code: 股票代码字符串（5位或以下）
        
    Returns:
        找到的股票信息列表，每个元素包含 (exchange_type, code, company_name)
    """
    import pandas as pd
    
    # 如果代码超过5位，不需要检查
    if len(code) > 5:
        return []
    
    # 补齐到5位和6位
    normalized_5 = code.zfill(5)
    normalized_6 = code.zfill(6)
    
    # 生成所有可能的代码变体（包括原格式）
    code_variants = [code]
    if code.isdigit():
        code_variants.append(normalized_5)
        code_variants.append(normalized_6)
    
    # 查找CSV文件
    possible_paths = [
        os.path.join(os.path.dirname(__file__), 'stock_data_news_collector', 'collectors', 'data', 'all_stocks.csv'),
        os.path.join(os.path.dirname(__file__), 'announcement_crawler', 'crawlers', 'stock_data', 'all_stocks.csv'),
        'stock_data_news_collector/collectors/data/all_stocks.csv',
        'announcement_crawler/crawlers/stock_data/all_stocks.csv'
    ]
    
    csv_path = None
    for path in possible_paths:
        if os.path.exists(path):
            csv_path = path
            break
    
    if csv_path is None:
        return []
    
    try:
        stock_df = pd.read_csv(csv_path)
        
        if 'code' not in stock_df.columns or 'column' not in stock_df.columns or 'zwjc' not in stock_df.columns:
            return []
        
        # 确保code列是字符串类型并去除空格
        stock_df['code'] = stock_df['code'].astype(str).str.strip()
        stock_df['column'] = stock_df['column'].astype(str).str.strip()
        stock_df['zwjc'] = stock_df['zwjc'].astype(str).str.strip()
        
        # 查找所有可能的代码变体
        matches = []
        
        # 遍历所有代码变体进行查找
        for variant in code_variants:
            match_rows = stock_df[stock_df['code'] == variant]
            if not match_rows.empty:
                for _, row in match_rows.iterrows():
                    # 根据交易所类型确定标准化后的代码
                    exchange = row['column']
                    if exchange == 'hke':
                        # 港股：使用5位代码
                        normalized_code = variant.zfill(5) if variant.isdigit() else variant
                    else:
                        # A股：使用6位代码
                        normalized_code = variant.zfill(6) if variant.isdigit() else variant
                    
                    matches.append({
                        'exchange': exchange,
                        'code': normalized_code,
                        'company_name': row['zwjc']
                    })
        
        # 去重（如果同一个交易所和代码出现多次，只保留一个）
        seen = set()
        unique_matches = []
        for match in matches:
            key = (match['exchange'], match['code'])
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)
        
        return unique_matches
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def interactive_input():
    """交互式输入界面"""
    print("\n" + "=" * 80)
    print("统一数据收集系统")
    print("   Unified Data Collection System")
    print("=" * 80 + "\n")
    
    # 输入公司信息
    print("请输入收集信息：\n")
    
    while True:
        company_input = input("请输入公司名称或股票代码 (例如: 平安银行, 000001): ").strip()
        if company_input:
            break
        print("公司信息不能为空，请重新输入！\n")
    
    # 初始化 exchange_type
    exchange_type = None
    
    # 尝试判断输入的是公司名还是股票代码
    # 如果全是数字，则认为是股票代码
    if company_input.isdigit():
        stock_code = company_input
        
        # 如果代码是5位或以下，检查是否在两个交易所都存在
        if len(stock_code) <= 5:
            matches = check_duplicate_stock_code(stock_code)
            
            if len(matches) > 1:
                # 有重复，让用户选择
                print(f"\n发现股票代码 '{stock_code}' 在多个交易所存在：")
                print("-" * 60)
                for i, match in enumerate(matches, 1):
                    exchange_name = "深交所" if match['exchange'] == 'szse' else "港交所"
                    print(f"{i}. {match['company_name']} ({match['code']}) - {exchange_name}")
                print("-" * 60)
                
                while True:
                    try:
                        choice = input(f"请选择 (1-{len(matches)}): ").strip()
                        choice_num = int(choice)
                        if 1 <= choice_num <= len(matches):
                            selected = matches[choice_num - 1]
                            stock_code = selected['code']
                            company_name = selected['company_name']
                            exchange_type = selected['exchange']  # 保存交易所类型
                            print(f"已选择: {company_name} ({stock_code}) - {exchange_type}")
                            # 将交易所类型保存到全局变量或传递给后续处理
                            break
                        else:
                            print(f"请输入 1 到 {len(matches)} 之间的数字！")
                    except ValueError:
                        print("请输入有效的数字！")
            elif len(matches) == 1:
                # 只有一个匹配，直接使用
                stock_code = matches[0]['code']
                company_name = matches[0]['company_name']
                exchange_type = matches[0]['exchange']  # 保存交易所类型
                print(f"找到: {company_name} ({stock_code})")
            else:
                # 没有找到，使用原代码
                company_name = company_input  # 默认使用股票代码，后续会被替换为实际公司名
                print(f"未在CSV中找到股票代码 '{stock_code}'，将使用原代码继续")
        else:
            company_name = company_input  # 默认使用股票代码，后续会被替换为实际公司名
    else:
        # 如果包含中文或其他字符，则认为是公司名
        company_name = company_input
        stock_code = None  # 将在后续查找
    
    # 输入年份
    print("\n请输入要收集的年份：")
    print("   提示：输入4位数年份，例如：2024")
    print("   支持多个年份，用逗号分隔，例如：2023,2024")
    
    while True:
        year_input = input("请输入年份: ").strip()
        if year_input:
            try:
                # 支持多个年份，用逗号分隔
                years = [y.strip() for y in year_input.split(',')]
                years_int = [int(y) for y in years]
                
                # 验证年份范围
                current_year = datetime.now().year
                for year in years_int:
                    if year < 2000 or year > current_year + 1:
                        print(f"年份 {year} 超出合理范围 (2000-{current_year+1})，请重新输入！\n")
                        raise ValueError
                
                # 如果只有一个年份，直接使用
                if len(years_int) == 1:
                    target_year = years_int[0]
                    # 计算该年份的日期范围
                    start_date = f"{target_year}-01-01"
                    end_date = f"{target_year}-12-31"
                else:
                    # 如果有多个年份，使用最小和最大年份
                    min_year = min(years_int)
                    max_year = max(years_int)
                    start_date = f"{min_year}-01-01"
                    end_date = f"{max_year}-12-31"
                    target_year = f"{min_year}-{max_year}"
                
                break
                
            except ValueError:
                print("年份格式错误，请输入有效的年份！\n")
        else:
            print("年份不能为空！\n")
    
    # 确认信息
    print("\n" + "-" * 80)
    print("确认收集信息：")
    print("-" * 80)
    print(f"  公司信息: {company_input}")
    if stock_code:
        print(f"  股票代码: {stock_code}")
    print(f"  收集年份: {target_year}")
    print(f"  日期范围: {start_date} 至 {end_date}")
    print("-" * 80)
    
    confirm = input("\n是否开始收集？(y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消收集")
        return None
    
    return {
        'company_name': company_name,
        'stock_code': stock_code,
        'start_date': start_date,
        'end_date': end_date,
        'year': target_year,
        'exchange_type': exchange_type  # 传递交易所类型
    }


# def input_checking(data: dict):
#     stock_code = data["parameters"]["stock_code"]
#     start_year = data["parameters"]["start_year"]
#     end_year = data["parameters"]["end_year"]


def data_collection():
    """主函数"""
    try:
        # 交互式输入
        input_data = interactive_input()
        
        if input_data is None:
            return
        
        # 创建收集器并运行
        # 如果没有提供股票代码，传入None，在collect_stock_data中会自动查找
        collector = UnifiedDataCollector(
            company_name=input_data['company_name'],
            stock_code=input_data.get('stock_code'),
            start_date=input_data['start_date'],
            end_date=input_data['end_date'],
            exchange_type=input_data.get('exchange_type')
        )
        
        root_path = collector.run_all()
        
    except KeyboardInterrupt:
        print("\n\n用户中断了收集过程")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    data_collection()

