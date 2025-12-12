"""
统一输出目录管理模块
Unified Output Directory Manager

负责为所有数据收集任务创建和管理统一的输出目录结构
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class UnifiedOutputManager:
    """统一输出目录管理器"""
    
    def __init__(self, company_name: str, base_dir: str = "unified_outputs"):
        """
        初始化输出管理器
        
        Args:
            company_name: 公司名称
            base_dir: 基础输出目录
        """
        self.company_name = company_name
        self.base_dir = base_dir
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 仅用于报告显示
        
        # 创建主输出目录: {公司名}
        self.output_root = os.path.join(
            base_dir, 
            company_name
        )
        
        # 定义子目录结构
        self.subdirs = {
            'stock_data': os.path.join(self.output_root, 'stock_data'),
            'news_data': os.path.join(self.output_root, 'news_data'),
            'announcements': os.path.join(self.output_root, 'announcements'),
            'pdf_extracts': os.path.join(self.output_root, 'pdf_extracts'),
            'logs': os.path.join(self.output_root, 'logs'),
            'summary': os.path.join(self.output_root, 'summary')
        }
        
        # 创建所有目录
        self._create_directories()
        
    def _create_directories(self):
        """创建所有必需的目录"""
        os.makedirs(self.output_root, exist_ok=True)
        # 排除logs目录，只有在实际需要时才创建
        for subdir_name, subdir_path in self.subdirs.items():
            if subdir_name != 'logs':
                os.makedirs(subdir_path, exist_ok=True)
    
    def get_subdir(self, subdir_name: str, create_if_missing: bool = True) -> str:
        """
        获取子目录路径
        
        Args:
            subdir_name: 子目录名称 (stock_data, news_data, announcements, pdf_extracts, logs, summary)
            create_if_missing: 如果目录不存在是否创建（对于logs目录，只有在实际需要时才创建）
            
        Returns:
            子目录的完整路径
        """
        if subdir_name not in self.subdirs:
            raise ValueError(f"Unknown subdirectory: {subdir_name}")
        
        subdir_path = self.subdirs[subdir_name]
        
        # 对于logs目录，只有在明确需要时才创建
        if subdir_name == 'logs':
            if create_if_missing:
                # 只有在实际需要写入日志文件时才创建
                os.makedirs(subdir_path, exist_ok=True)
        else:
            # 确保其他目录存在
            if create_if_missing:
                os.makedirs(subdir_path, exist_ok=True)
        
        return subdir_path
    
    def get_logs_dir(self) -> str:
        """
        获取logs目录路径，并在需要时创建
        
        Returns:
            logs目录的完整路径
        """
        return self.get_subdir('logs', create_if_missing=True)
    
    def get_root_dir(self) -> str:
        """获取根目录路径"""
        return self.output_root
    
    def get_summary_file(self) -> str:
        """获取汇总报告文件路径"""
        return os.path.join(self.subdirs['summary'], 'collection_summary.txt')
    
    def create_summary_report(self, report_data: dict):
        """
        创建汇总报告
        
        Args:
            report_data: 包含各模块收集结果的字典
        """
        summary_file = self.get_summary_file()
        
        # 确保summary目录存在
        summary_dir = os.path.dirname(summary_file)
        os.makedirs(summary_dir, exist_ok=True)
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write(f"数据收集汇总报告 - {self.company_name}\n")
            f.write(f"Data Collection Summary Report\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"公司名称: {self.company_name}\n")
            f.write(f"收集时间: {self.timestamp}\n")
            f.write(f"输出目录: {self.output_root}\n\n")
            
            f.write("-" * 80 + "\n")
            f.write("收集结果 Collection Results:\n")
            f.write("-" * 80 + "\n\n")
            
            # 股票数据
            if 'stock_data' in report_data:
                stock_info = report_data['stock_data']
                f.write("股票数据 Stock Data:\n")
                f.write(f"   状态: {'成功' if stock_info.get('success') else '失败'}\n")
                if stock_info.get('success'):
                    f.write(f"   交易日数量: {stock_info.get('trading_days', 0)}\n")
                    f.write(f"   财务数据集: {stock_info.get('financial_datasets', 0)}\n")
                    f.write(f"   输出目录: {stock_info.get('output_dir', 'N/A')}\n")
                else:
                    f.write(f"   错误信息: {stock_info.get('error', 'Unknown error')}\n")
                f.write("\n")
            
            # 新闻数据
            if 'news_data' in report_data:
                news_info = report_data['news_data']
                f.write("新闻数据 News Data:\n")
                f.write(f"   状态: {'成功' if news_info.get('success') else '失败'}\n")
                if news_info.get('success'):
                    f.write(f"   新闻条数: {news_info.get('news_count', 0)}\n")
                    f.write(f"   输出文件: {news_info.get('output_file', 'N/A')}\n")
                else:
                    f.write(f"   错误信息: {news_info.get('error', 'Unknown error')}\n")
                f.write("\n")
            
            # 公告数据
            if 'announcements' in report_data:
                ann_info = report_data['announcements']
                f.write("公告数据 Announcements:\n")
                f.write(f"   状态: {'成功' if ann_info.get('success') else '失败'}\n")
                if ann_info.get('success'):
                    f.write(f"   公告数量: {ann_info.get('announcement_count', 0)}\n")
                    f.write(f"   输出目录: {ann_info.get('output_dir', 'N/A')}\n")
                else:
                    f.write(f"   错误信息: {ann_info.get('error', 'Unknown error')}\n")
                f.write("\n")
            
            # PDF提取数据
            if 'pdf_extracts' in report_data:
                pdf_info = report_data['pdf_extracts']
                f.write("PDF提取 PDF Extraction:\n")
                f.write(f"   状态: {'成功' if pdf_info.get('success') else '失败'}\n")
                if pdf_info.get('success'):
                    f.write(f"   处理文件数: {pdf_info.get('files_processed', 0)}\n")
                    f.write(f"   输出目录: {pdf_info.get('output_dir', 'N/A')}\n")
                else:
                    f.write(f"   错误信息: {pdf_info.get('error', 'Unknown error')}\n")
                f.write("\n")
            
            f.write("=" * 80 + "\n")
            f.write("报告生成完成\n")
            f.write("=" * 80 + "\n")
        
        return summary_file
    
    def cleanup_empty_dirs(self):
        """
        清理所有空的子目录
        如果目录存在但没有文件，则删除该目录
        递归清理，包括子目录中的空目录
        """
        # 从最深的目录开始清理
        subdirs_to_check = ['pdf_extracts', 'announcements', 'news_data', 'stock_data', 'logs', 'summary']
        
        for subdir_name in subdirs_to_check:
            if subdir_name in self.subdirs:
                subdir_path = self.subdirs[subdir_name]
                self._remove_if_empty(subdir_path)
    
    def _remove_if_empty(self, dir_path: str):
        """
        递归删除空目录
        
        Args:
            dir_path: 要检查的目录路径
        """
        if not os.path.exists(dir_path):
            return
        
        try:
            # 如果是目录，先递归处理子目录
            if os.path.isdir(dir_path):
                # 获取所有子项
                items = list(os.listdir(dir_path))
                
                # 递归处理子目录
                for item in items:
                    item_path = os.path.join(dir_path, item)
                    if os.path.isdir(item_path):
                        self._remove_if_empty(item_path)
                
                # 再次检查是否为空（子目录可能已被删除）
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
        except OSError:
            # 如果删除失败（可能目录不为空或正在使用），忽略
            pass
    
    def __str__(self):
        return f"UnifiedOutputManager(company={self.company_name}, root={self.output_root})"

