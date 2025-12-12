"""
根据关键词提取主要财务报表
Extract Main Financial Statements Based on Keywords

从csv_selected中筛选包含以下关键词的表格：
- 资产负债表 / 财务状况表
- 收益表 / 利润表 / 损益表
- 权益变动表
- 现金流量表
"""

import os
import pandas as pd
import shutil
import re
from pathlib import Path
from typing import List, Dict, Optional


# 定义主要财务报表的关键词（简繁体中文 + 英文）
MAIN_STATEMENT_KEYWORDS = {
    'balance_sheet': {
        'keywords': [
            # 简体中文
            '资产负债表', '财务状况表', '合并资产负债表', '综合财务状况表',
            # 繁体中文
            '資產負債表', '財務狀況表', '合併資產負債表', '綜合財務狀況表',
            # 英文
            'Balance Sheet', 'Statement of Financial Position',
            'Consolidated Balance Sheet'
        ],
        'name': '资产负债表'
    },
    'income_statement': {
        'keywords': [
            # 简体中文
            '收益表', '利润表', '损益表', '经营业绩', '综合收益表', '综合损益表', 
            '合并利润表', '合并经营业绩', '综合全面收益表',
            # 繁体中文
            '收益表', '利潤表', '損益表', '經營業績', '綜合收益表', '綜合損益表',
            '合併利潤表', '合併經營業績', '綜合全面收益表',
            # 英文
            'Income Statement', 'Profit and Loss', 'Statement of Operations',
            'Consolidated Income Statement', 'Comprehensive Income'
        ],
        'name': '收益表'
    },
    'equity_statement': {
        'keywords': [
            # 简体中文
            '权益变动表', '股东权益变动', '股东权益', '合并股东权益', '综合权益变动',
            # 繁体中文
            '權益變動表', '股東權益變動', '股東權益', '合併股東權益', '綜合權益變動',
            # 英文
            'Statement of Changes in Equity', 'Changes in Equity',
            'Consolidated Equity'
        ],
        'name': '权益变动表'
    },
    'cash_flow': {
        'keywords': [
            # 简体中文
            '现金流量表', '现金流', '合并现金流量表', '综合现金流量表',
            # 繁体中文
            '現金流量表', '現金流', '合併現金流量表', '綜合現金流量表',
            # 英文
            'Cash Flow', 'Statement of Cash Flows',
            'Consolidated Cash Flow'
        ],
        'name': '现金流量表'
    }
}


def read_csv_first_rows(filepath: str, nrows: int = 5) -> str:
    """
    读取CSV文件的前几行（可能包含表格标题）
    
    Args:
        filepath: CSV文件路径
        nrows: 读取的行数（默认5行）
        
    Returns:
        前几行内容（合并所有行和列）
    """
    try:
        df = pd.read_csv(filepath, nrows=nrows, header=None, encoding='utf-8-sig')
        if not df.empty:
            # 合并所有行的所有列
            all_text = ' '.join([
                ' '.join(row.astype(str).values) 
                for _, row in df.iterrows()
            ])
            return all_text
    except:
        pass
    return ''


def match_statement_type(filename: str, filepath: str) -> Optional[str]:
    """
    根据文件名和内容判断财务报表类型
    
    Args:
        filename: 文件名
        filepath: 文件路径
        
    Returns:
        报表类型键名，如果不匹配返回None
    """
    filename_lower = filename.lower()
    filepath_lower = filepath.lower()
    
    # 读取CSV前几行（可能包含表格标题，如"綜合收益表"）
    first_rows = read_csv_first_rows(filepath, nrows=5)
    first_rows_lower = first_rows.lower()
    
    # 检查每种报表类型
    for statement_type, config in MAIN_STATEMENT_KEYWORDS.items():
        keywords = config['keywords']
        
        # 检查关键词是否在文件名、路径或前几行中
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if (keyword_lower in filename_lower or 
                keyword_lower in filepath_lower or 
                keyword_lower in first_rows_lower):
                return statement_type
    
    return None


def find_tables_by_keywords(pdf_extracts_dir: str) -> Dict[str, List[str]]:
    """
    根据关键词查找主要财务报表CSV文件
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        
    Returns:
        按报表类型分组的文件路径字典
    """
    categorized_files = {key: [] for key in MAIN_STATEMENT_KEYWORDS.keys()}
    
    # 遍历csv_selected文件夹
    for root, dirs, files in os.walk(pdf_extracts_dir):
        # 只处理csv_selected目录
        if 'csv_selected' not in root.lower():
            continue
        
        for file in files:
            if not file.endswith('.csv'):
                continue
            
            filepath = os.path.join(root, file)
            
            # 判断报表类型
            statement_type = match_statement_type(file, filepath)
            
            if statement_type:
                categorized_files[statement_type].append(filepath)
    
    return categorized_files


def extract_main_statements(pdf_extracts_dir: str, output_dir: str) -> Dict[str, List[str]]:
    """
    根据关键词提取主要财务报表
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        output_dir: 输出目录
        
    Returns:
        按类别组织的文件路径字典
    """
    print("=" * 80)
    print("根据关键词提取主要财务报表")
    print("=" * 80)
    
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: pdf_extracts目录不存在: {pdf_extracts_dir}")
        return {}
    
    # 创建输出目录
    main_statements_dir = os.path.join(output_dir, 'main_financial_statements')
    os.makedirs(main_statements_dir, exist_ok=True)
    
    print(f"输出目录: {main_statements_dir}\n")
    print("查找关键词：")
    for statement_type, config in MAIN_STATEMENT_KEYWORDS.items():
        print(f"  - {config['name']}: {', '.join(config['keywords'][:3])}...")
    print()
    
    # 根据关键词查找文件
    categorized_files = find_tables_by_keywords(pdf_extracts_dir)
    
    results = {}
    total_found = 0
    
    for statement_type, src_files in categorized_files.items():
        statement_name = MAIN_STATEMENT_KEYWORDS[statement_type]['name']
        
        if not src_files:
            print(f"  [未找到] {statement_name}")
            continue
        
        print(f"  [找到] {statement_name}: {len(src_files)} 个文件")
        
        # 复制到输出目录
        category_files = []
        for src_file in src_files:
            filename = os.path.basename(src_file)
            # 添加类别前缀
            new_filename = f"{statement_type}_{filename}"
            dst_file = os.path.join(main_statements_dir, new_filename)
            
            try:
                shutil.copy2(src_file, dst_file)
                category_files.append(dst_file)
                print(f"      - {filename}")
                total_found += 1
            except Exception as e:
                print(f"      [ERROR] 复制失败: {filename}, {e}")
        
        results[statement_type] = category_files
    
    print("\n" + "=" * 80)
    print(f"提取完成！共找到 {total_found} 个主要财务报表")
    print(f"输出目录: {main_statements_dir}")
    print("=" * 80)
    
    return results




def main():
    """主函数"""
    import sys
    
    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    else:
        # 默认使用最新的输出目录
        base_dir = "unified_outputs"
        if os.path.exists(base_dir):
            dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
            if dirs:
                dirs.sort(key=lambda x: os.path.getmtime(os.path.join(base_dir, x)), reverse=True)
                output_dir = os.path.join(base_dir, dirs[0])
                print(f"使用最新的输出目录: {output_dir}")
            else:
                print("错误: 未找到输出目录")
                return
        else:
            print("错误: unified_outputs目录不存在")
            return
    
    pdf_extracts_dir = os.path.join(output_dir, 'pdf_extracts')
    
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: pdf_extracts目录不存在: {pdf_extracts_dir}")
        return
    
    # 自动提取主要财务报表
    extract_main_statements(pdf_extracts_dir, output_dir)


if __name__ == "__main__":
    main()

