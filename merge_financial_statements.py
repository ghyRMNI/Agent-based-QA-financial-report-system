"""
合并主要财务报表到一个CSV文件
Merge Main Financial Statements into One CSV File
"""
import os
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import re


# 财务报表类型映射
STATEMENT_TYPE_MAPPING = {
    'balance_sheet': '资产负债表',
    'income_statement': '收益表', 
    'equity_statement': '权益变动表',
    'cash_flow': '现金流量表'
}


def extract_statement_info(filename: str) -> Dict[str, str]:
    """
    从文件名中提取报表信息
    
    Args:
        filename: CSV文件名
        
    Returns:
        包含报表类型、公司名、年报名称的字典
    """
    # 文件名格式: balance_sheet_长和_2022年年报_page131_table3.csv
    parts = filename.replace('.csv', '').split('_')
    
    info = {
        'statement_type_en': '',
        'statement_type_cn': '',
        'company_name': '',
        'report_year': '',
        'source_file': filename
    }
    
    # 提取报表类型
    if filename.startswith('balance_sheet'):
        info['statement_type_en'] = 'balance_sheet'
        info['statement_type_cn'] = '资产负债表'
        start_idx = 2
    elif filename.startswith('income_statement'):
        info['statement_type_en'] = 'income_statement'
        info['statement_type_cn'] = '收益表'
        start_idx = 2
    elif filename.startswith('equity_statement'):
        info['statement_type_en'] = 'equity_statement'
        info['statement_type_cn'] = '权益变动表'
        start_idx = 2
    elif filename.startswith('cash_flow'):
        info['statement_type_en'] = 'cash_flow'
        info['statement_type_cn'] = '现金流量表'
        start_idx = 2
    else:
        return info
    
    # 提取公司名和年份
    remaining_parts = parts[start_idx:]
    
    # 找到page的位置
    page_idx = None
    for i, part in enumerate(remaining_parts):
        if part.startswith('page'):
            page_idx = i
            break
    
    if page_idx is not None and page_idx > 0:
        # page之前的部分是公司名_年报
        name_parts = remaining_parts[:page_idx]
        if len(name_parts) >= 1:
            info['company_name'] = name_parts[0]
        
        # 提取年份（从年报名称中）
        if len(name_parts) >= 2:
            year_part = '_'.join(name_parts[1:])
            year_match = re.search(r'(\d{4})年', year_part)
            if year_match:
                info['report_year'] = year_match.group(1)
    
    return info


def is_valid_row(row: pd.Series) -> bool:
    """
    检查行是否有效（不是乱码或空行）
    
    Args:
        row: DataFrame的一行
        
    Returns:
        是否有效
    """
    # 转换为字符串并连接所有单元格
    row_str = ' '.join([str(val) for val in row if pd.notna(val)])
    
    # 如果行太短，可能是无效的
    if len(row_str.strip()) < 2:
        return False
    
    # 检查是否全是数字、逗号、空格（可能是格式错误）
    if re.match(r'^[\d,.\s\-()]+$', row_str.strip()):
        # 但如果包含有意义的数字格式，保留
        if re.search(r'\d{1,3}(,\d{3})+|\d+\.\d+', row_str):
            return True
        return False
    
    return True


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理DataFrame，移除乱码和无效行
    
    Args:
        df: 原始DataFrame
        
    Returns:
        清理后的DataFrame
    """
    # 移除完全空的行
    df = df.dropna(how='all')
    
    # 移除无效行
    valid_rows = []
    for idx, row in df.iterrows():
        if is_valid_row(row):
            valid_rows.append(idx)
    
    if valid_rows:
        df = df.loc[valid_rows]
    
    # 重置索引
    df = df.reset_index(drop=True)
    
    return df


def merge_financial_statements(statements_dir: str, output_dir: str = None) -> str:
    """
    合并主要财务报表到一个CSV文件
    
    Args:
        statements_dir: main_financial_statements目录路径
        output_dir: 输出目录（如果为None，则保存到statements_dir的父目录）
        
    Returns:
        输出文件路径
    """
    if not os.path.exists(statements_dir):
        print(f"错误: 目录不存在: {statements_dir}")
        return None
    
    # 确定输出目录
    if output_dir is None:
        output_dir = os.path.dirname(statements_dir)
    
    print("\n" + "=" * 80)
    print("合并主要财务报表")
    print("=" * 80)
    print(f"输入目录: {statements_dir}")
    print(f"输出目录: {output_dir}\n")
    
    # 获取所有CSV文件
    csv_files = [f for f in os.listdir(statements_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print("未找到CSV文件")
        return None
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    
    # 分类统计
    statement_counts = {
        '资产负债表': 0,
        '收益表': 0,
        '权益变动表': 0,
        '现金流量表': 0
    }
    
    # 按类型分组
    statements_by_type = {
        '资产负债表': [],
        '收益表': [],
        '权益变动表': [],
        '现金流量表': []
    }
    
    # 处理每个CSV文件
    for csv_file in sorted(csv_files):
        csv_path = os.path.join(statements_dir, csv_file)
        
        # 提取报表信息
        info = extract_statement_info(csv_file)
        
        if not info['statement_type_cn']:
            print(f"  跳过未识别的文件: {csv_file}")
            continue
        
        statement_counts[info['statement_type_cn']] += 1
        
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            # 清理DataFrame
            df = clean_dataframe(df)
            
            if len(df) == 0:
                print(f"  [跳过] {csv_file} - 清理后无有效数据")
                continue
            
            # 保存到分组
            statements_by_type[info['statement_type_cn']].append({
                'info': info,
                'data': df
            })
            
            print(f"  [成功] {info['statement_type_cn']} - {csv_file} ({len(df)} 行)")
            
        except Exception as e:
            print(f"  [失败] 读取失败: {csv_file} - {e}")
            continue
    
    # 创建合并后的内容
    print(f"\n正在合并报表...")
    
    all_rows = []
    
    # 按顺序输出：资产负债表 -> 收益表 -> 现金流量表 -> 权益变动表
    statement_order = ['资产负债表', '收益表', '现金流量表', '权益变动表']
    
    for statement_type in statement_order:
        statements = statements_by_type[statement_type]
        
        if not statements:
            continue
        
        for statement in statements:
            info = statement['info']
            df = statement['data']
            
            # 创建表头行
            company = info['company_name'] if info['company_name'] else '未知公司'
            year = info['report_year'] if info['report_year'] else '未知年份'
            
            header_text = f"【{statement_type}】{company} {year}年"
            
            # 表头行：第一列是标题，其余列为空
            header_row = [header_text] + [''] * (len(df.columns) - 1)
            all_rows.append(header_row)
            
            # 添加表格数据
            for _, row in df.iterrows():
                all_rows.append(row.tolist())
            
            # 添加空行分隔
            all_rows.append([''] * len(df.columns))
    
    if not all_rows:
        print("\n没有成功合并任何数据")
        return None
    
    # 找出最大列数
    max_cols = max(len(row) for row in all_rows)
    
    # 补齐所有行到相同列数
    for row in all_rows:
        while len(row) < max_cols:
            row.append('')
    
    # 创建DataFrame
    merged_df = pd.DataFrame(all_rows)
    
    # 保存合并后的文件
    output_filename = "financial_statements.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    merged_df.to_csv(output_path, index=False, header=False, encoding='utf-8-sig')
    
    # 打印统计信息
    print("\n" + "=" * 80)
    print("合并完成!")
    print("=" * 80)
    print(f"输出文件: {output_path}")
    print(f"\n报表统计:")
    for statement_type in statement_order:
        count = len(statements_by_type[statement_type])
        if count > 0:
            print(f"  {statement_type}: {count} 个")
    print(f"\n总行数: {len(merged_df)}")
    print(f"总列数: {len(merged_df.columns)}")
    print("=" * 80 + "\n")
    
    return output_path


def process_all_companies(unified_outputs_dir: str):
    """
    处理unified_outputs目录下所有公司的财务报表
    
    Args:
        unified_outputs_dir: unified_outputs目录路径
    """
    if not os.path.exists(unified_outputs_dir):
        print(f"错误: 目录不存在: {unified_outputs_dir}")
        return
    
    print("\n" + "=" * 80)
    print("批量处理所有公司的财务报表")
    print("=" * 80)
    print(f"扫描目录: {unified_outputs_dir}\n")
    
    # 查找所有包含main_financial_statements的公司目录
    processed_count = 0
    
    for company_dir in os.listdir(unified_outputs_dir):
        company_path = os.path.join(unified_outputs_dir, company_dir)
        
        if not os.path.isdir(company_path):
            continue
        
        statements_dir = os.path.join(company_path, 'main_financial_statements')
        
        if os.path.exists(statements_dir):
            print(f"\n处理公司: {company_dir}")
            print("-" * 60)
            
            output_path = merge_financial_statements(statements_dir, company_path)
            
            if output_path:
                processed_count += 1
    
    print("\n" + "=" * 80)
    print(f"批量处理完成! 共处理 {processed_count} 家公司")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法:")
        print("  1. 处理单个公司:")
        print("     python merge_financial_statements.py <main_financial_statements目录>")
        print("     例如: python merge_financial_statements.py unified_outputs/长和/main_financial_statements")
        print()
        print("  2. 批量处理所有公司:")
        print("     python merge_financial_statements.py --all <unified_outputs目录>")
        print("     例如: python merge_financial_statements.py --all unified_outputs")
        sys.exit(1)
    
    if sys.argv[1] == '--all':
        if len(sys.argv) < 3:
            print("错误: 请指定unified_outputs目录")
            print("例如: python merge_financial_statements.py --all unified_outputs")
            sys.exit(1)
        
        unified_outputs_dir = sys.argv[2]
        process_all_companies(unified_outputs_dir)
    else:
        statements_dir = sys.argv[1]
        merge_financial_statements(statements_dir)
