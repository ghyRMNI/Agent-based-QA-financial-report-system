"""
清理和整理财务报表CSV文件，修复列对齐问题
"""
import os
import pandas as pd
import re
from pathlib import Path
from typing import List, Tuple, Optional


def detect_table_structure(df: pd.DataFrame) -> dict:
    """
    检测表格结构，识别哪些列是数据列，哪些是文本列
    
    Returns:
        dict: 包含表格结构信息
    """
    # 统计每列的非空值数量
    non_null_counts = df.notna().sum()
    
    # 识别可能的文本列（第一列通常是项目名称）
    text_columns = []
    data_columns = []
    
    # 第一列通常是文本列
    if len(df.columns) > 0:
        first_col = df.columns[0]
        if non_null_counts[first_col] > len(df) * 0.3:  # 至少30%有值
            text_columns.append(first_col)
    
    # 其他列根据内容判断
    for col in df.columns[1:]:
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue
        
        # 检查是否主要是数字（可能是数据列）
        numeric_count = 0
        for val in col_data:
            val_str = str(val).strip()
            # 移除常见的数字格式符号
            val_clean = val_str.replace(',', '').replace('(', '').replace(')', '').replace('–', '-').strip()
            if val_clean and (val_clean.replace('.', '').replace('-', '').isdigit() or val_clean == ''):
                numeric_count += 1
        
        if numeric_count > len(col_data) * 0.5:  # 超过50%是数字
            data_columns.append(col)
        else:
            text_columns.append(col)
    
    return {
        'text_columns': text_columns,
        'data_columns': data_columns,
        'total_columns': len(df.columns)
    }


def merge_multiline_text(df: pd.DataFrame, text_col_idx: int = 0) -> pd.DataFrame:
    """
    合并跨多行的文本（项目名称等）
    
    Args:
        df: DataFrame
        text_col_idx: 文本列的索引
        
    Returns:
        整理后的DataFrame
    """
    df = df.copy()
    text_col = df.columns[text_col_idx]
    
    # 创建新DataFrame存储结果
    result_rows = []
    current_text = ""
    current_row_data = None
    
    for idx, row in df.iterrows():
        # 获取文本列的值
        text_val = str(row[text_col]) if pd.notna(row[text_col]) else ""
        text_val = text_val.strip()
        
        # 检查其他列是否有数据
        other_cols = [col for col in df.columns if col != text_col]
        has_data = any(pd.notna(row[col]) and str(row[col]).strip() for col in other_cols)
        
        if has_data:
            # 如果有数据，说明这是一行完整的记录
            if current_text:
                # 合并之前的文本
                if current_row_data is not None:
                    current_row_data[text_col] = current_text.strip()
                    result_rows.append(current_row_data)
                current_text = ""
            
            # 保存当前行
            row_data = row.to_dict()
            if text_val:
                row_data[text_col] = text_val
            else:
                row_data[text_col] = current_text.strip() if current_text else ""
            result_rows.append(row_data)
            current_row_data = None
        else:
            # 如果没有数据，说明这是文本的延续
            if text_val:
                if current_text:
                    current_text += " " + text_val
                else:
                    current_text = text_val
            # 如果当前行有部分数据，保存它
            if current_row_data is None:
                current_row_data = row.to_dict()
    
    # 处理最后一行
    if current_text and current_row_data is not None:
        current_row_data[text_col] = current_text.strip()
        result_rows.append(current_row_data)
    
    if result_rows:
        return pd.DataFrame(result_rows)
    else:
        return df


def is_numeric_value(val: str) -> bool:
    """判断字符串是否是数字格式（包括带括号的负数、带逗号的数字等）"""
    if not val or val.strip() == '':
        return False
    
    val_clean = val.strip().replace(',', '').replace('(', '').replace(')', '').replace('–', '-').replace('"', '').replace('$', '').replace('€', '').replace('£', '').strip()
    
    # 检查是否是数字
    if val_clean == '' or val_clean == '-':
        return False
    
    # 移除可能的负号
    if val_clean.startswith('-'):
        val_clean = val_clean[1:]
    
    # 检查是否全是数字或小数点
    return val_clean.replace('.', '').isdigit()


def align_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    对齐列，将分散的数据整理到正确的列中
    
    Args:
        df: 原始DataFrame
        
    Returns:
        整理后的DataFrame
    """
    df = df.copy()
    
    if df.empty:
        return df
    
    # 创建新的DataFrame存储结果
    result_rows = []
    
    # 第一列通常是文本列
    text_col = df.columns[0]
    current_text_parts = []
    
    for idx, row in df.iterrows():
        # 获取第一列（文本列）的值
        text_val = str(row[text_col]) if pd.notna(row[text_col]) else ""
        text_val = text_val.strip()
        
        # 收集这一行的所有数值
        numeric_values = []
        for col in df.columns[1:]:
            val = row[col]
            if pd.notna(val):
                val_str = str(val).strip()
                if is_numeric_value(val_str):
                    numeric_values.append(val_str)
        
        # 如果有数值，说明这是一行完整的记录
        if numeric_values:
            # 合并之前的文本
            if current_text_parts:
                combined_text = ' '.join(current_text_parts).strip()
                current_text_parts = []
            else:
                combined_text = text_val
            
            # 创建新行
            new_row = {text_col: combined_text}
            for i, val in enumerate(numeric_values):
                col_name = f"数值_{i+1}"
                new_row[col_name] = val
            result_rows.append(new_row)
        else:
            # 如果没有数值，说明这是文本的延续
            if text_val:
                current_text_parts.append(text_val)
    
    # 处理最后可能剩余的文本
    if current_text_parts:
        combined_text = ' '.join(current_text_parts).strip()
        if combined_text:
            new_row = {text_col: combined_text}
            result_rows.append(new_row)
    
    if result_rows:
        # 确定最大列数
        max_cols = max([len(row) for row in result_rows])
        
        # 确保所有行都有相同的列数
        for row in result_rows:
            for i in range(1, max_cols):
                col_name = f"数值_{i}"
                if col_name not in row:
                    row[col_name] = ''
        
        new_df = pd.DataFrame(result_rows)
        return new_df
    else:
        return df


def clean_financial_table(filepath: str) -> Optional[pd.DataFrame]:
    """
    清理单个财务报表CSV文件
    
    Args:
        filepath: CSV文件路径
        
    Returns:
        清理后的DataFrame，如果失败返回None
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(filepath, encoding='utf-8-sig', on_bad_lines='skip', header=None)
        
        if df.empty:
            return None
        
        # 合并多行文本
        df = merge_multiline_text(df, text_col_idx=0)
        
        # 对齐列
        df = align_columns(df)
        
        # 清理数据：移除完全空白的行
        df = df.dropna(how='all')
        
        # 清理文本列：合并连续的文本
        if len(df.columns) > 0:
            text_col = df.columns[0]
            df[text_col] = df[text_col].astype(str).str.strip()
            # 移除空字符串
            df = df[df[text_col] != '']
        
        return df
        
    except Exception as e:
        print(f"  清理文件失败 {filepath}: {e}")
        return None


def clean_all_financial_tables(financial_tables_dir: str, output_dir: Optional[str] = None) -> int:
    """
    清理所有财务报表CSV文件
    
    Args:
        financial_tables_dir: 财务报表CSV文件夹路径
        output_dir: 输出目录（如果为None，则覆盖原文件，默认保存在同一文件夹）
        
    Returns:
        成功清理的文件数量
    """
    if not os.path.exists(financial_tables_dir):
        print(f"错误: 目录不存在: {financial_tables_dir}")
        return 0
    
    # 默认输出到同一文件夹（覆盖原文件）
    if output_dir is None:
        output_dir = financial_tables_dir
    else:
        os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有CSV文件
    csv_files = [f for f in os.listdir(financial_tables_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"未找到CSV文件: {financial_tables_dir}")
        return 0
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    print("开始清理...")
    
    cleaned_count = 0
    
    for csv_file in csv_files:
        filepath = os.path.join(financial_tables_dir, csv_file)
        print(f"  处理: {csv_file}")
        
        cleaned_df = clean_financial_table(filepath)
        
        if cleaned_df is not None and not cleaned_df.empty:
            # 保存清理后的文件
            output_path = os.path.join(output_dir, csv_file)
            cleaned_df.to_csv(output_path, index=False, header=False, encoding='utf-8-sig')
            print(f"    ✓ 清理完成: {len(cleaned_df)} 行")
            cleaned_count += 1
        else:
            print(f"    ✗ 清理失败或文件为空")
    
    print(f"\n清理完成: {cleaned_count}/{len(csv_files)} 个文件")
    return cleaned_count


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python clean_financial_tables.py <financial_tables_csv目录> [输出目录]")
        print("注意: 如果不指定输出目录，清理后的文件将覆盖原文件（保存在同一文件夹）")
        sys.exit(1)
    
    financial_tables_dir = sys.argv[1]
    # 默认输出到同一文件夹（覆盖原文件）
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    if output_dir is None:
        print(f"清理文件将保存在原文件夹: {financial_tables_dir}")
    
    clean_all_financial_tables(financial_tables_dir, output_dir)

