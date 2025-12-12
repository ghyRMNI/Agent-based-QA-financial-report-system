"""
从PDF提取的CSV文件中识别并提取财务报表
Extract Financial Statements from PDF Extracted CSV Files

识别财务报表（资产负债表、利润表、现金流量表、股东权益变动表等）并合并
"""

import os
import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Tuple


# 财务报表关键词（中英文）
FINANCIAL_STATEMENT_KEYWORDS = {
    'balance_sheet': [
        '资产负债表', '财务状况表', 'Balance Sheet', 'Statement of Financial Position',
        '合并资产负债表', '综合财务状况表', 'Consolidated Balance Sheet'
    ],
    'income_statement': [
        '利润表', '收益表', '损益表', 'Income Statement', 'Profit and Loss',
        '合并利润表', '综合收益表', '综合损益表', 'Consolidated Income Statement',
        '经营业绩', 'Operations', 'Statement of Operations'
    ],
    'cash_flow': [
        '现金流量表', 'Cash Flow', 'Statement of Cash Flows',
        '合并现金流量表', '综合现金流量表', 'Consolidated Cash Flow'
    ],
    'equity': [
        '股东权益', '权益变动', 'Equity', 'Statement of Changes in Equity',
        '合并股东权益', '综合权益变动', 'Consolidated Equity'
    ],
    'comprehensive_income': [
        '全面收益', '综合收益', 'Comprehensive Income',
        '综合全面收益表', 'Consolidated Comprehensive Income'
    ]
}

# 财务报表相关的其他关键词
OTHER_FINANCIAL_KEYWORDS = [
    '财务', 'Financial', '报表', 'Statement', '报告', 'Report',
    '资产', 'Asset', '负债', 'Liability', '收入', 'Revenue',
    '费用', 'Expense', '成本', 'Cost', '利润', 'Profit', 'Loss'
]


def identify_table_type(filepath: str, df: pd.DataFrame) -> Tuple[str, float]:
    """
    识别表格类型
    
    Args:
        filepath: 文件路径
        df: DataFrame
        
    Returns:
        (table_type, confidence): 表格类型和置信度
    """
    filename = os.path.basename(filepath).lower()
    filepath_lower = filepath.lower()
    
    # 检查文件名和路径
    text_to_check = filename + ' ' + filepath_lower
    
    # 检查表格内容（前几行）
    content_text = ''
    if not df.empty:
        # 获取前5行和前5列的内容作为检查文本
        preview_df = df.head(5).iloc[:, :5]
        content_text = ' '.join(preview_df.astype(str).values.flatten()).lower()
    
    all_text = (text_to_check + ' ' + content_text).lower()
    
    # 计算每种类型的匹配度
    scores = {}
    for table_type, keywords in FINANCIAL_STATEMENT_KEYWORDS.items():
        score = 0
        for keyword in keywords:
            keyword_lower = keyword.lower()
            # 在文件名中匹配
            if keyword_lower in text_to_check:
                score += 3
            # 在内容中匹配
            if keyword_lower in content_text:
                score += 2
            # 在完整文本中匹配
            if keyword_lower in all_text:
                score += 1
        
        if score > 0:
            scores[table_type] = score
    
    if not scores:
        return ('other', 0.0)
    
    # 返回得分最高的类型
    best_type = max(scores, key=scores.get)
    max_score = scores[best_type]
    
    # 计算置信度（归一化到0-1）
    total_possible_score = len(FINANCIAL_STATEMENT_KEYWORDS[best_type]) * 3
    confidence = min(max_score / total_possible_score, 1.0)
    
    return (best_type, confidence)


def is_valid_financial_table(df: pd.DataFrame, min_rows: int = 3, min_cols: int = 2) -> bool:
    """
    判断是否是有效的财务报表
    
    Args:
        df: DataFrame
        min_rows: 最小行数
        min_cols: 最小列数
        
    Returns:
        是否是有效的财务报表
    """
    if df.empty:
        return False
    
    # 检查基本尺寸
    if len(df) < min_rows or len(df.columns) < min_cols:
        return False
    
    # 检查是否包含数值数据（财务报表应该包含数字）
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) == 0:
        # 如果没有数值列，检查是否可以转换为数值
        has_numbers = False
        for col in df.columns:
            try:
                # 尝试将列转换为数值（忽略非数值）
                pd.to_numeric(df[col], errors='coerce')
                if df[col].notna().any():
                    has_numbers = True
                    break
            except:
                continue
        if not has_numbers:
            return False
    
    return True


def find_and_copy_financial_tables(pdf_extracts_dir: str, output_dir: str, basic_info: Dict = None) -> List[str]:
    """
    找出财务报表CSV文件并复制到专门文件夹
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        output_dir: 输出目录
        basic_info: 基本信息字典
        
    Returns:
        找到的财务报表文件路径列表
    """
    print("=" * 80)
    print("开始查找财务报表CSV文件")
    print("=" * 80)
    
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: pdf_extracts目录不存在: {pdf_extracts_dir}")
        return []
    
    # 创建财务报表文件夹
    financial_tables_dir = os.path.join(output_dir, 'financial_tables_csv')
    os.makedirs(financial_tables_dir, exist_ok=True)
    print(f"财务报表文件夹: {financial_tables_dir}")
    
    found_files = []
    processed_files = []
    skipped_files = []
    
    # 遍历所有子目录中的csv_selected文件夹（只使用筛选后的优质表格）
    for root, dirs, files in os.walk(pdf_extracts_dir):
        # 只处理csv_selected目录（不使用csv目录）
        if 'csv_selected' not in root.lower():
            continue
        
        csv_files = [f for f in files if f.endswith('.csv')]
        
        for csv_file in csv_files:
            filepath = os.path.join(root, csv_file)
            
            try:
                # 读取CSV文件的前几行进行快速检查
                df = pd.read_csv(filepath, encoding='utf-8-sig', nrows=10, on_bad_lines='skip')
                
                # 检查是否是有效的财务报表
                if not is_valid_financial_table(df, min_rows=3, min_cols=2):
                    skipped_files.append((filepath, "无效表格（行数或列数不足）"))
                    continue
                
                # 识别表格类型
                table_type, confidence = identify_table_type(filepath, df)
                
                # 只保留财务报表类型（置信度>0.2）或包含明显财务报表关键词的
                is_financial = False
                
                if table_type != 'other' and confidence > 0.2:
                    is_financial = True
                else:
                    # 检查文件名和路径中是否包含财务报表关键词
                    filename_lower = csv_file.lower()
                    filepath_lower = filepath.lower()
                    
                    # 检查是否包含财务报表关键词
                    for keywords_list in FINANCIAL_STATEMENT_KEYWORDS.values():
                        for keyword in keywords_list:
                            if keyword.lower() in filename_lower or keyword.lower() in filepath_lower:
                                is_financial = True
                                break
                        if is_financial:
                            break
                    
                    # 检查表格内容（读取完整文件）
                    if not is_financial:
                        try:
                            full_df = pd.read_csv(filepath, encoding='utf-8-sig', nrows=20, on_bad_lines='skip')
                            content_text = ' '.join(full_df.astype(str).values.flatten()).lower()
                            
                            for keywords_list in FINANCIAL_STATEMENT_KEYWORDS.values():
                                for keyword in keywords_list:
                                    if keyword.lower() in content_text:
                                        is_financial = True
                                        break
                                if is_financial:
                                    break
                        except:
                            pass
                
                if not is_financial:
                    skipped_files.append((filepath, f"非财务报表（类型: {table_type}, 置信度: {confidence:.2f}）"))
                    continue
                
                # 复制文件到财务报表文件夹
                # 创建有意义的文件名：包含PDF名称、页码、表格类型
                path_parts = Path(filepath).parts
                pdf_name = None
                for i, part in enumerate(path_parts):
                    if 'pdf_extracts' in part or (i > 0 and 'pdf_extracts' in path_parts[i-1]):
                        if i + 1 < len(path_parts):
                            pdf_name = path_parts[i + 1]
                            break
                
                # 提取页码
                page_match = re.search(r'page(\d+)', filepath, re.IGNORECASE)
                page_number = page_match.group(1) if page_match else 'unknown'
                
                # 创建新文件名
                safe_pdf_name = re.sub(r'[<>:"/\\|?*]', '_', str(pdf_name)) if pdf_name else 'unknown'
                new_filename = f"{table_type}_page{page_number}_{safe_pdf_name}_{csv_file}"
                new_filepath = os.path.join(financial_tables_dir, new_filename)
                
                # 复制文件
                import shutil
                shutil.copy2(filepath, new_filepath)
                
                found_files.append(new_filepath)
                processed_files.append((filepath, table_type, confidence, new_filepath))
                
            except Exception as e:
                skipped_files.append((filepath, f"处理失败: {str(e)}"))
                continue
    
    print(f"\n查找完成:")
    print(f"  找到财务报表: {len(processed_files)} 个文件")
    print(f"  跳过: {len(skipped_files)} 个文件")
    
    if processed_files:
        print(f"\n财务报表类型分布:")
        type_counts = {}
        for _, table_type, _, _ in processed_files:
            type_counts[table_type] = type_counts.get(table_type, 0) + 1
        for table_type, count in sorted(type_counts.items()):
            print(f"  {table_type}: {count} 个")
        
        print(f"\n财务报表文件已保存到: {financial_tables_dir}")
        print(f"前10个文件:")
        for i, (_, _, _, new_path) in enumerate(processed_files[:10]):
            print(f"  {i+1}. {os.path.basename(new_path)}")
    
    if skipped_files and len(skipped_files) <= 20:
        print(f"\n跳过的文件（前20个）:")
        for filepath, reason in skipped_files[:20]:
            print(f"  {os.path.basename(filepath)}: {reason}")
    elif skipped_files:
        print(f"\n跳过了 {len(skipped_files)} 个文件")
    
    return found_files


def extract_financial_tables(pdf_extracts_dir: str, basic_info: Dict = None) -> pd.DataFrame:
    """
    从pdf_extracts目录中提取财务报表
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        basic_info: 基本信息字典
        
    Returns:
        合并后的DataFrame
    """
    print("=" * 80)
    print("开始提取财务报表")
    print("=" * 80)
    
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: pdf_extracts目录不存在: {pdf_extracts_dir}")
        return pd.DataFrame()
    
    all_tables = []
    processed_files = []
    skipped_files = []
    
    # 遍历所有子目录中的csv_selected文件夹（只使用筛选后的优质表格）
    for root, dirs, files in os.walk(pdf_extracts_dir):
        # 只处理csv_selected目录（不使用csv目录）
        if 'csv_selected' not in root.lower():
            continue
        
        csv_files = [f for f in files if f.endswith('.csv')]
        
        for csv_file in csv_files:
            filepath = os.path.join(root, csv_file)
            
            try:
                # 读取CSV文件
                df = pd.read_csv(filepath, encoding='utf-8-sig', on_bad_lines='skip')
                
                # 检查是否是有效的财务报表
                if not is_valid_financial_table(df):
                    skipped_files.append((filepath, "无效表格（行数或列数不足）"))
                    continue
                
                # 识别表格类型
                table_type, confidence = identify_table_type(filepath, df)
                
                # 只保留置信度较高的财务报表（置信度>0.1）
                if table_type == 'other' and confidence < 0.1:
                    # 但如果是明显的财务报表关键词，也保留
                    filename_lower = csv_file.lower()
                    has_financial_keyword = any(
                        keyword.lower() in filename_lower 
                        for keyword in OTHER_FINANCIAL_KEYWORDS
                    )
                    if not has_financial_keyword:
                        skipped_files.append((filepath, f"非财务报表（类型: {table_type}, 置信度: {confidence:.2f}）"))
                        continue
                
                # 提取页码（从文件名）
                page_match = re.search(r'page(\d+)', filepath, re.IGNORECASE)
                page_number = int(page_match.group(1)) if page_match else None
                
                # 提取PDF名称（从路径）
                path_parts = Path(filepath).parts
                pdf_name = None
                for i, part in enumerate(path_parts):
                    if 'pdf_extracts' in part or (i > 0 and 'pdf_extracts' in path_parts[i-1]):
                        # 找到pdf_extracts后的第一个目录名
                        if i + 1 < len(path_parts):
                            pdf_name = path_parts[i + 1]
                            break
                
                # 为每行数据添加元数据
                df['source'] = 'pdf_table'
                df['data_type'] = table_type
                df['table_type'] = table_type
                df['file_source'] = csv_file
                df['file_path'] = filepath
                df['page_number'] = page_number
                df['pdf_name'] = pdf_name
                df['confidence'] = confidence
                
                # 添加基本信息
                if basic_info:
                    df['stock_code'] = basic_info.get('code', '')
                    df['company_name'] = basic_info.get('name', '')
                else:
                    df['stock_code'] = ''
                    df['company_name'] = ''
                
                # 添加行索引（在原始表格中的行号）
                df['row_index'] = range(len(df))
                
                all_tables.append(df)
                processed_files.append((filepath, table_type, confidence))
                
            except Exception as e:
                skipped_files.append((filepath, f"读取失败: {str(e)}"))
                continue
    
    print(f"\n处理完成:")
    print(f"  成功提取: {len(processed_files)} 个表格")
    print(f"  跳过: {len(skipped_files)} 个文件")
    
    if processed_files:
        print(f"\n提取的表格类型分布:")
        type_counts = {}
        for _, table_type, _ in processed_files:
            type_counts[table_type] = type_counts.get(table_type, 0) + 1
        for table_type, count in sorted(type_counts.items()):
            print(f"  {table_type}: {count} 个")
    
    if skipped_files and len(skipped_files) <= 20:
        print(f"\n跳过的文件（前20个）:")
        for filepath, reason in skipped_files[:20]:
            print(f"  {os.path.basename(filepath)}: {reason}")
    elif skipped_files:
        print(f"\n跳过了 {len(skipped_files)} 个文件（详细信息已省略）")
    
    if not all_tables:
        print("\n警告: 没有找到任何财务报表")
        return pd.DataFrame()
    
    # 合并所有表格
    print("\n合并表格...")
    integrated_df = pd.concat(all_tables, ignore_index=True)
    
    print(f"  总记录数: {len(integrated_df)}")
    print(f"  总列数: {len(integrated_df.columns)}")
    
    return integrated_df


def save_financial_tables(integrated_df: pd.DataFrame, output_dir: str, basic_info: Dict = None):
    """
    保存财务报表数据（暂时不使用，先只复制文件）
    """
    """
    保存财务报表数据
    
    Args:
        integrated_df: 合并后的DataFrame
        output_dir: 输出目录
        basic_info: 基本信息
    """
    if integrated_df.empty:
        print("没有数据需要保存")
        return
    
    # 删除空列
    print("\n检查并删除空列...")
    initial_columns = len(integrated_df.columns)
    
    empty_columns = []
    for col in integrated_df.columns:
        if integrated_df[col].isna().all():
            empty_columns.append(col)
        elif integrated_df[col].dtype == 'object':
            if (integrated_df[col].astype(str).str.strip() == '').all():
                empty_columns.append(col)
    
    # 保留核心列
    core_columns = ['source', 'data_type', 'table_type', 'stock_code', 'company_name',
                   'file_source', 'file_path', 'page_number', 'pdf_name', 'row_index', 'confidence']
    columns_to_drop = [col for col in empty_columns if col not in core_columns]
    
    if columns_to_drop:
        integrated_df = integrated_df.drop(columns=columns_to_drop)
        print(f"  删除了 {len(columns_to_drop)} 个空列")
    else:
        print("  没有发现空列")
    
    final_columns = len(integrated_df.columns)
    print(f"  列数: {initial_columns} -> {final_columns}")
    
    # 保存文件
    output_file = os.path.join(output_dir, 'financial_tables_from_pdf.csv')
    integrated_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n财务报表已保存:")
    print(f"  文件: {output_file}")
    print(f"  记录数: {len(integrated_df)}")
    print(f"  列数: {final_columns}")
    
    # 按表格类型统计
    if 'table_type' in integrated_df.columns:
        print(f"\n表格类型统计:")
        type_counts = integrated_df['table_type'].value_counts()
        for table_type, count in type_counts.items():
            print(f"  {table_type}: {count} 条记录")


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
    
    # 加载基本信息
    basic_info = {}
    basic_info_path = os.path.join(output_dir, 'stock_data', '000001_Basic_Info.csv')
    if os.path.exists(basic_info_path):
        try:
            df = pd.read_csv(basic_info_path)
            if not df.empty:
                basic_info = df.iloc[0].to_dict()
        except:
            pass
    
    # 查找并复制财务报表CSV文件
    pdf_extracts_dir = os.path.join(output_dir, 'pdf_extracts')
    found_files = find_and_copy_financial_tables(pdf_extracts_dir, output_dir, basic_info)
    
    if found_files:
        print(f"\n成功找到 {len(found_files)} 个财务报表CSV文件")
        print("文件已保存到 financial_tables_csv 文件夹")
    else:
        print("\n未找到财务报表CSV文件")
    
    print("=" * 80)


if __name__ == "__main__":
    main()

