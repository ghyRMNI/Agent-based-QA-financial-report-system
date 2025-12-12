"""
整合股票数据和新闻数据
Integrate Stock Data and News Data

将stock_data和news_data文件夹中的数据整合到一个CSV文件中
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import json


def load_basic_info(basic_info_path):
    """加载基本信息"""
    if os.path.exists(basic_info_path):
        df = pd.read_csv(basic_info_path)
        if not df.empty:
            return df.iloc[0].to_dict()
    return {}


def process_stock_price_data(price_data_path, basic_info):
    """处理股票价格数据"""
    if not os.path.exists(price_data_path):
        return pd.DataFrame()
    
    df = pd.read_csv(price_data_path)
    
    # 添加数据来源标识
    df['source'] = 'stock'
    df['data_type'] = 'price'
    
    # 添加基本信息
    if basic_info:
        df['stock_code'] = basic_info.get('code', '')
        df['company_name'] = basic_info.get('name', '')
    else:
        # 从文件名提取股票代码
        filename = os.path.basename(price_data_path)
        stock_code = filename.split('_')[0]
        df['stock_code'] = stock_code
        df['company_name'] = ''
    
    # 确保日期列存在
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['year'] = df['date'].dt.year
    else:
        df['date'] = None
        df['year'] = None
    
    # 重命名列，添加前缀以便区分
    price_columns = ['open', 'close', 'high', 'low', 'volume', 'amount', 
                    'amplitude', 'change_percent', 'change_amount', 'turnover',
                    'MA5', 'MA10', 'MA20', 'MA60', 'RSI', 'MACD', 
                    'MACD_Signal', 'MACD_Histogram', 'BB_Middle', 'BB_Upper', 'BB_Lower']
    
    # 选择需要的列
    columns_to_keep = ['source', 'data_type', 'date', 'year', 'stock_code', 'company_name']
    columns_to_keep.extend([col for col in price_columns if col in df.columns])
    
    return df[columns_to_keep]


def process_financial_data(financial_data_path, basic_info):
    """处理财务数据"""
    if not os.path.exists(financial_data_path):
        return pd.DataFrame()
    
    df = pd.read_csv(financial_data_path)
    
    # 财务数据是宽格式，需要转换为长格式
    # 第一列是指标名称，后面是不同时间点的数据
    if df.empty or len(df.columns) < 2:
        return pd.DataFrame()
    
    # 获取指标列名（第一列）
    indicator_col = df.columns[0]
    
    # 获取所有日期列（除了第一列）
    date_columns = [col for col in df.columns[1:] if col != indicator_col]
    
    result_rows = []
    
    for _, row in df.iterrows():
        indicator_name = row[indicator_col]
        
        # 遍历每个日期列
        for date_col in date_columns:
            value = row[date_col]
            
            # 跳过空值
            if pd.isna(value):
                continue
            
            # 解析日期（格式可能是YYYYMMDD或YYYY-MM-DD）
            try:
                if len(str(date_col)) == 8 and str(date_col).isdigit():
                    # YYYYMMDD格式
                    date_str = f"{date_col[:4]}-{date_col[4:6]}-{date_col[6:8]}"
                    date = pd.to_datetime(date_str, errors='coerce')
                else:
                    date = pd.to_datetime(date_col, errors='coerce')
                
                if pd.isna(date):
                    continue
                
                result_rows.append({
                    'source': 'stock',
                    'data_type': 'financial',
                    'date': date,
                    'year': date.year if not pd.isna(date) else None,
                    'stock_code': basic_info.get('code', '') if basic_info else '',
                    'company_name': basic_info.get('name', '') if basic_info else '',
                    'financial_indicator': indicator_name,
                    'financial_value': value
                })
            except:
                continue
    
    if result_rows:
        return pd.DataFrame(result_rows)
    return pd.DataFrame()


def process_news_data(news_data_dir, basic_info):
    """处理新闻数据"""
    news_files = []
    
    # 查找所有新闻CSV文件
    if os.path.exists(news_data_dir):
        for file in os.listdir(news_data_dir):
            if file.endswith('.csv') and 'news' in file.lower():
                news_files.append(os.path.join(news_data_dir, file))
    
    if not news_files:
        return pd.DataFrame()
    
    # 合并所有新闻文件
    all_news = []
    for news_file in news_files:
        try:
            df = pd.read_csv(news_file, encoding='utf-8-sig')
            all_news.append(df)
        except Exception as e:
            print(f"读取新闻文件失败 {news_file}: {e}")
            continue
    
    if not all_news:
        return pd.DataFrame()
    
    # 合并所有新闻数据
    news_df = pd.concat(all_news, ignore_index=True)
    
    # 添加数据来源标识
    news_df['source'] = 'news'
    news_df['data_type'] = 'news_item'
    
    # 添加基本信息
    if basic_info:
        news_df['stock_code'] = basic_info.get('code', '')
        if 'company_name' not in news_df.columns or news_df['company_name'].isna().all():
            news_df['company_name'] = basic_info.get('name', '')
    else:
        # 从文件名提取股票代码
        if news_files:
            filename = os.path.basename(news_files[0])
            stock_code = filename.split('_')[0]
            news_df['stock_code'] = stock_code
        else:
            news_df['stock_code'] = ''
        
        if 'Company' in news_df.columns:
            news_df['company_name'] = news_df['Company']
        else:
            news_df['company_name'] = ''
    
    # 处理时间字段
    if 'Time' in news_df.columns:
        # 尝试解析时间
        news_df['date'] = pd.to_datetime(news_df['Time'], errors='coerce')
        # 如果解析失败，尝试从TimeRange提取
        if news_df['date'].isna().all() and 'TimeRange' in news_df.columns:
            # 从TimeRange中提取开始日期
            def extract_date(tr):
                if pd.isna(tr):
                    return None
                try:
                    if ' to ' in str(tr):
                        start_date = str(tr).split(' to ')[0]
                        return pd.to_datetime(start_date, errors='coerce')
                except:
                    pass
                return None
            news_df['date'] = news_df['TimeRange'].apply(extract_date)
        
        news_df['year'] = news_df['date'].dt.year
    else:
        news_df['date'] = None
        news_df['year'] = None
    
    # 选择需要的列
    news_columns = ['Title', 'Source', 'Time', 'Summary', 'Link', 'TimeRange']
    columns_to_keep = ['source', 'data_type', 'date', 'year', 'stock_code', 'company_name']
    columns_to_keep.extend([col for col in news_columns if col in news_df.columns])
    
    # 重命名列以便统一
    rename_map = {
        'Title': 'news_title',
        'Source': 'news_source',
        'Time': 'news_time',
        'Summary': 'news_summary',
        'Link': 'news_link',
        'TimeRange': 'news_time_range'
    }
    
    result_df = news_df[columns_to_keep].copy()
    result_df.rename(columns=rename_map, inplace=True)
    
    return result_df


def integrate_data(output_dir):
    """
    整合数据
    
    Args:
        output_dir: 输出目录路径（包含stock_data和news_data文件夹）
    """
    print("=" * 80)
    print("开始整合股票数据和新闻数据")
    print("=" * 80)
    
    stock_data_dir = os.path.join(output_dir, 'stock_data')
    news_data_dir = os.path.join(output_dir, 'news_data')
    
    # 检查目录是否存在
    if not os.path.exists(stock_data_dir):
        print(f"错误: stock_data目录不存在: {stock_data_dir}")
        return
    
    if not os.path.exists(news_data_dir):
        print(f"警告: news_data目录不存在: {news_data_dir}")
    
    # 加载基本信息
    basic_info_path = os.path.join(stock_data_dir, '000001_Basic_Info.csv')
    basic_info = load_basic_info(basic_info_path)
    
    if basic_info:
        print(f"加载基本信息: {basic_info.get('name', '')} ({basic_info.get('code', '')})")
    
    # 处理股票价格数据
    print("\n处理股票价格数据...")
    price_data_path = os.path.join(stock_data_dir, '000001_Price_Data.csv')
    price_df = process_stock_price_data(price_data_path, basic_info)
    print(f"  价格数据: {len(price_df)} 条记录")
    
    # 处理财务数据
    print("\n处理财务数据...")
    financial_data_path = os.path.join(stock_data_dir, '000001_Financial_Financial_Summary.csv')
    financial_df = process_financial_data(financial_data_path, basic_info)
    print(f"  财务数据: {len(financial_df)} 条记录")
    
    # 处理新闻数据
    print("\n处理新闻数据...")
    news_df = process_news_data(news_data_dir, basic_info)
    print(f"  新闻数据: {len(news_df)} 条记录")
    
    # 合并所有数据
    print("\n合并数据...")
    all_data = []
    
    if not price_df.empty:
        all_data.append(price_df)
    if not financial_df.empty:
        all_data.append(financial_df)
    if not news_df.empty:
        all_data.append(news_df)
    
    if not all_data:
        print("错误: 没有找到任何数据")
        return
    
    # 合并DataFrame
    integrated_df = pd.concat(all_data, ignore_index=True)
    
    # 按日期排序
    if 'date' in integrated_df.columns:
        integrated_df = integrated_df.sort_values('date', na_position='last')
    
    # 删除空列（所有值都是NaN或None的列）
    print("\n检查并删除空列...")
    initial_columns = len(integrated_df.columns)
    
    # 检查每一列是否为空
    empty_columns = []
    for col in integrated_df.columns:
        # 检查列是否全为空（NaN、None、空字符串）
        if integrated_df[col].isna().all():
            empty_columns.append(col)
        elif integrated_df[col].dtype == 'object':
            # 对于字符串类型，还要检查是否全是空字符串
            if (integrated_df[col].astype(str).str.strip() == '').all():
                empty_columns.append(col)
    
    # 删除空列（但保留核心标识列）
    core_columns = ['source', 'data_type', 'date', 'year', 'stock_code', 'company_name']
    columns_to_drop = [col for col in empty_columns if col not in core_columns]
    
    if columns_to_drop:
        integrated_df = integrated_df.drop(columns=columns_to_drop)
        print(f"  删除了 {len(columns_to_drop)} 个空列: {', '.join(columns_to_drop)}")
    else:
        print("  没有发现空列")
    
    final_columns = len(integrated_df.columns)
    print(f"  列数: {initial_columns} -> {final_columns}")
    
    # 保存整合后的数据
    output_file = os.path.join(output_dir, 'integrated_stock_news_data.csv')
    integrated_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n整合完成!")
    print(f"  总记录数: {len(integrated_df)}")
    print(f"  输出文件: {output_file}")
    
    # 打印数据统计
    print("\n数据统计:")
    print(f"  价格数据: {len(price_df)} 条")
    print(f"  财务数据: {len(financial_df)} 条")
    print(f"  新闻数据: {len(news_df)} 条")
    
    print("\n数据来源分布:")
    if 'source' in integrated_df.columns:
        print(integrated_df['source'].value_counts().to_string())
    
    print("\n数据类型分布:")
    if 'data_type' in integrated_df.columns:
        print(integrated_df['data_type'].value_counts().to_string())
    
    print("=" * 80)


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
                # 按修改时间排序，选择最新的
                dirs.sort(key=lambda x: os.path.getmtime(os.path.join(base_dir, x)), reverse=True)
                output_dir = os.path.join(base_dir, dirs[0])
                print(f"使用最新的输出目录: {output_dir}")
            else:
                print("错误: 未找到输出目录")
                return
        else:
            print("错误: unified_outputs目录不存在")
            return
    
    integrate_data(output_dir)


if __name__ == "__main__":
    main()

