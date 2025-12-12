"""
从PDF提取的txt文件中提取有用的数据信息，分类保存为CSV
"""
import os
import re
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional


# 定义分类关键词
CATEGORY_KEYWORDS = {
    '财务数据': [
        '收益', '收入', '溢利', '亏损', 'EBITDA', 'EBIT', '净利润', '毛利润', '营业额',
        '资产', '负债', '权益', '现金流', '资本', '投资', '回报', '股息', '派息',
        '百万', '亿元', '万元', '港币', '美元', '欧元', '英镑', '百分比', '%',
        '增长', '减少', '上升', '下降', '同比', '环比'
    ],
    '业务回顾': [
        '业务', '经营', '运营', '客户', '市场', '销售', '服务', '产品', '技术',
        '网络', '覆盖', '基站', '5G', '4G', '数据', '流量', '用户', '订阅',
        '零售', '港口', '能源', '电讯', '基建', '地产', '酒店', '投资'
    ],
    '风险提示': [
        '风险', '挑战', '困难', '不利', '影响', '威胁', '不确定性', '波动',
        '监管', '政策', '法律', '合规', '诉讼', '争议'
    ],
    '公司治理': [
        '董事', '董事会', '委员会', '治理', '管治', '合规', '审计', '核数',
        '股东', '权益', '投票', '决议', '会议'
    ],
    '战略规划': [
        '战略', '规划', '计划', '目标', '展望', '未来', '发展', '扩张',
        '收购', '合并', '重组', '转型', '创新', '数字化'
    ],
    '其他重要信息': [
        '重要', '关键', '主要', '核心', '显著', '重大', '特别', '公告', '通知'
    ]
}

# 财务数据模式 - 检测金额和百分比
FINANCIAL_PATTERNS = [
    r'\d+[.,]?\d*\s*(百万|亿元|万元|亿|万|港币|美元|欧元|英镑|元)',
    r'\d+[.,]?\d*%',
    r'[增减]长?\s*\d+[.,]?\d*%',
    r'[上升下降]\s*\d+[.,]?\d*%',
    r'[较与]?\s*(去年|去年|同期|上[年季])\s*[增减]长?\s*\d+[.,]?\d*%',
    r'\d+[.,]?\d*\s*(亿|万)\s*(元|港币|美元)',
]

# 货币单位关键词
CURRENCY_KEYWORDS = ['港币', '美元', '欧元', '英镑', '元', '万元', '亿元', '百万', '万', '亿']

# 最大句子长度
MAX_SENTENCE_LENGTH = 200


def split_long_sentence(text: str, max_length: int = MAX_SENTENCE_LENGTH) -> List[str]:
    """
    将长句子拆分为较短的句子
    
    Args:
        text: 原始文本
        max_length: 最大句子长度
        
    Returns:
        拆分后的句子列表
    """
    if len(text) <= max_length:
        return [text]
    
    sentences = []
    # 按句号、分号、换行符拆分
    parts = re.split(r'[。；\n]', text)
    
    current_sentence = ""
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        # 如果当前句子加上新部分不超过最大长度
        if len(current_sentence) + len(part) + 1 <= max_length:
            if current_sentence:
                current_sentence += "。" + part
            else:
                current_sentence = part
        else:
            # 保存当前句子
            if current_sentence:
                sentences.append(current_sentence)
            # 如果新部分本身就很长，需要进一步拆分
            if len(part) > max_length:
                # 按逗号拆分
                sub_parts = re.split(r'[，,]', part)
                sub_sentence = ""
                for sub_part in sub_parts:
                    sub_part = sub_part.strip()
                    if not sub_part:
                        continue
                    if len(sub_sentence) + len(sub_part) + 1 <= max_length:
                        if sub_sentence:
                            sub_sentence += "，" + sub_part
                        else:
                            sub_sentence = sub_part
                    else:
                        if sub_sentence:
                            sentences.append(sub_sentence)
                        sub_sentence = sub_part
                if sub_sentence:
                    current_sentence = sub_sentence
            else:
                current_sentence = part
    
    if current_sentence:
        sentences.append(current_sentence)
    
    return sentences if sentences else [text]


def contains_financial_data(text: str) -> bool:
    """
    检查文本是否包含财务数据（金额或百分比）
    只检查是否包含金额（多少钱）和百分比，不需要其他关键词
    
    Args:
        text: 文本内容
        
    Returns:
        是否包含财务数据
    """
    # 检查是否包含金额（数字+货币单位）
    # 模式1: 数字 + 货币单位（港币、美元、万元、亿元等）
    amount_patterns = [
        r'\d+[.,]?\d*\s*(百万|亿元|万元|亿|万|港币|美元|欧元|英镑|元)',
        r'(港币|美元|欧元|英镑)\s*\d+[.,]?\d*',
        r'\d+[.,]?\d*\s*(亿|万)\s*(元|港币|美元)',
    ]
    has_amount = any(re.search(pattern, text) for pattern in amount_patterns)
    
    # 检查是否包含百分比
    has_percentage = bool(re.search(r'\d+[.,]?\d*%', text))
    
    # 检查是否包含数字+货币关键词（更宽松的匹配）
    if not has_amount and re.search(r'\d+[.,]?\d+', text):  # 至少包含一个带小数点的数字
        for currency in CURRENCY_KEYWORDS:
            if currency in text:
                # 检查货币单位前后是否有数字（允许中间有空格）
                pattern = rf'\d+[.,]?\d*\s*{currency}|{currency}\s*\d+[.,]?\d*'
                if re.search(pattern, text):
                    has_amount = True
                    break
    
    return has_amount or has_percentage


def categorize_text(text: str) -> str:
    """
    对文本进行分类
    
    Args:
        text: 文本内容
        
    Returns:
        分类名称
    """
    text_lower = text.lower()
    
    # 计算每个分类的匹配度
    scores = {}
    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in text)
        if score > 0:
            scores[category] = score
    
    if not scores:
        return '其他'
    
    # 返回得分最高的分类
    return max(scores, key=scores.get)


def extract_useful_sections(text: str, min_length: int = 5) -> List[Dict]:
    """
    从文本中提取有用的句子（片段化，按句子拆分）
    
    Args:
        text: 完整文本
        min_length: 最小句子长度
        
    Returns:
        提取的句子列表，每个句子包含：内容、分类、页码
    """
    sections = []
    
    # 按页面分割
    pages = re.split(r'---\s*Page\s*(\d+)\s*---', text)
    
    current_page = None
    for i, part in enumerate(pages):
        if i % 2 == 1:
            # 这是页码（奇数索引）
            if part.strip().isdigit():
                current_page = int(part.strip())
        else:
            # 这是页面内容（偶数索引）
            if i == 0:
                # 跳过第一个分割之前的内容
                continue
            page_content = part.strip()
            if not page_content:
                continue
            
            # 按句子拆分（句号、分号、换行符）
            # 先按换行符拆分（保持表格数据的行结构）
            lines = page_content.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or len(line) < min_length:
                    continue
                
                # 检查是否包含金额或百分比
                if contains_financial_data(line):
                    # 如果句子太长，进一步拆分
                    if len(line) > MAX_SENTENCE_LENGTH:
                        sentences = split_long_sentence(line)
                    else:
                        sentences = [line]
                    
                    for sentence in sentences:
                        sentence = sentence.strip()
                        if len(sentence) < min_length:
                            continue
                        
                        # 再次检查拆分后的句子是否包含财务数据
                        if contains_financial_data(sentence):
                            # 分类
                            category = categorize_text(sentence)
                            
                            sections.append({
                                '内容': sentence,
                                '分类': category,
                                '页码': current_page if current_page else None,
                                '长度': len(sentence)
                            })
    
    return sections


def process_txt_file(txt_filepath: str, output_dir: str) -> Dict[str, int]:
    """
    处理单个txt文件
    
    Args:
        txt_filepath: txt文件路径
        output_dir: 输出目录
        
    Returns:
        统计信息：{分类: 数量}
    """
    print(f"  处理文件: {os.path.basename(txt_filepath)}")
    
    try:
        # 读取文件
        with open(txt_filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 提取有用段落
        sections = extract_useful_sections(content)
        
        if not sections:
            print(f"    未找到有用信息")
            return {}
        
        # 按分类分组
        categorized = {}
        for section in sections:
            category = section['分类']
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(section)
        
        # 保存每个分类的CSV
        stats = {}
        for category, items in categorized.items():
            df = pd.DataFrame(items)
            
            # 保存CSV
            output_filename = f"{Path(txt_filepath).stem}_{category}.csv"
            output_path = os.path.join(output_dir, output_filename)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            stats[category] = len(items)
            print(f"    {category}: {len(items)} 条")
        
        return stats
        
    except Exception as e:
        print(f"    处理失败: {e}")
        return {}


def extract_text_data_from_pdf_extracts(pdf_extracts_dir: str, output_dir: Optional[str] = None) -> Dict[str, int]:
    """
    从pdf_extracts目录中提取txt文件的有用信息
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        output_dir: 输出目录（如果为None，则保存到pdf_extracts的父目录，即公司根目录）
        
    Returns:
        总体统计信息
    """
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: 目录不存在: {pdf_extracts_dir}")
        return {}
    
    # 设置输出目录 - 默认为公司根目录
    if output_dir is None:
        output_dir = os.path.dirname(pdf_extracts_dir)
    
    print("=" * 80)
    print("开始从txt文件提取关键词信息")
    print("=" * 80)
    print(f"输入目录: {pdf_extracts_dir}")
    print(f"输出目录: {output_dir}\n")
    
    # 查找所有txt文件
    txt_files = []
    for root, dirs, files in os.walk(pdf_extracts_dir):
        # 只查找txt目录下的文件
        if 'txt' in root.lower():
            for file in files:
                if file.endswith('.txt') and file.endswith('_text.txt'):
                    txt_files.append(os.path.join(root, file))
    
    if not txt_files:
        print("未找到txt文件")
        return {}
    
    print(f"找到 {len(txt_files)} 个txt文件\n")
    
    # 存储所有提取的数据
    all_sections = []
    
    # 处理每个txt文件
    for txt_file in txt_files:
        print(f"  处理文件: {os.path.basename(txt_file)}")
        
        try:
            # 读取文件
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取有用段落
            sections = extract_useful_sections(content)
            
            if sections:
                all_sections.extend(sections)
                print(f"    提取了 {len(sections)} 条关键信息")
            else:
                print(f"    未找到关键信息")
                
        except Exception as e:
            print(f"    处理失败: {e}")
            continue
    
    # 如果没有提取到任何关键词，不生成文件
    if not all_sections:
        print("\n" + "=" * 80)
        print("未提取到任何关键词信息，不生成CSV文件")
        print("=" * 80 + "\n")
        return {}
    
    # 转换为DataFrame
    df = pd.DataFrame(all_sections)
    
    # 按分类统计
    total_stats = df['分类'].value_counts().to_dict()
    
    # 保存为单个CSV文件
    output_filename = "extracted_keywords.csv"
    output_path = os.path.join(output_dir, output_filename)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # 打印总结
    print("\n" + "=" * 80)
    print("关键词提取完成！")
    print("=" * 80)
    print(f"输出文件: {output_path}")
    print(f"\n分类统计:")
    for category, count in sorted(total_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"  {category}: {count} 条")
    print(f"\n总计: {len(df)} 条")
    print("=" * 80 + "\n")
    
    return total_stats


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python extract_text_data.py <pdf_extracts目录> [输出目录]")
        print("示例: python extract_text_data.py unified_outputs/长和_20251126_210725/pdf_extracts")
        sys.exit(1)
    
    pdf_extracts_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    extract_text_data_from_pdf_extracts(pdf_extracts_dir, output_dir)
