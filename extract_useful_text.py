"""
从PDF提取的TXT文件中提取有用信息并分类保存为CSV
"""
import os
import re
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import jieba


class TextExtractor:
    """提取和分类文本信息"""
    
    # 财务关键词
    FINANCIAL_KEYWORDS = [
        '收入', '利润', '资产', '负债', '权益', '现金流', '营业额', '毛利',
        '净利', '总资产', '总负债', '营业收入', '营业成本', '营业利润',
        '税前利润', '税后利润', '每股收益', '股息', '分红', '派息',
        '流动资产', '流动负债', '非流动资产', '非流动负债',
        '经营活动', '投资活动', '筹资活动', '现金及现金等价物',
        '应收账款', '存货', '固定资产', '无形资产', '商誉',
        '百万', '亿', '港币', '美元', '人民币', 'HK$', 'US$', 'RMB'
    ]
    
    # 业务关键词
    BUSINESS_KEYWORDS = [
        '业务', '经营', '战略', '市场', '产品', '服务', '客户', '竞争',
        '发展', '增长', '扩张', '收购', '合并', '投资', '合作', '协议',
        '项目', '计划', '目标', '里程碑', '成就', '创新', '研发',
        '子公司', '附属公司', '联营公司', '合资企业', '分部', '业务分部'
    ]
    
    # 风险关键词
    RISK_KEYWORDS = [
        '风险', '不确定', '可能', '影响', '挑战', '困难', '问题',
        '波动', '下降', '减少', '损失', '诉讼', '监管', '合规',
        '市场风险', '信用风险', '流动性风险', '操作风险', '法律风险'
    ]
    
    # 管理层关键词
    MANAGEMENT_KEYWORDS = [
        '管理层', '董事', '高级管理人员', '执行董事', '非执行董事',
        '独立董事', '审核委员会', '薪酬委员会', '提名委员会',
        '讨论与分析', '展望', '前景', '策略', '方针'
    ]
    
    # 公司信息关键词
    COMPANY_INFO_KEYWORDS = [
        '公司简介', '公司资料', '公司信息', '注册地址', '主要办事处',
        '股份代号', '网站', '上市', '成立', '历史', '背景'
    ]
    
    def __init__(self):
        """初始化"""
        # 加载jieba分词
        for word in self.FINANCIAL_KEYWORDS + self.BUSINESS_KEYWORDS + self.RISK_KEYWORDS:
            jieba.add_word(word)
    
    def is_useful_paragraph(self, text: str, min_length: int = 10, max_length: int = 500) -> bool:
        """
        判断段落是否有用
        
        Args:
            text: 段落文本
            min_length: 最小长度
            max_length: 最大长度
            
        Returns:
            是否有用
        """
        text = text.strip()
        
        # 长度过滤
        if len(text) < min_length or len(text) > max_length:
            return False
        
        # 过滤纯数字或纯符号的行
        if re.match(r'^[\d\s.,\-()]+$', text):
            return False
        
        # 过滤页码、页眉页脚
        if re.match(r'^第?\s*\d+\s*页', text) or re.match(r'^\d+\s*$', text):
            return False
        
        # 过滤目录
        if re.match(r'^[\d\s.]+.{1,30}[\d\s.]+$', text):
            return False
        
        return True
    
    def contains_numbers(self, text: str) -> bool:
        """检查文本是否包含数字（财务数据）"""
        # 检查是否包含数字
        has_numbers = bool(re.search(r'\d', text))
        
        # 检查是否包含金额相关的模式
        has_amount_pattern = bool(re.search(r'\d+[,.]?\d*\s*(百万|亿|万|元|港币|美元|HK\$|US\$|RMB)', text))
        
        return has_numbers and (has_amount_pattern or len(re.findall(r'\d+', text)) >= 2)
    
    def classify_paragraph(self, text: str) -> Tuple[str, float]:
        """
        分类段落
        
        Args:
            text: 段落文本
            
        Returns:
            (类别, 置信度)
        """
        text_lower = text.lower()
        
        # 统计关键词出现次数
        financial_count = sum(1 for kw in self.FINANCIAL_KEYWORDS if kw in text)
        business_count = sum(1 for kw in self.BUSINESS_KEYWORDS if kw in text)
        risk_count = sum(1 for kw in self.RISK_KEYWORDS if kw in text)
        management_count = sum(1 for kw in self.MANAGEMENT_KEYWORDS if kw in text)
        company_info_count = sum(1 for kw in self.COMPANY_INFO_KEYWORDS if kw in text)
        
        # 检查是否包含数字
        has_numbers = self.contains_numbers(text)
        
        # 确定类别
        scores = {
            'financial_data': financial_count * 2 + (5 if has_numbers else 0),
            'business_info': business_count * 2,
            'risk_factors': risk_count * 2,
            'management_discussion': management_count * 2,
            'company_info': company_info_count * 2,
        }
        
        # 如果没有明显类别，但包含数字，归类为财务数据
        if all(score == 0 for score in scores.values()) and has_numbers:
            scores['financial_data'] = 3
        
        # 获取最高分类别
        if max(scores.values()) == 0:
            return 'other', 0.0
        
        category = max(scores, key=scores.get)
        confidence = scores[category] / (sum(scores.values()) + 1)
        
        return category, confidence
    
    def extract_key_info(self, text: str) -> Dict:
        """
        提取关键信息
        
        Args:
            text: 段落文本
            
        Returns:
            关键信息字典
        """
        info = {
            'numbers': [],
            'amounts': [],
            'dates': [],
            'percentages': []
        }
        
        # 提取数字
        numbers = re.findall(r'\d+[,.]?\d*', text)
        info['numbers'] = numbers[:5]  # 最多5个
        
        # 提取金额
        amounts = re.findall(r'\d+[,.]?\d*\s*(百万|亿|万|元|港币|美元|HK\$|US\$|RMB)', text)
        info['amounts'] = amounts[:3]
        
        # 提取日期
        dates = re.findall(r'\d{4}年\d{1,2}月\d{1,2}日|\d{4}-\d{1,2}-\d{1,2}|\d{4}年\d{1,2}月|\d{4}年', text)
        info['dates'] = dates[:3]
        
        # 提取百分比
        percentages = re.findall(r'\d+[.]?\d*\s*%', text)
        info['percentages'] = percentages[:3]
        
        return info
    
    def split_text_into_paragraphs(self, text: str) -> List[str]:
        """
        将文本分割成段落
        
        Args:
            text: 完整文本
            
        Returns:
            段落列表
        """
        # 按换行符分割
        lines = text.split('\n')
        
        paragraphs = []
        current_paragraph = []
        
        for line in lines:
            line = line.strip()
            
            # 跳过空行
            if not line:
                if current_paragraph:
                    para_text = ' '.join(current_paragraph)
                    if self.is_useful_paragraph(para_text):
                        paragraphs.append(para_text)
                    current_paragraph = []
                continue
            
            # 如果行以句号、问号、感叹号结尾，说明是段落结束
            if line.endswith(('。', '！', '？', '.', '!', '?')):
                current_paragraph.append(line)
                para_text = ' '.join(current_paragraph)
                if self.is_useful_paragraph(para_text):
                    paragraphs.append(para_text)
                current_paragraph = []
            else:
                current_paragraph.append(line)
        
        # 处理最后一个段落
        if current_paragraph:
            para_text = ' '.join(current_paragraph)
            if self.is_useful_paragraph(para_text):
                paragraphs.append(para_text)
        
        return paragraphs
    
    def process_txt_file(self, txt_file: str) -> List[Dict]:
        """
        处理单个TXT文件
        
        Args:
            txt_file: TXT文件路径
            
        Returns:
            提取的信息列表
        """
        try:
            # 读取文件
            with open(txt_file, 'r', encoding='utf-8') as f:
                text = f.read()
            
            # 提取页码
            page_match = re.search(r'page(\d+)', txt_file, re.IGNORECASE)
            page_number = int(page_match.group(1)) if page_match else None
            
            # 分割段落
            paragraphs = self.split_text_into_paragraphs(text)
            
            # 处理每个段落
            results = []
            for idx, para in enumerate(paragraphs):
                # 分类
                category, confidence = self.classify_paragraph(para)
                
                # 只保留置信度较高的
                if confidence < 0.1:
                    category = 'other'
                
                # 提取关键信息
                key_info = self.extract_key_info(para)
                
                result = {
                    'page_number': page_number,
                    'paragraph_index': idx,
                    'category': category,
                    'confidence': confidence,
                    'text': para,
                    'text_length': len(para),
                    'numbers': ','.join(str(n) for n in key_info['numbers']),
                    'amounts': ','.join(str(a) for a in key_info['amounts']),
                    'dates': ','.join(key_info['dates']),
                    'percentages': ','.join(key_info['percentages']),
                    'source_file': os.path.basename(txt_file)
                }
                
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"  处理文件失败 {txt_file}: {e}")
            return []


def extract_from_txt_folder(txt_folder: str, output_dir: str) -> Dict[str, int]:
    """
    从TXT文件夹中提取信息
    
    Args:
        txt_folder: TXT文件夹路径
        output_dir: 输出目录
        
    Returns:
        统计信息
    """
    if not os.path.exists(txt_folder):
        print(f"错误: 目录不存在: {txt_folder}")
        return {}
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有TXT文件
    txt_files = []
    for root, dirs, files in os.walk(txt_folder):
        for file in files:
            if file.endswith('.txt'):
                txt_files.append(os.path.join(root, file))
    
    if not txt_files:
        print(f"未找到TXT文件: {txt_folder}")
        return {}
    
    print(f"找到 {len(txt_files)} 个TXT文件")
    print("开始提取信息...")
    
    extractor = TextExtractor()
    all_results = []
    
    for txt_file in txt_files:
        print(f"  处理: {os.path.basename(txt_file)}")
        results = extractor.process_txt_file(txt_file)
        all_results.extend(results)
        print(f"    提取了 {len(results)} 个段落")
    
    if not all_results:
        print("未提取到有效信息")
        return {}
    
    # 转换为DataFrame
    df = pd.DataFrame(all_results)
    
    # 按类别统计
    category_counts = df['category'].value_counts().to_dict()
    
    print(f"\n提取统计:")
    print(f"  总段落数: {len(df)}")
    for category, count in category_counts.items():
        print(f"  {category}: {count}")
    
    # 保存全部数据
    all_output_file = os.path.join(output_dir, 'extracted_text_all.csv')
    df.to_csv(all_output_file, index=False, encoding='utf-8-sig')
    print(f"\n保存全部数据: {all_output_file}")
    
    # 按类别保存
    for category in df['category'].unique():
        if category == 'other':
            continue
        
        category_df = df[df['category'] == category]
        category_file = os.path.join(output_dir, f'extracted_text_{category}.csv')
        category_df.to_csv(category_file, index=False, encoding='utf-8-sig')
        print(f"  {category}: {category_file} ({len(category_df)} 条)")
    
    return category_counts


def process_pdf_extracts_txt(pdf_extracts_dir: str, output_dir: Optional[str] = None):
    """
    处理pdf_extracts目录中的所有TXT文件
    
    Args:
        pdf_extracts_dir: pdf_extracts目录路径
        output_dir: 输出目录（如果为None，则在pdf_extracts同级创建extracted_text文件夹）
    """
    print("=" * 80)
    print("开始提取PDF文本中的有用信息")
    print("=" * 80)
    
    if not os.path.exists(pdf_extracts_dir):
        print(f"错误: pdf_extracts目录不存在: {pdf_extracts_dir}")
        return
    
    # 确定输出目录
    if output_dir is None:
        parent_dir = os.path.dirname(pdf_extracts_dir)
        output_dir = os.path.join(parent_dir, 'extracted_text')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 查找所有TXT文件夹
    txt_folders = []
    for root, dirs, files in os.walk(pdf_extracts_dir):
        if 'txt' in os.path.basename(root).lower():
            txt_folders.append(root)
    
    if not txt_folders:
        print("未找到TXT文件夹")
        return
    
    print(f"找到 {len(txt_folders)} 个TXT文件夹")
    
    # 处理每个TXT文件夹
    total_stats = {}
    for txt_folder in txt_folders:
        print(f"\n处理文件夹: {txt_folder}")
        stats = extract_from_txt_folder(txt_folder, output_dir)
        
        # 合并统计
        for category, count in stats.items():
            total_stats[category] = total_stats.get(category, 0) + count
    
    print("\n" + "=" * 80)
    print("提取完成!")
    print("=" * 80)
    print(f"输出目录: {output_dir}")
    print("\n总体统计:")
    for category, count in total_stats.items():
        print(f"  {category}: {count}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python extract_useful_text.py <pdf_extracts目录> [输出目录]")
        print("示例: python extract_useful_text.py unified_outputs/长和_20251126_210725/pdf_extracts")
        sys.exit(1)
    
    pdf_extracts_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    process_pdf_extracts_txt(pdf_extracts_dir, output_dir)

