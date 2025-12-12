#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查 PDF 文件并提取中文内容
"""
import os
import sys
import io
import fitz
from pathlib import Path

# Fix encoding for Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def check_pdf(pdf_path):
    """检查 PDF 文件内容"""
    print(f"检查文件: {pdf_path}")
    print("=" * 60)
    
    if not os.path.exists(pdf_path):
        print(f"错误：文件不存在")
        return False
    
    try:
        doc = fitz.open(pdf_path)
        print(f"PDF 页数: {len(doc)}")
        
        total_text = ""
        has_text = False
        has_chinese = False
        
        # 检查前3页
        check_pages = min(3, len(doc))
        for i in range(check_pages):
            page = doc[i]
            
            # 方法1: 标准提取
            text1 = page.get_text()
            # 方法2: 文本模式提取
            text2 = page.get_text("text")
            # 方法3: 字典模式提取
            text_dict = page.get_text("dict")
            
            page_text = text2 if text2 else text1
            if not page_text and text_dict:
                # 从字典中提取文本
                blocks_text = []
                if "blocks" in text_dict:
                    for block in text_dict["blocks"]:
                        if "lines" in block:
                            for line in block["lines"]:
                                if "spans" in line:
                                    for span in line["spans"]:
                                        if "text" in span:
                                            blocks_text.append(span["text"])
                page_text = "".join(blocks_text)
            
            if page_text:
                has_text = True
                total_text += page_text
                # 检查中文字符
                for char in page_text:
                    if '\u4e00' <= char <= '\u9fff':
                        has_chinese = True
                        break
        
        doc.close()
        
        print(f"\n文本提取结果:")
        print(f"  - 是否提取到文本: {'是' if has_text else '否'}")
        print(f"  - 是否包含中文: {'是' if has_chinese else '否'}")
        print(f"  - 提取的文本长度: {len(total_text)} 字符")
        
        if total_text:
            # 显示前500个字符
            preview = total_text[:500].replace('\n', ' ')
            print(f"\n文本预览（前500字符）:")
            print("-" * 60)
            print(preview)
            print("-" * 60)
            
            # 统计中文字符
            chinese_chars = [c for c in total_text if '\u4e00' <= c <= '\u9fff']
            if chinese_chars:
                print(f"\n中文字符数量: {len(chinese_chars)}")
                print(f"中文字符示例: {''.join(chinese_chars[:20])}")
        else:
            print("\n[警告] 未提取到任何文本内容")
            print("可能原因:")
            print("  1. PDF 中的内容是图片形式（扫描件）")
            print("  2. PDF 使用了特殊编码或字体")
            print("  3. PDF 文件损坏")
            print("\n建议: 使用 --ocr 选项进行 OCR 识别")
        
        return True
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # 查找 examples 目录下的所有 PDF 文件
    examples_dir = Path(__file__).parent / "examples"
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        # 列出所有 PDF 文件
        pdf_files = list(examples_dir.glob("*.pdf"))
        if not pdf_files:
            print("错误：examples 目录下未找到 PDF 文件")
            sys.exit(1)
        
        print("找到以下 PDF 文件:")
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"  {i}. {pdf_file.name}")
        
        # 查找包含中文的文件名
        chinese_pdfs = [f for f in pdf_files if any('\u4e00' <= c <= '\u9fff' for c in f.name)]
        if chinese_pdfs:
            pdf_path = str(chinese_pdfs[0])
            print(f"\n自动选择: {chinese_pdfs[0].name}")
        else:
            pdf_path = str(pdf_files[0])
            print(f"\n自动选择: {pdf_files[0].name}")
    
    check_pdf(pdf_path)

