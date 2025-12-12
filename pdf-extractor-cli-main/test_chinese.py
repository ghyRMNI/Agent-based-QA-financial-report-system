#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script: Check Chinese PDF extraction functionality
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

def test_chinese_extraction(pdf_path):
    """Test Chinese extraction functionality"""
    if not os.path.exists(pdf_path):
        print(f"Error: File does not exist - {pdf_path}")
        return False
    
    print(f"Testing file: {pdf_path}")
    print("=" * 60)
    
    try:
        doc = fitz.open(pdf_path)
        print(f"PDF pages: {len(doc)}")
        
        # Test first page
        if len(doc) > 0:
            page = doc[0]
            print("\nMethod 1: get_text()")
            text1 = page.get_text()
            print(f"Extracted length: {len(text1)} characters")
            if text1:
                preview = text1[:200].replace('\n', ' ')
                print(f"Preview: {preview}")
            
            print("\nMethod 2: get_text('text')")
            text2 = page.get_text("text")
            print(f"Extracted length: {len(text2)} characters")
            if text2:
                preview = text2[:200].replace('\n', ' ')
                print(f"Preview: {preview}")
            
            # Check if contains Chinese characters
            has_chinese = False
            for text in [text1, text2]:
                if text:
                    for char in text:
                        if '\u4e00' <= char <= '\u9fff':  # Chinese character range
                            has_chinese = True
                            break
                    if has_chinese:
                        break
            
            if has_chinese:
                print("\n[OK] Chinese characters detected!")
            else:
                print("\n[WARNING] No Chinese characters detected, may be image-based PDF, OCR required")
        
        doc.close()
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        # Default to test files in examples directory
        examples_dir = Path(__file__).parent / "examples"
        pdf_files = list(examples_dir.glob("*.pdf"))
        if pdf_files:
            pdf_path = str(pdf_files[0])
            print(f"No file specified, using: {pdf_path}")
        else:
            print("Error: No PDF file found")
            print("Usage: python test_chinese.py <pdf_file_path>")
            sys.exit(1)
    
    success = test_chinese_extraction(pdf_path)
    sys.exit(0 if success else 1)

