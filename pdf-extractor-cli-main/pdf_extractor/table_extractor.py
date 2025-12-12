"""
Module for extracting tables from PDF files using pdfplumber
"""
import os
import logging
import pdfplumber
import pandas as pd
import re
from typing import List, Set, Dict, Any, Optional
from pathlib import Path

from .utils import ensure_output_dir, sanitize_filename, get_pdf_output_dirs


class TableExtractor:
    """
    Extract tables from PDF files using pdfplumber
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the table extractor
        
        Args:
            logger: Logger object for logging messages
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def _is_financial_statement_table(self, table_data: List[List[Any]]) -> bool:
        """
        Determine if a table is likely a financial statement table based on its structure and content.
        
        Criteria:
        1. Has sufficient columns (at least 3)
        2. Has sufficient rows (at least 5)
        3. Contains numeric data with financial formatting (commas, parentheses, currency symbols)
        4. Has structured layout (not just text)
        
        Args:
            table_data: Raw table data as list of rows
            
        Returns:
            True if table appears to be a financial statement, False otherwise
        """
        if not table_data or len(table_data) < 2:
            return False
        
        # Filter out completely empty rows
        non_empty_rows = [row for row in table_data if any(cell and str(cell).strip() for cell in row)]
        
        if len(non_empty_rows) < 2:
            return False
        
        # For large tables (10+ rows), be more lenient with requirements
        is_large_table = len(non_empty_rows) >= 10
        
        # Check column count - financial statements typically have multiple columns
        max_cols = max(len(row) for row in non_empty_rows if row)
        # Allow 1 column for large tables, 2 columns for smaller tables
        min_cols = 1 if is_large_table else 2
        if max_cols < min_cols:
            return False
        
        # Count numeric cells with financial formatting
        # Look for: numbers with commas, numbers in parentheses (negative), currency symbols
        # Pattern for numbers: digits with optional commas, parentheses, or Chinese number characters
        numeric_pattern = re.compile(r'[\(（]?\s*[\d,，]+\s*[\)）]?|[\d,，]+')
        # Pattern for cells containing numbers (more flexible)
        has_number_pattern = re.compile(r'\d[\d,，\s\(（\)）]*')
        currency_pattern = re.compile(r'[人民幣元百千萬億港幣美元€£¥]', re.IGNORECASE)
        
        numeric_cell_count = 0
        financial_numeric_count = 0  # Count cells with financial-format numbers (with commas, parentheses, etc.)
        total_cells = 0
        has_currency = False
        long_text_cell_count = 0  # Cells with long text (likely sentences)
        text_cell_lengths = []  # Track text lengths for analysis
        pure_text_cell_count = 0  # Cells that are purely text (no numbers at all)
        
        # Pattern to detect sentence-like text (contains punctuation, connectors)
        sentence_pattern = re.compile(r'[。，、；：！？,\.;:!?].{3,}|[的之了在是].{2,}')
        # Pattern for financial-format numbers: must have commas, parentheses, or be multi-digit
        financial_number_pattern = re.compile(r'\d{1,3}([,，]\d{3})+|[\(（]\s*\d+[\d,，]*\s*[\)）]|\d{4,}')
        
        for row in non_empty_rows:
            for cell in row:
                if cell:
                    cell_str = str(cell).strip()
                    if cell_str and cell_str not in ['–', '-', '—', '']:
                        total_cells += 1
                        cell_length = len(cell_str)
                        text_cell_lengths.append(cell_length)
                        
                        # Check if cell contains any digits
                        has_digits = bool(re.search(r'\d', cell_str))
                        
                        # Check for long text cells (likely sentences/paragraphs, not data)
                        if cell_length > 20:  # Long text likely indicates prose
                            long_text_cell_count += 1
                            # If it looks like a sentence (has punctuation and connectors)
                            if sentence_pattern.search(cell_str):
                                long_text_cell_count += 0.5  # Extra penalty for sentence-like text
                        
                        # Count pure text cells (no digits at all)
                        if not has_digits:
                            pure_text_cell_count += 1
                        
                        # Check for numeric patterns - look for digits
                        if has_number_pattern.search(cell_str):
                            # Check if it's a meaningful number (not just a single digit in text)
                            # Look for patterns like: "1,234", "(123)", "123", "1,234,567" etc.
                            if re.search(r'\d{1,3}([,，]\d{3})*|\(\d+\)|[\d,，]{2,}', cell_str):
                                numeric_cell_count += 1
                                
                                # Check if it's a financial-format number (more strict)
                                if financial_number_pattern.search(cell_str):
                                    financial_numeric_count += 1
                        
                        # Check for currency symbols
                        if currency_pattern.search(cell_str):
                            has_currency = True
        
        # Financial statements should have a significant proportion of numeric cells
        if total_cells == 0:
            return False
        
        numeric_ratio = numeric_cell_count / total_cells
        financial_numeric_ratio = financial_numeric_count / total_cells
        long_text_ratio = long_text_cell_count / total_cells
        pure_text_ratio = pure_text_cell_count / total_cells
        
        # Calculate average text length
        avg_text_length = sum(text_cell_lengths) / len(text_cell_lengths) if text_cell_lengths else 0
        
        # Filter out tables that are mostly text (prose/descriptions)
        # Very lenient thresholds - only filter extremely obvious text tables
        # For large tables, be more lenient
        long_text_threshold = 0.90 if is_large_table else 0.85
        if long_text_ratio > long_text_threshold:
            return False
        
        # If more than 99% of cells are pure text (no numbers), filter out
        # For large tables, allow up to 99.5%
        pure_text_threshold = 0.995 if is_large_table else 0.99
        if pure_text_ratio > pure_text_threshold:
            return False
        
        # If average text length is very long (likely sentences), filter out
        # For large tables, be more lenient - only filter if clearly prose
        avg_text_threshold = 80 if is_large_table else 70
        numeric_ratio_threshold = 0.05 if is_large_table else 0.08
        if avg_text_length > avg_text_threshold and numeric_ratio < numeric_ratio_threshold:
            return False
        
        # For large tables, be more lenient with financial number requirements
        min_financial_ratio = 0.01 if is_large_table else 0.02
        min_numeric_ratio_for_check = 0.10 if is_large_table else 0.15
        
        # If there are very few financial-format numbers, likely not a financial statement
        # Financial statements should have many numbers with commas, parentheses, etc.
        # Only filter if extremely few numbers
        if financial_numeric_ratio < min_financial_ratio and numeric_ratio < min_numeric_ratio_for_check:
            return False
        
        # Criteria for financial statement (very lenient requirements):
        # Accept tables with any reasonable structure and some numeric content
        # For large tables, use very lenient thresholds
        if is_large_table:
            # Very lenient criteria for large tables - accept most structured tables
            is_financial = (
                (financial_numeric_ratio >= 0.01 and numeric_ratio >= 0.08 and long_text_ratio < 0.85 and pure_text_ratio < 0.99) or
                (has_currency and numeric_ratio >= 0.05 and long_text_ratio < 0.85 and pure_text_ratio < 0.99) or
                (max_cols >= 3 and numeric_ratio >= 0.05 and long_text_ratio < 0.80 and pure_text_ratio < 0.95) or
                (max_cols >= 2 and numeric_ratio >= 0.08 and long_text_ratio < 0.75 and pure_text_ratio < 0.95) or
                (max_cols >= 1 and numeric_ratio >= 0.10 and long_text_ratio < 0.70 and pure_text_ratio < 0.90) or
                (numeric_ratio >= 0.15)  # If 15%+ cells have numbers, likely a data table
            )
        else:
            # Standard criteria for smaller tables - also very lenient
            is_financial = (
                (financial_numeric_ratio >= 0.02 and numeric_ratio >= 0.10 and long_text_ratio < 0.75 and pure_text_ratio < 0.95) or
                (has_currency and numeric_ratio >= 0.08 and long_text_ratio < 0.75 and pure_text_ratio < 0.95) or
                (max_cols >= 3 and numeric_ratio >= 0.08 and long_text_ratio < 0.70 and pure_text_ratio < 0.90) or
                (max_cols >= 2 and numeric_ratio >= 0.10 and long_text_ratio < 0.65 and pure_text_ratio < 0.90) or
                (numeric_ratio >= 0.20)  # If 20%+ cells have numbers, likely a data table
            )
        
        return is_financial
    
    def _is_valid_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate if a DataFrame is a valid, useful table that should be saved.
        
        Criteria:
        1. Has valid column headers (not all empty)
        2. Has sufficient data rows
        3. Has meaningful structure (not just numbers without context)
        4. Not just text descriptions
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if DataFrame is valid and should be saved, False otherwise
        """
        if df.empty or len(df) == 0:
            return False
        
        # For large tables (15+ rows), be more lenient
        is_large_table = len(df) >= 15
        
        # Check if we have valid column headers
        # At least some columns should have non-empty names
        non_empty_cols = [col for col in df.columns if col and str(col).strip() and str(col).strip() not in ['', '–', '-', '—']]
        if len(non_empty_cols) < 1:
            # Need at least 1 column with name
            return False
        
        # Check if first column has meaningful content (row labels)
        # This is important for financial statements
        first_col_text_count = 0
        first_col_numeric_only_count = 0
        
        if len(df.columns) > 0:
            first_col = df.columns[0]
            
            # Count rows with text in first column (not just numbers or empty)
            for val in df[first_col].dropna():
                val_str = str(val).strip()
                if val_str and val_str not in ['–', '-', '—', '']:
                    # Check if it's text (not just numbers)
                    if not re.match(r'^[\d,，\(\)（\)\s\.]+$', val_str):
                        # Has text content (likely a row label)
                        if len(val_str) > 2:  # Meaningful text (more than 2 chars)
                            first_col_text_count += 1
                    else:
                        # Only numbers
                        first_col_numeric_only_count += 1
            
            # If first column has no text labels and mostly numbers, it's likely not a proper table
            # This catches tables like page258_table1.csv (only numbers, no row labels)
            # But be very lenient - only filter if table is very large and has no structure
            # For large tables, be even more lenient
            min_rows_for_check = 15 if is_large_table else 12
            if first_col_text_count == 0 and first_col_numeric_only_count > 0 and len(df) >= min_rows_for_check:
                # No text labels in first column and has numbers - likely just a data fragment
                # But only filter if table is very large to avoid false positives
                return False
        
        # Check if table is too small (less than 1 data row)
        if len(df) < 1:
            # Very small tables are usually not useful
            return False
        
        # For small tables (1-4 rows), be very lenient
        # Only filter if it's clearly just empty or meaningless
        if len(df) < 5:
            # Allow small tables if they have any structure
            # Only filter if completely no text labels AND no meaningful column names AND no numbers
            if first_col_text_count == 0:
                # Check if columns have meaningful names
                meaningful_cols = [col for col in df.columns if col and str(col).strip() and 
                                  len(str(col).strip()) > 1 and str(col).strip() not in ['–', '-', '—']]
                # Check if there are any numbers in the table
                has_numbers = False
                for col in df.columns:
                    for val in df[col].dropna():
                        if re.search(r'\d', str(val)):
                            has_numbers = True
                            break
                    if has_numbers:
                        break
                
                if len(meaningful_cols) < 1 and not has_numbers:
                    # No text labels, no meaningful column names, and no numbers - likely not useful
                    return False
        
        # Check if table is mostly text descriptions (not data)
        total_cells = len(df) * len(df.columns)
        text_cells = 0
        numeric_cells = 0
        
        # For large tables, be very lenient - they're likely important financial statements
        if is_large_table:
            # Large tables are almost always worth keeping if they passed financial statement check
            # Only filter if they're clearly just text descriptions
            # Count text vs numeric cells
            for _, row in df.iterrows():
                for cell in row:
                    if cell is not None and str(cell).strip():
                        cell_str = str(cell).strip()
                        # Check if it's mostly text (long text without numbers)
                        if len(cell_str) > 15 and not re.search(r'\d{2,}', cell_str):
                            text_cells += 1
                        elif re.search(r'\d', cell_str):
                            numeric_cells += 1
            
            if total_cells > 0:
                text_ratio = text_cells / total_cells
                # Only filter large tables if they're almost entirely long text (95%+) and have very few numbers
                if text_ratio > 0.95 and numeric_cells < total_cells * 0.02:
                    return False
            return True  # Large tables that passed financial check are likely valid
        
        for col in df.columns:
            for val in df[col].dropna():
                val_str = str(val).strip()
                if val_str and val_str not in ['–', '-', '—', '']:
                    # Check if it's a number
                    if re.search(r'\d{1,3}([,，]\d{3})+|\d{4,}|\(\d+\)', val_str):
                        numeric_cells += 1
                    # Check if it's long text (likely description)
                    elif len(val_str) > 30:
                        text_cells += 1
        
        if total_cells > 0:
            text_ratio = text_cells / total_cells
            numeric_ratio = numeric_cells / total_cells
            
            # If more than 90% are long text cells, likely a description table
            if text_ratio > 0.90:
                return False
            
            # If very few numbers and many text, likely not a data table (very lenient)
            # Only filter if clearly prose (extremely high text ratio and extremely low numeric ratio)
            if numeric_ratio < 0.01 and text_ratio > 0.85:
                # Only filter if extremely few numbers AND very high text ratio
                return False
        
        # Check if all rows are just numbers without context (no headers, no labels)
        # This catches tables like page258_table1.csv
        # But be more lenient - only filter obvious cases
        if len(df.columns) > 0:
            # Check if most columns are unnamed or have generic names
            generic_col_names = 0
            for col in df.columns:
                col_str = str(col).strip()
                if not col_str or col_str in ['', '–', '-', '—'] or re.match(r'^[\d\s]+$', col_str):
                    generic_col_names += 1
            
            # If ALL columns are unnamed/generic AND first column has no text labels AND table is large
            # Only filter very obvious cases - be very lenient
            if generic_col_names >= len(df.columns) * 0.95 and first_col_text_count == 0 and len(df) >= 10:
                # Almost all columns unnamed, no text labels, and large table - likely not useful
                # But only filter if table is quite large (10+ rows) and 95%+ columns are generic
                return False
        
        return True
    
    def extract_tables(self, pdf_path: str, pages: Optional[Set[int]] = None,
                      output_dir: str = "output", 
                      output_format: str = "csv") -> List[str]:
        """
        Extract tables from a PDF file
        
        Args:
            pdf_path: Path to the PDF file
            pages: Set of page numbers to extract tables from (1-indexed).
                  If None, extract from all pages.
            output_dir: Directory to save output files
            output_format: Format to save tables (only csv is supported)
            
        Returns:
            List of paths to the output files
        """
        if output_format != "csv":
            raise ValueError("Output format must be 'csv'")
            
        # Get PDF-specific output directories
        pdf_dirs = get_pdf_output_dirs(output_dir, pdf_path)
        # Use csv directory for table files
        output_dir = pdf_dirs['csv']
        pdf_filename = Path(pdf_path).stem
        output_files = []
        
        self.logger.info(f"Extracting tables from {pdf_path}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Determine which pages to process
                if pages:
                    # pdfplumber is 0-indexed, but our interface is 1-indexed
                    page_indices = [p - 1 for p in pages if 0 < p <= len(pdf.pages)]
                    if len(page_indices) < len(pages):
                        self.logger.warning(f"Some requested pages are out of range. PDF has {len(pdf.pages)} pages.")
                    pdf_pages = [pdf.pages[i] for i in page_indices]
                else:
                    pdf_pages = pdf.pages

                # Process each page
                for i, page in enumerate(pdf_pages):
                    page_number = page.page_number + 1  # Convert back to 1-indexed
                    tables = []
                    all_tables = []  # Collect all tables from different strategies
                    
                    # Try multiple extraction strategies to handle different table types
                    # Strategy 0: Use find_tables() which is more powerful for complex tables with borders
                    try:
                        find_tables_result = page.find_tables()
                        if find_tables_result:
                            self.logger.info(f"Found {len(find_tables_result)} table objects using find_tables on page {page_number}")
                            for table_obj in find_tables_result:
                                try:
                                    table_data = table_obj.extract()
                                    if table_data and len(table_data) > 0:
                                        all_tables.append(table_data)
                                        self.logger.debug(f"Extracted table with {len(table_data)} rows using find_tables")
                                except Exception as e:
                                    self.logger.debug(f"Error extracting from find_tables result: {e}")
                    except Exception as e:
                        self.logger.debug(f"Error using find_tables: {e}")
                    
                    # Strategy 1: Default extraction (works best for tables with borders)
                    try:
                        default_tables = page.extract_tables()
                        if default_tables:
                            self.logger.info(f"Found {len(default_tables)} tables using default extract_tables on page {page_number}")
                            all_tables.extend(default_tables)
                    except Exception as e:
                        self.logger.debug(f"Error using default extract_tables: {e}")
                    
                    # Strategy 1.5: Try find_tables with explicit settings for tables with borders
                    try:
                        table_settings = {
                            "vertical_strategy": "lines",  # Use lines for columns
                            "horizontal_strategy": "lines",  # Use lines for rows
                            "snap_tolerance": 5,
                            "join_tolerance": 3,
                            "edge_min_length": 3,
                        }
                        find_tables_result = page.find_tables(table_settings=table_settings)
                        if find_tables_result:
                            self.logger.info(f"Found {len(find_tables_result)} table objects using find_tables with lines strategy on page {page_number}")
                            for table_obj in find_tables_result:
                                try:
                                    table_data = table_obj.extract()
                                    if table_data and len(table_data) > 0:
                                        all_tables.append(table_data)
                                except Exception as e:
                                    self.logger.debug(f"Error extracting from find_tables with lines: {e}")
                    except Exception as e:
                        self.logger.debug(f"Error using find_tables with lines strategy: {e}")
                    
                    # Strategy 2: Try with text-based strategy (for tables without borders)
                    try:
                        table_settings = {
                            "vertical_strategy": "text",  # Use text alignment to detect columns
                            "horizontal_strategy": "text",  # Use text alignment to detect rows
                            "snap_tolerance": 5,  # Tolerance for snapping lines
                            "join_tolerance": 3,  # Tolerance for joining lines
                            "text_tolerance": 3,  # Tolerance for text alignment
                            "intersection_tolerance": 3,  # Tolerance for intersection detection
                        }
                        text_tables = page.extract_tables(table_settings=table_settings)
                        if text_tables:
                            self.logger.info(f"Found {len(text_tables)} tables using text-based strategy on page {page_number}")
                            all_tables.extend(text_tables)
                    except Exception as e:
                        self.logger.debug(f"Error using text-based strategy: {e}")
                    
                    # Strategy 2.5: Try find_tables with mixed strategy (horizontal lines + text for vertical)
                    try:
                        table_settings = {
                            "vertical_strategy": "text",  # Use text alignment for columns
                            "horizontal_strategy": "lines",  # Use horizontal lines for rows
                            "snap_tolerance": 8,
                            "join_tolerance": 5,
                            "text_tolerance": 8,  # More lenient for column detection
                            "intersection_tolerance": 5,
                        }
                        find_tables_result = page.find_tables(table_settings=table_settings)
                        if find_tables_result:
                            self.logger.info(f"Found {len(find_tables_result)} table objects using find_tables with mixed strategy on page {page_number}")
                            for table_obj in find_tables_result:
                                try:
                                    table_data = table_obj.extract()
                                    if table_data and len(table_data) > 0:
                                        all_tables.append(table_data)
                                except Exception as e:
                                    self.logger.debug(f"Error extracting from find_tables with mixed: {e}")
                    except Exception as e:
                        self.logger.debug(f"Error using find_tables with mixed strategy: {e}")
                    
                    # Strategy 3: Try with explicit lines strategy (for tables with partial borders)
                    try:
                        table_settings = {
                            "vertical_strategy": "lines",  # Line detection
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 5,
                            "join_tolerance": 3,
                            "edge_min_length": 3,  # Minimum edge length to consider
                        }
                        lines_tables = page.extract_tables(table_settings=table_settings)
                        if lines_tables:
                            self.logger.info(f"Found {len(lines_tables)} tables using explicit lines strategy on page {page_number}")
                            all_tables.extend(lines_tables)
                    except Exception as e:
                        self.logger.debug(f"Error using explicit lines strategy: {e}")
                    
                    # Strategy 4: Try with mixed strategy (lines for vertical, text for horizontal)
                    try:
                        table_settings = {
                            "vertical_strategy": "lines",  # Use lines for columns
                            "horizontal_strategy": "text",  # Use text alignment for rows
                            "snap_tolerance": 5,
                            "join_tolerance": 3,
                            "text_tolerance": 3,
                            "intersection_tolerance": 3,
                        }
                        mixed_tables = page.extract_tables(table_settings=table_settings)
                        if mixed_tables:
                            self.logger.info(f"Found {len(mixed_tables)} tables using mixed strategy on page {page_number}")
                            all_tables.extend(mixed_tables)
                    except Exception as e:
                        self.logger.debug(f"Error using mixed strategy: {e}")
                    
                    # Strategy 5: Try with very loose text-based strategy
                    try:
                        table_settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text",
                            "snap_tolerance": 10,
                            "join_tolerance": 5,
                            "text_tolerance": 5,  # More lenient text alignment
                            "intersection_tolerance": 5,  # More lenient intersection detection
                            "min_words_vertical": 1,  # Minimum words for vertical alignment
                            "min_words_horizontal": 1,  # Minimum words for horizontal alignment
                        }
                        loose_tables = page.extract_tables(table_settings=table_settings)
                        if loose_tables:
                            self.logger.info(f"Found {len(loose_tables)} tables using loose text strategy on page {page_number}")
                            all_tables.extend(loose_tables)
                    except Exception as e:
                        self.logger.debug(f"Error using loose text strategy: {e}")
                    
                    # Remove duplicate tables and keep best quality version
                    # Use improved deduplication that compares content, not just structure
                    def get_table_signature(table):
                        """Create a signature from table content for comparison"""
                        if not table or len(table) == 0:
                            return None
                        # Extract key content: normalize and compare
                        signature_parts = []
                        for row in table[:min(15, len(table))]:  # Check first 15 rows
                            if row:
                                # Try to find first non-empty column (might not be column 0)
                                first_text_col = None
                                for col_idx, cell in enumerate(row):
                                    if cell and str(cell).strip() and str(cell).strip() not in ['–', '-', '—', '']:
                                        cell_str = str(cell).strip()
                                        # Skip if it's just a number or very short
                                        if len(cell_str) > 2 and not re.match(r'^[\d,，\(\)（\)\s]+$', cell_str):
                                            first_text_col = cell_str[:50]
                                            break
                                
                                # Extract all numbers from row (normalize - combine split numbers)
                                all_cells_text = ' '.join(str(cell) for cell in row if cell)
                                # Find all numbers (including those split across cells)
                                numbers = re.findall(r'\d+', all_cells_text)
                                # Combine consecutive small numbers that might be split (e.g., "95,88" and "8" -> "95888")
                                combined_numbers = []
                                i = 0
                                while i < len(numbers):
                                    num = numbers[i]
                                    # If number is small (1-3 digits) and next number is also small, might be split
                                    if i < len(numbers) - 1 and len(num) <= 3 and len(numbers[i+1]) <= 3:
                                        # Check if they form a larger number when combined
                                        combined = num + numbers[i+1]
                                        if len(combined) >= 4:  # Likely a split number
                                            combined_numbers.append(combined)
                                            i += 2
                                            continue
                                    combined_numbers.append(num)
                                    i += 1
                                
                                if first_text_col or combined_numbers:
                                    signature_parts.append((first_text_col or "", tuple(combined_numbers[:8])))  # More numbers
                        return tuple(signature_parts)
                    
                    def score_table_quality(table):
                        """Score table quality - higher is better"""
                        if not table or len(table) == 0:
                            return -100  # Very bad
                        
                        score = 0
                        max_cols = max(len(row) for row in table) if table else 0
                        total_cells = 0
                        numeric_cells = 0
                        complete_numbers = 0  # Numbers that aren't split
                        split_numbers = 0  # Numbers that appear to be split (1-3 digits)
                        empty_cells = 0
                        very_short_numeric_cells = 0  # Cells with only 1-3 digits (likely split)
                        
                        # Check for reasonable column count (1-15 is acceptable)
                        if 2 <= max_cols <= 10:
                            score += 15
                        elif max_cols == 1 or (max_cols >= 11 and max_cols <= 15):
                            score += 5  # Acceptable
                        elif max_cols > 15:
                            score -= (max_cols - 15) * 2  # Light penalty for too many columns
                        # No penalty for single column tables
                        
                        # Analyze cell content
                        incomplete_number_patterns = 0  # Patterns like "95,88" (incomplete)
                        for row_idx, row in enumerate(table):
                            for col_idx, cell in enumerate(row):
                                if cell:
                                    cell_str = str(cell).strip()
                                    if cell_str and cell_str not in ['–', '-', '—', '']:
                                        total_cells += 1
                                        # Check for incomplete number patterns (e.g., "95,88" without final digits)
                                        # This is a strong indicator of split numbers
                                        if re.search(r'\d{1,2}[,，]\d{1,2}$', cell_str):  # Pattern like "95,88"
                                            incomplete_number_patterns += 1
                                        
                                        # Check for complete numbers (with commas, not split)
                                        if re.search(r'\d{1,3}([,，]\d{3})+', cell_str):
                                            complete_numbers += 1
                                            numeric_cells += 1
                                        elif re.search(r'\d{4,}', cell_str):  # 4+ digit numbers
                                            complete_numbers += 1
                                            numeric_cells += 1
                                        elif re.search(r'\d', cell_str):
                                            numeric_cells += 1
                                            # Check if it's a very short number (likely split)
                                            digits_only = re.sub(r'[^\d]', '', cell_str)
                                            if 1 <= len(digits_only) <= 3:
                                                very_short_numeric_cells += 1
                                                split_numbers += 1
                                        
                                        # Check if adjacent cells might be split numbers
                                        # Look for pattern: cell with "X,YY" followed by cell with single digit
                                        if col_idx < len(row) - 1:
                                            next_cell = row[col_idx + 1] if col_idx + 1 < len(row) else None
                                            if next_cell:
                                                next_str = str(next_cell).strip()
                                                # If current cell has pattern like "95,88" and next is single digit
                                                if (re.search(r'\d{1,2}[,，]\d{1,2}$', cell_str) and 
                                                    re.match(r'^\d{1,2}$', next_str)):
                                                    incomplete_number_patterns += 1
                                    else:
                                        empty_cells += 1
                        
                        # Quality metrics
                        if total_cells > 0:
                            # Very heavy penalty for incomplete number patterns (strong indicator of split table)
                            if incomplete_number_patterns > 0:
                                incomplete_ratio = incomplete_number_patterns / total_cells
                                if incomplete_ratio > 0.05:  # More than 5% incomplete patterns
                                    score -= 50  # Very heavy penalty - almost certainly a split table
                                elif incomplete_ratio > 0.02:  # More than 2% incomplete patterns
                                    score -= 30
                                else:
                                    score -= 10  # Even a few is suspicious
                            
                            # Heavy bonus for complete numbers (not split)
                            if complete_numbers > 0:
                                score += min(30, complete_numbers * 3)  # Increased weight
                            
                            # Heavy penalty for split numbers
                            if split_numbers > 0:
                                split_ratio = split_numbers / total_cells
                                if split_ratio > 0.1:  # More than 10% split numbers
                                    score -= 40  # Increased penalty
                                elif split_ratio > 0.05:  # More than 5% split numbers
                                    score -= 20
                                elif split_ratio > 0.02:  # More than 2% split numbers
                                    score -= 10
                            
                            # Prefer tables with reasonable numeric ratio (very lenient)
                            numeric_ratio = numeric_cells / total_cells if total_cells > 0 else 0
                            if 0.05 <= numeric_ratio <= 0.80:
                                score += 15
                            elif numeric_ratio < 0.05:
                                score -= 5  # Light penalty for very few numbers
                            
                            # Penalize too many empty cells (very lenient)
                            empty_ratio = empty_cells / (total_cells + empty_cells) if (total_cells + empty_cells) > 0 else 0
                            if empty_ratio > 0.8:
                                score -= 10  # Light penalty only for extremely sparse tables
                            elif empty_ratio > 0.6:
                                score -= 5
                        
                        # Prefer tables with more rows (very lenient)
                        row_count = len([r for r in table if any(cell and str(cell).strip() for cell in r)])
                        if 2 <= row_count <= 200:
                            score += 10
                        elif row_count == 1:
                            score += 5  # Even single row tables get some points
                        # No penalty for any row count
                        
                        # Additional penalty for tables with many very short numeric cells (indicates splitting)
                        if very_short_numeric_cells > complete_numbers * 2:
                            score -= 30  # Increased penalty - likely a split table
                        
                        # Additional check: if we have incomplete patterns, this is almost certainly a bad table
                        if incomplete_number_patterns >= 3:
                            score -= 20  # Additional penalty for multiple incomplete patterns
                        
                        return score
                    
                    # Group tables by signature and keep best quality version
                    table_groups = {}
                    for table in all_tables:
                        if not table or len(table) == 0:
                            continue
                        signature = get_table_signature(table)
                        if signature:
                            if signature not in table_groups:
                                table_groups[signature] = []
                            table_groups[signature].append(table)
                    
                    # For each group, keep the table with highest quality score
                    # Also filter out very low quality tables
                    unique_tables = []
                    min_quality_score = -20  # Very low minimum quality threshold to accept more tables
                    
                    for signature, group in table_groups.items():
                        if len(group) == 1:
                            # Even single tables need to meet quality threshold
                            score = score_table_quality(group[0])
                            if score >= min_quality_score:
                                unique_tables.append(group[0])
                            else:
                                self.logger.debug(f"Filtered out low quality table (score: {score})")
                        else:
                            # Multiple tables with same signature - keep best quality
                            scored = [(score_table_quality(t), t) for t in group]
                            scored.sort(reverse=True, key=lambda x: x[0])
                            best_score, best_table = scored[0]
                            
                            if best_score >= min_quality_score:
                                unique_tables.append(best_table)
                                if len(group) > 1:
                                    self.logger.info(f"Found {len(group)} duplicate tables on page {page_number}, keeping best quality version (score: {best_score})")
                            else:
                                self.logger.debug(f"Filtered out all {len(group)} duplicate tables (best score: {best_score} < {min_quality_score})")
                    
                    tables = unique_tables
                    
                    if not tables:
                        self.logger.info(f"No tables found on page {page_number} after trying all strategies")
                        continue
                        
                    self.logger.info(f"Found {len(tables)} tables on page {page_number}")
                    
                    # Process each table
                    for table_idx, table_data in enumerate(tables):
                        # Convert table data to DataFrame
                        # First row is header if it has different formatting
                        if not table_data:
                            continue
                            
                        # Filter out empty rows, but preserve structure
                        # Important: Keep rows even if only first column has content (row labels)
                        filtered_table = []
                        
                        # Find maximum column count to ensure consistent row lengths
                        max_cols = max(len(row) for row in table_data) if table_data else 0
                        
                        for row in table_data:
                            # Check if row has any non-empty content
                            # For financial statements, first column often contains row labels
                            # So we should keep rows that have content in any column
                            if any(cell and str(cell).strip() for cell in row):
                                # Ensure row has consistent length (pad with empty strings if needed)
                                # This helps preserve column structure, especially the first column
                                padded_row = list(row) + [''] * (max_cols - len(row))
                                filtered_table.append(padded_row)
                        
                        if not filtered_table or len(filtered_table) < 1:
                            self.logger.debug(f"Skipping table {table_idx+1} on page {page_number}: too few rows")
                            continue
                        
                        # Check if this is a financial statement table
                        if not self._is_financial_statement_table(filtered_table):
                            self.logger.debug(f"Skipping table {table_idx+1} on page {page_number}: not a financial statement table")
                            continue
                        
                        self.logger.info(f"Keeping table {table_idx+1} on page {page_number}: identified as financial statement")
                        
                        # Use first row as header if available
                        if len(filtered_table) > 0:
                            header = filtered_table[0]
                            data_rows = filtered_table[1:]
                            
                            # Create DataFrame
                            try:
                                df = pd.DataFrame(data_rows, columns=header)
                                
                                # Preserve first column (usually row labels/descriptions) even if partially empty
                                # Remove completely empty columns, but always keep the first column
                                first_col_name = df.columns[0] if len(df.columns) > 0 else None
                                
                                # Remove completely empty columns (except first column)
                                cols_to_drop = []
                                for col in df.columns:
                                    # Skip first column - always keep it
                                    if col == first_col_name:
                                        continue
                                    
                                    try:
                                        # Check if column is completely empty
                                        col_series = df[col]
                                        # 确保是Series对象
                                        if isinstance(col_series, pd.Series):
                                            is_all_na = col_series.isna().all()
                                            is_all_empty = (col_series.astype(str).str.strip() == '').all()
                                            if is_all_na or is_all_empty:
                                                cols_to_drop.append(col)
                                    except Exception as e:
                                        # 如果处理列时出错，跳过该列
                                        self.logger.debug(f"Error checking column {col}: {e}")
                                        continue
                                
                                if cols_to_drop:
                                    df = df.drop(columns=cols_to_drop)
                                
                                # Ensure first column is preserved and has a proper name
                                if len(df.columns) > 0 and first_col_name:
                                    # If first column name is empty or None, give it a default name
                                    if not first_col_name or str(first_col_name).strip() == '':
                                        df.columns.values[0] = '项目'  # Default name for first column
                                
                                # Skip if DataFrame is empty or too small
                                if df.empty or len(df) == 0:
                                    self.logger.warning(f"Skipping table {table_idx+1} on page {page_number}: empty DataFrame")
                                    continue
                                
                                # Validate DataFrame quality before saving
                                if not self._is_valid_dataframe(df):
                                    self.logger.debug(f"Skipping table {table_idx+1} on page {page_number}: low quality DataFrame (missing headers/labels or mostly text)")
                                    continue
                                
                            except Exception as e:
                                self.logger.warning(f"Error creating DataFrame for table {table_idx+1} on page {page_number}: {e}")
                                continue
                        else:
                            continue
                        
                        # Generate output filename
                        output_basename = f"{sanitize_filename(pdf_filename)}_page{page_number}_table{table_idx+1}"
                        
                        # Save table as CSV
                        output_file = os.path.join(output_dir, f"{output_basename}.csv")
                        # Use UTF-8 encoding with BOM for better Excel compatibility with Chinese
                        df.to_csv(output_file, index=False, encoding='utf-8-sig')
                        output_files.append(output_file)
                        self.logger.info(f"Table saved to {output_file}")
                
                # Secondary filtering: remove files with only one column or high text content
                if output_files:
                    original_count = len(output_files)
                    filtered_files = []
                    removed_count = 0
                    single_col_removed = 0
                    text_heavy_removed = 0
                    missing_headers_removed = 0
                    too_small_removed = 0
                    no_headers_removed = 0
                    contact_info_removed = 0
                    small_poor_structure_removed = 0
                    
                    for output_file in output_files:
                        try:
                            should_remove = False
                            remove_reason = ""
                            
                            # Read CSV file to check column count and text content
                            df_check = pd.read_csv(output_file, encoding='utf-8-sig')
                            
                            # Check 1: Only one column
                            if len(df_check.columns) <= 1:
                                should_remove = True
                                remove_reason = "single-column"
                            
                            # Check 1.5: Very small tables (too few rows or columns)
                            if not should_remove:
                                rows = len(df_check)
                                cols = len(df_check.columns)
                                # Remove tables with very few rows (less than 3) or very few columns (less than 2)
                                if rows < 3 or cols < 2:
                                    should_remove = True
                                    remove_reason = f"too-small (rows: {rows}, cols: {cols})"
                            
                            # Check 2: Missing or invalid column headers (like page179_table1.csv, page185_table1.csv, page232_table1.csv)
                            if not should_remove:
                                # Check if column names are mostly empty, numeric, or special characters
                                # This indicates the first row might be data instead of headers
                                invalid_col_count = 0
                                unnamed_col_count = 0  # Count columns with "Unnamed" pattern (pandas default for no headers)
                                
                                for col in df_check.columns:
                                    col_str = str(col).strip()
                                    # Check if column name matches pandas "Unnamed" pattern (e.g., "Unnamed: 0", "Unnamed: 1")
                                    if re.match(r'^Unnamed:\s*\d+$', col_str, re.IGNORECASE):
                                        unnamed_col_count += 1
                                        invalid_col_count += 1
                                    # Check if column name is empty, just numbers, or only special characters
                                    elif (not col_str or 
                                          col_str in ['–', '-', '—', ''] or
                                          re.match(r'^[\d,，\(\)（\)\s\.]+$', col_str) or
                                          re.match(r'^[^\w\u4e00-\u9fff]+$', col_str)):  # Only special chars, no Chinese/English letters
                                        invalid_col_count += 1
                                
                                # If ALL columns are Unnamed, definitely no headers (like page179, page185, page232)
                                if len(df_check.columns) > 0 and unnamed_col_count == len(df_check.columns):
                                    should_remove = True
                                    remove_reason = f"no-headers-all-unnamed ({unnamed_col_count} columns)"
                                
                                # If most columns have invalid names, likely no proper headers
                                elif len(df_check.columns) > 0:
                                    invalid_col_ratio = invalid_col_count / len(df_check.columns)
                                    if invalid_col_ratio >= 0.7:  # 70%+ columns have invalid names
                                        # Additional check: count empty columns (columns with mostly empty cells)
                                        empty_col_count = 0
                                        for col in df_check.columns:
                                            non_empty_in_col = df_check[col].notna().sum()
                                            if non_empty_in_col == 0 or (non_empty_in_col / len(df_check)) < 0.1:
                                                empty_col_count += 1
                                        
                                        empty_col_ratio = empty_col_count / len(df_check.columns) if len(df_check.columns) > 0 else 0
                                        
                                        # Additional check: if first column is also all numeric (no row labels)
                                        first_col_all_numeric = False
                                        if len(df_check.columns) > 0:
                                            first_col = df_check.columns[0]
                                            first_col_numeric_count = 0
                                            first_col_total = 0
                                            for val in df_check[first_col].dropna():
                                                val_str = str(val).strip()
                                                if val_str and val_str not in ['–', '-', '—', '']:
                                                    first_col_total += 1
                                                    # Check if value is purely numeric (with commas, parentheses, etc.)
                                                    if re.match(r'^[\d,，\(\)（\)\s\.\-–—]+$', val_str):
                                                        first_col_numeric_count += 1
                                            
                                            # If first column has values and all are numeric, likely no row labels
                                            if first_col_total > 0 and first_col_numeric_count == first_col_total:
                                                first_col_all_numeric = True
                                        
                                        # If many columns are empty AND column names are invalid, likely a data fragment
                                        # OR if first column is all numeric (no row labels), likely a data fragment
                                        if empty_col_ratio >= 0.4 or (first_col_all_numeric and invalid_col_ratio >= 0.8):
                                            should_remove = True
                                            remove_reason = f"missing-headers-sparse-data (invalid-headers: {invalid_col_ratio:.1%}, empty-cols: {empty_col_ratio:.1%}, first-col-numeric: {first_col_all_numeric})"
                            
                            # Check 2.5: Contact information pattern (like page275_table1.csv)
                            if not should_remove:
                                # Detect contact information patterns (phone, fax, address, email, etc.)
                                contact_patterns = [
                                    r'電話[:：]', r'傳真[:：]', r'電話[:：]', r'传真[:：]',  # Phone/Fax
                                    r'郵編[:：]', r'邮编[:：]',  # Postal code
                                    r'網址[:：]', r'网址[:：]', r'www\.',  # Website
                                    r'@',  # Email
                                    r'地址[:：]', r'地址',  # Address
                                    r'辦事處', r'办事处',  # Office
                                ]
                                
                                contact_cell_count = 0
                                total_cells_for_contact = 0
                                
                                for _, row in df_check.iterrows():
                                    for cell in row:
                                        if pd.notna(cell):
                                            cell_str = str(cell).strip()
                                            if cell_str and cell_str not in ['–', '-', '—', '']:
                                                total_cells_for_contact += 1
                                                # Check if cell contains contact information patterns
                                                for pattern in contact_patterns:
                                                    if re.search(pattern, cell_str, re.IGNORECASE):
                                                        contact_cell_count += 1
                                                        break
                                
                                # If more than 40% of cells contain contact information, likely a contact info table
                                if total_cells_for_contact > 0:
                                    contact_ratio = contact_cell_count / total_cells_for_contact
                                    if contact_ratio > 0.40:
                                        should_remove = True
                                        remove_reason = f"contact-information (contact-ratio: {contact_ratio:.1%})"
                            
                            # Check 3: Text content ratio and text characteristics (if not already marked for removal)
                            if not should_remove:
                                total_cells = 0
                                text_cells = 0  # Cells without any digits
                                long_text_cells = 0  # Cells with long text (>30 chars)
                                sentence_like_cells = 0  # Cells that look like sentences
                                text_lengths = []  # Track text cell lengths
                                first_col_long_text_count = 0  # Count long text in first column
                                first_col_total = 0  # Total cells in first column
                                first_col_text_count = 0  # Count text cells in first column
                                first_col_special_char_count = 0  # Count special char cells in first column (like '-')
                                first_col_long_text_rows = 0  # Count rows with long text in first column
                                first_col_text_rows = 0  # Count rows with text (no 3+ digit numbers) in first column
                                fragmented_text_count = 0  # Count fragmented text cells (like page25_table2.csv)
                                incomplete_text_count = 0  # Count cells with incomplete text (ends with comma, no punctuation)
                                very_short_text_count = 0  # Count very short text cells (likely fragments)
                                descriptive_row_count = 0  # Count rows that are mainly descriptive text
                                total_rows = len(df_check)
                                
                                # Pattern to detect sentence-like text (contains punctuation, connectors)
                                sentence_pattern = re.compile(r'[。，、；：！？,\.;:!?].{3,}|[的之了在是].{2,}')
                                
                                # First pass: Check if first column is all special chars (like page242_table3.csv)
                                # This must be done before excluding these cells
                                first_col_all_special_check = True
                                first_col_non_empty_count = 0
                                for row_idx, (_, row) in enumerate(df_check.iterrows()):
                                    if len(row) > 0:
                                        first_cell = row.iloc[0] if hasattr(row, 'iloc') else row[0]
                                        if pd.notna(first_cell):
                                            first_cell_str = str(first_cell).strip()
                                            if first_cell_str:
                                                first_col_non_empty_count += 1
                                                # Check if it's special char only
                                                if not re.match(r'^[–\-\—\s]+$', first_cell_str):
                                                    first_col_all_special_check = False
                                
                                # If first column has non-empty cells and all are special chars, mark for removal
                                if first_col_non_empty_count > 0 and first_col_all_special_check:
                                    should_remove = True
                                    remove_reason = f"first-column-all-special-chars ({first_col_non_empty_count} cells)"
                                
                                # Second pass: Analyze all cells
                                if not should_remove:
                                    for row_idx, (_, row) in enumerate(df_check.iterrows()):
                                        row_text_cells = 0
                                        row_total_cells = 0
                                        row_first_col_long_text = False
                                        row_first_col_text = False
                                        
                                        for col_idx, cell in enumerate(row):
                                            if pd.notna(cell):
                                                cell_str = str(cell).strip()
                                                if cell_str and cell_str not in ['–', '-', '—', '']:
                                                    total_cells += 1
                                                    row_total_cells += 1
                                                    
                                                    # Track first column separately
                                                    if col_idx == 0:
                                                        first_col_total += 1
                                                        # Check if first column has long text
                                                        if len(cell_str) > 30:
                                                            first_col_long_text_count += 1
                                                            row_first_col_long_text = True
                                                        # Check if first column is text (no 3+ digit numbers)
                                                        if not re.search(r'\d{3,}', cell_str):  # No 3+ digit numbers
                                                            first_col_text_count += 1
                                                            row_first_col_text = True
                                        
                                                    # Check if cell contains any digits
                                                    has_digits = bool(re.search(r'\d', cell_str))
                                                    
                                                    if not has_digits:
                                                        text_cells += 1
                                                        row_text_cells += 1
                                                        text_lengths.append(len(cell_str))
                                                        
                                                        # Check for long text cells (likely prose/descriptions)
                                                        if len(cell_str) > 30:
                                                            long_text_cells += 1
                                                        
                                                        # Check if cell looks like a sentence
                                                        if sentence_pattern.search(cell_str):
                                                            sentence_like_cells += 1
                                                        
                                                        # Check for fragmented text patterns (incomplete sentences, broken lines)
                                                        # Pattern: text that looks like it was split across cells
                                                        if (len(cell_str) > 20 and 
                                                            not re.search(r'\d{3,}', cell_str) and  # Not a number
                                                            (re.search(r'[，,]\s*$', cell_str) or  # Ends with comma
                                                             (re.search(r'^[^。！？\.!?]+$', cell_str) and len(cell_str) > 30))):  # Long text without sentence ending
                                                            fragmented_text_count += 1
                                                        
                                                        # Check for incomplete text (ends with comma or no ending punctuation)
                                                        if (not has_digits and 
                                                            len(cell_str) > 10 and 
                                                            (re.search(r'[，,]\s*$', cell_str) or  # Ends with comma
                                                             (not re.search(r'[。！？\.!?]$', cell_str) and len(cell_str) > 15))):  # No ending punctuation
                                                            incomplete_text_count += 1
                                                        
                                                        # Check for very short text cells (likely fragments, like page193_table4.csv)
                                                        if (not has_digits and 
                                                            len(cell_str) < 10 and 
                                                            len(cell_str) > 2 and
                                                            not re.match(r'^[\d\s\-–—]+$', cell_str)):  # Not just numbers/spaces
                                                            very_short_text_count += 1
                                        
                                        # Track first column row statistics
                                        if row_first_col_long_text:
                                            first_col_long_text_rows += 1
                                        if row_first_col_text:
                                            first_col_text_rows += 1
                                        
                                        # Check if this row is mainly descriptive text (first column has long text, other columns mostly empty)
                                        if row_total_cells > 0:
                                            row_text_ratio = row_text_cells / row_total_cells
                                            # If first column has long text and row is mostly text, likely descriptive
                                            if row_first_col_long_text and row_text_ratio > 0.6:
                                                descriptive_row_count += 1
                                
                                if total_cells > 0:
                                    text_ratio = text_cells / total_cells
                                    long_text_ratio = long_text_cells / total_cells if total_cells > 0 else 0
                                    sentence_ratio = sentence_like_cells / total_cells if total_cells > 0 else 0
                                    
                                    # Calculate average text length
                                    avg_text_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                                    
                                    # Check first column text ratio (like page214_table2.csv, page186_table2.csv, page152_table3.csv)
                                    first_col_text_ratio = first_col_long_text_count / first_col_total if first_col_total > 0 else 0
                                    first_col_text_only_ratio = first_col_text_count / first_col_total if first_col_total > 0 else 0
                                    
                                    # Check first column row ratios (more accurate for detecting descriptive tables)
                                    first_col_long_text_row_ratio = first_col_long_text_rows / total_rows if total_rows > 0 else 0
                                    first_col_text_row_ratio = first_col_text_rows / total_rows if total_rows > 0 else 0
                                    
                                    # Calculate fragmented text ratio
                                    fragmented_text_ratio = fragmented_text_count / total_cells if total_cells > 0 else 0
                                    incomplete_text_ratio = incomplete_text_count / total_cells if total_cells > 0 else 0
                                    very_short_text_ratio = very_short_text_count / total_cells if total_cells > 0 else 0
                                    
                                    # Check for chaotic structure (like page193_table4.csv, page243_table4.csv)
                                    # Many cells with incomplete text or very short fragments
                                    chaotic_structure = (incomplete_text_ratio > 0.25 or 
                                                        (very_short_text_ratio > 0.30 and fragmented_text_ratio > 0.15))
                                    
                                    # Check for descriptive text table (like page186_table2.csv, page152_table3.csv, page204_table2.csv, page202_table3.csv, page248_table2.csv)
                                    # Most rows are descriptive text (first column has long text, row is mostly text)
                                    descriptive_row_ratio = descriptive_row_count / total_rows if total_rows > 0 else 0
                                    
                                    # Check if table is mainly descriptive (first column mostly text, many descriptive rows)
                                    # More lenient: if 30%+ rows have long text in first column, or 50%+ rows have text in first column with 30%+ descriptive rows
                                    mainly_descriptive = (
                                        (first_col_long_text_row_ratio > 0.30 and first_col_total >= 3) or  # 30%+ rows have long text in first column
                                        (first_col_text_row_ratio > 0.50 and descriptive_row_ratio > 0.30 and first_col_total >= 3) or  # 50%+ rows have text, 30%+ descriptive
                                        (first_col_text_only_ratio > 0.60 and descriptive_row_ratio > 0.25 and first_col_total >= 3)  # 60%+ first col cells are text, 25%+ descriptive rows
                                    )
                                    
                                    # Remove if:
                                    # 1. More than 65% text content (lowered from 70%), OR
                                    # 2. More than 45% long text cells (lowered from 50%), OR
                                    # 3. More than 35% sentence-like cells AND average text length > 18 (lowered from 40% and 20), OR
                                    # 4. First column has more than 30% long text rows (new, more accurate), OR
                                    # 5. First column has more than 35% long text cells (lowered from 40%), OR
                                    # 6. More than 30% sentence-like cells (lowered from 35%), OR
                                    # 7. More than 25% fragmented text (lowered from 30%), OR
                                    # 8. Chaotic structure detected (many incomplete or very short text fragments), OR
                                    # 9. Table is mainly descriptive (first column mostly text, many descriptive rows)
                                    if (text_ratio > 0.65 or  # Lowered from 0.70
                                        long_text_ratio > 0.45 or  # Lowered from 0.50
                                        (sentence_ratio > 0.35 and avg_text_length > 18) or  # Lowered from 0.40 and 20
                                        (first_col_long_text_row_ratio > 0.30 and first_col_total >= 3) or  # New: 30%+ rows with long text in first column
                                        (first_col_text_ratio > 0.35 and first_col_total >= 3) or  # Lowered from 0.40
                                        (sentence_ratio > 0.30) or  # Lowered from 0.35
                                        (fragmented_text_ratio > 0.25) or  # Lowered from 0.30
                                        chaotic_structure or  # Chaotic structure
                                        mainly_descriptive):  # Mainly descriptive text table
                                        should_remove = True
                                        remove_reason = f"text-heavy (text: {text_ratio:.1%}, long-text: {long_text_ratio:.1%}, sentences: {sentence_ratio:.1%}, first-col-long-text-rows: {first_col_long_text_row_ratio:.1%}, first-col-long-text-cells: {first_col_text_ratio:.1%}, first-col-text-only: {first_col_text_only_ratio:.1%}, descriptive-rows: {descriptive_row_ratio:.1%}, fragmented: {fragmented_text_ratio:.1%}, incomplete: {incomplete_text_ratio:.1%}, chaotic: {chaotic_structure}, mainly-descriptive: {mainly_descriptive})"
                            
                            # Check 3.5: Small table with poor structure (like page189_table3.csv)
                            if not should_remove:
                                rows = len(df_check)
                                cols = len(df_check.columns)
                                
                                # For small tables (less than 8 rows), check if structure is poor
                                if rows < 8:
                                    # Count invalid column names
                                    invalid_cols = sum(1 for col in df_check.columns 
                                                      if not str(col).strip() or 
                                                      str(col).strip() in ['–', '-', '—', ''] or
                                                      re.match(r'^Unnamed:\s*\d+$', str(col), re.IGNORECASE))
                                    
                                    # Count empty or mostly empty columns
                                    empty_cols = 0
                                    for col in df_check.columns:
                                        non_empty = df_check[col].notna().sum()
                                        if non_empty == 0 or (non_empty / rows) < 0.3:
                                            empty_cols += 1
                                    
                                    # If small table has many invalid/empty columns, likely incomplete
                                    if cols > 0:
                                        invalid_col_ratio = invalid_cols / cols
                                        empty_col_ratio = empty_cols / cols
                                        
                                        # Small table with poor structure (many invalid/empty columns)
                                        if (invalid_col_ratio >= 0.5 and empty_col_ratio >= 0.4) or \
                                           (invalid_col_ratio >= 0.7):
                                            should_remove = True
                                            remove_reason = f"small-poor-structure (rows: {rows}, invalid-cols: {invalid_col_ratio:.1%}, empty-cols: {empty_col_ratio:.1%})"
                            
                            if should_remove:
                                # Delete the file
                                if os.path.exists(output_file):
                                    os.remove(output_file)
                                    removed_count += 1
                                    if "single-column" in remove_reason:
                                        single_col_removed += 1
                                    if "text-heavy" in remove_reason or "high-text-content" in remove_reason:
                                        text_heavy_removed += 1
                                    if "missing-headers-sparse-data" in remove_reason:
                                        missing_headers_removed += 1
                                    if "too-small" in remove_reason:
                                        too_small_removed += 1
                                    if "no-headers-all-unnamed" in remove_reason:
                                        no_headers_removed += 1
                                    if "contact-information" in remove_reason:
                                        contact_info_removed += 1
                                    if "small-poor-structure" in remove_reason:
                                        small_poor_structure_removed += 1
                                    self.logger.info(f"Removed {remove_reason} file: {output_file}")
                            else:
                                # Keep the file
                                filtered_files.append(output_file)
                                
                        except Exception as e:
                            # If we can't read the file, keep it to be safe
                            self.logger.warning(f"Error checking file {output_file} for secondary filtering: {e}")
                            filtered_files.append(output_file)
                    
                    output_files = filtered_files
                    if removed_count > 0:
                        details = []
                        if single_col_removed > 0:
                            details.append(f"{single_col_removed} single-column")
                        if text_heavy_removed > 0:
                            details.append(f"{text_heavy_removed} text-heavy")
                        if missing_headers_removed > 0:
                            details.append(f"{missing_headers_removed} missing-headers-sparse-data")
                        if too_small_removed > 0:
                            details.append(f"{too_small_removed} too-small")
                        if no_headers_removed > 0:
                            details.append(f"{no_headers_removed} no-headers-all-unnamed")
                        if contact_info_removed > 0:
                            details.append(f"{contact_info_removed} contact-information")
                        if small_poor_structure_removed > 0:
                            details.append(f"{small_poor_structure_removed} small-poor-structure")
                        detail_str = ", ".join(details) if details else f"{removed_count}"
                        self.logger.info(f"Secondary filtering completed: {detail_str} file(s) removed, {len(output_files)} file(s) remaining")
                    
                    # Step 3: Keep only the file with most information for each page
                    if output_files:
                        # Group files by page number
                        files_by_page = {}
                        ungrouped_files = []  # Files that can't be grouped by page
                        
                        for output_file in output_files:
                            # Extract page number from filename (format: filename_pageX_tableY.csv)
                            filename = os.path.basename(output_file)
                            # Match pattern like "page123" or "page12"
                            page_match = re.search(r'page(\d+)', filename)
                            if page_match:
                                page_num = int(page_match.group(1))
                                if page_num not in files_by_page:
                                    files_by_page[page_num] = []
                                files_by_page[page_num].append(output_file)
                            else:
                                # If we can't extract page number, keep it separately
                                self.logger.warning(f"Could not extract page number from filename: {filename}")
                                ungrouped_files.append(output_file)
                        
                        # For each page, keep only the file with most structured table
                        final_files = []
                        duplicate_removed = 0
                        
                        # Add ungrouped files (files without page number) to final list
                        final_files.extend(ungrouped_files)
                        
                        def calculate_structure_score(df: pd.DataFrame) -> float:
                            """
                            Calculate structure score for a DataFrame - higher score means more structured/neat table
                            
                            Criteria:
                            1. Column consistency: all rows should have same number of columns (DataFrame ensures this)
                            2. Empty cell distribution: empty cells should be minimal and evenly distributed
                            3. Column alignment: each column should have reasonable content (not all empty)
                            4. Row structure: rows should have reasonable structure
                            5. Cell content consistency: similar content types in same column
                            
                            Returns:
                                Structure score (higher is better)
                            """
                            if df.empty or len(df) == 0:
                                return 0.0
                            
                            rows = len(df)
                            cols = len(df.columns)
                            
                            if rows == 0 or cols == 0:
                                return 0.0
                            
                            score = 0.0
                            total_cells = rows * cols
                            
                            # 1. Count non-empty cells and calculate density
                            non_empty_cells = 0
                            empty_cells = 0
                            column_non_empty_counts = [0] * cols  # Count non-empty cells per column
                            row_non_empty_counts = [0] * rows     # Count non-empty cells per row
                            
                            for row_idx, (_, row) in enumerate(df.iterrows()):
                                for col_idx, cell in enumerate(row):
                                    if pd.notna(cell):
                                        cell_str = str(cell).strip()
                                        if cell_str and cell_str not in ['–', '-', '—', '']:
                                            non_empty_cells += 1
                                            column_non_empty_counts[col_idx] += 1
                                            row_non_empty_counts[row_idx] += 1
                                        else:
                                            empty_cells += 1
                                    else:
                                        empty_cells += 1
                            
                            # 2. Score based on non-empty cell ratio (prefer tables with more content)
                            if total_cells > 0:
                                non_empty_ratio = non_empty_cells / total_cells
                                # Bonus for higher non-empty ratio, but not too high (some empty cells are normal)
                                if 0.3 <= non_empty_ratio <= 0.9:
                                    score += 30.0  # Optimal range
                                elif 0.2 <= non_empty_ratio < 0.3 or 0.9 < non_empty_ratio <= 0.95:
                                    score += 20.0  # Acceptable range
                                elif 0.1 <= non_empty_ratio < 0.2 or 0.95 < non_empty_ratio <= 0.98:
                                    score += 10.0  # Less ideal but acceptable
                                elif non_empty_ratio < 0.1:
                                    score -= 20.0  # Too sparse
                                elif non_empty_ratio > 0.98:
                                    score -= 10.0  # Too dense (might be merged cells issue)
                            
                            # 3. Score based on column consistency (prefer columns with consistent content)
                            # Check if columns have reasonable content distribution
                            non_empty_columns = sum(1 for count in column_non_empty_counts if count > 0)
                            if cols > 0:
                                column_coverage = non_empty_columns / cols
                                # Prefer tables where most columns have content
                                if column_coverage >= 0.8:
                                    score += 25.0
                                elif column_coverage >= 0.6:
                                    score += 15.0
                                elif column_coverage >= 0.4:
                                    score += 5.0
                                else:
                                    score -= 10.0  # Too many empty columns
                            
                            # 4. Score based on row consistency (prefer rows with consistent content)
                            non_empty_rows = sum(1 for count in row_non_empty_counts if count > 0)
                            if rows > 0:
                                row_coverage = non_empty_rows / rows
                                # Prefer tables where most rows have content
                                if row_coverage >= 0.8:
                                    score += 25.0
                                elif row_coverage >= 0.6:
                                    score += 15.0
                                elif row_coverage >= 0.4:
                                    score += 5.0
                                else:
                                    score -= 10.0  # Too many empty rows
                            
                            # 5. Score based on column uniformity (prefer columns with similar non-empty cell counts)
                            if cols > 1 and non_empty_columns > 1:
                                # Calculate variance in column non-empty counts
                                avg_col_count = sum(column_non_empty_counts) / cols
                                if avg_col_count > 0:
                                    variance = sum((count - avg_col_count) ** 2 for count in column_non_empty_counts) / cols
                                    std_dev = variance ** 0.5
                                    coefficient_of_variation = std_dev / avg_col_count if avg_col_count > 0 else 1.0
                                    # Lower CV means more uniform columns (better structure)
                                    if coefficient_of_variation < 0.3:
                                        score += 15.0  # Very uniform
                                    elif coefficient_of_variation < 0.5:
                                        score += 10.0  # Fairly uniform
                                    elif coefficient_of_variation < 0.7:
                                        score += 5.0   # Acceptable
                                    else:
                                        score -= 5.0   # Inconsistent columns
                            
                            # 6. Score based on row uniformity (prefer rows with similar non-empty cell counts)
                            if rows > 1 and non_empty_rows > 1:
                                # Calculate variance in row non-empty counts
                                avg_row_count = sum(row_non_empty_counts) / rows
                                if avg_row_count > 0:
                                    variance = sum((count - avg_row_count) ** 2 for count in row_non_empty_counts) / rows
                                    std_dev = variance ** 0.5
                                    coefficient_of_variation = std_dev / avg_row_count if avg_row_count > 0 else 1.0
                                    # Lower CV means more uniform rows (better structure)
                                    if coefficient_of_variation < 0.3:
                                        score += 15.0  # Very uniform
                                    elif coefficient_of_variation < 0.5:
                                        score += 10.0  # Fairly uniform
                                    elif coefficient_of_variation < 0.7:
                                        score += 5.0   # Acceptable
                                    else:
                                        score -= 5.0   # Inconsistent rows
                            
                            # 7. Bonus for reasonable table size (not too small, not too large)
                            if 3 <= rows <= 100 and 2 <= cols <= 15:
                                score += 10.0  # Optimal size
                            elif 2 <= rows <= 200 and 2 <= cols <= 20:
                                score += 5.0   # Acceptable size
                            
                            # 8. Check first column (usually contains row labels) - should have good content
                            if cols > 0:
                                first_col_non_empty = column_non_empty_counts[0]
                                if rows > 0:
                                    first_col_ratio = first_col_non_empty / rows
                                    if first_col_ratio >= 0.7:
                                        score += 10.0  # Good first column (row labels)
                                    elif first_col_ratio >= 0.5:
                                        score += 5.0
                                    elif first_col_ratio < 0.2:
                                        score -= 5.0   # Poor first column
                            
                            return score
                        
                        for page_num, page_files in files_by_page.items():
                            if len(page_files) == 1:
                                # Only one file for this page, keep it
                                final_files.append(page_files[0])
                            else:
                                # Multiple files for this page, find the one with best structure
                                best_file = None
                                best_structure_score = float('-inf')
                                
                                for file_path in page_files:
                                    try:
                                        structure_score = 0.0
                                        
                                        # Read CSV file and calculate structure score
                                        df_info = pd.read_csv(file_path, encoding='utf-8-sig')
                                        structure_score = calculate_structure_score(df_info)
                                        
                                        if structure_score > best_structure_score:
                                            best_structure_score = structure_score
                                            best_file = file_path
                                            
                                    except Exception as e:
                                        self.logger.warning(f"Error calculating structure score for {file_path}: {e}")
                                        # If we can't calculate score, keep the first file we encountered
                                        if best_file is None:
                                            best_file = file_path
                                
                                if best_file:
                                    final_files.append(best_file)
                                    # Remove other files from the same page
                                    for file_path in page_files:
                                        if file_path != best_file:
                                            try:
                                                if os.path.exists(file_path):
                                                    os.remove(file_path)
                                                    duplicate_removed += 1
                                                    self.logger.info(f"Removed duplicate file from page {page_num}: {os.path.basename(file_path)} (kept: {os.path.basename(best_file)}, structure score: {best_structure_score:.2f})")
                                            except Exception as e:
                                                self.logger.warning(f"Error removing duplicate file {file_path}: {e}")
                        
                        output_files = final_files
                        if duplicate_removed > 0:
                            self.logger.info(f"Structure-based filtering completed: {duplicate_removed} duplicate file(s) removed, {len(output_files)} file(s) remaining")
                
                if not output_files:
                    self.logger.info("No tables were found in the PDF after filtering")
                    
                return output_files
                
        except Exception as e:
            self.logger.error(f"Error extracting tables: {e}")
            raise 