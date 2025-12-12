"""
Utility functions for the PDF Extractor CLI
"""
import os
import logging
import re
from pathlib import Path
from typing import List, Tuple, Set


def setup_logging(log_level: int = logging.INFO) -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        log_level: The logging level to use
        
    Returns:
        A configured logger object
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("pdf_extractor")
    return logger


def ensure_output_dir(dirname: str = "output") -> str:
    """
    Ensure the output directory exists, creating it if necessary
    
    Args:
        dirname: Name of the directory to create
        
    Returns:
        Path to the output directory
    """
    os.makedirs(dirname, exist_ok=True)
    return dirname


def get_pdf_output_dirs(output_dir: str, pdf_path: str) -> dict:
    """
    Create PDF-specific output directory structure and return paths
    
    Directory structure:
    output/
      pdf_name/
        csv/     (for CSV/JSON files)
        txt/     (for text files)
        images/  (for image files)
    
    Args:
        output_dir: Base output directory (e.g., "output")
        pdf_path: Path to the PDF file
        
    Returns:
        Dictionary with keys: 'base', 'csv', 'txt', 'images'
    """
    # Ensure base output directory exists
    ensure_output_dir(output_dir)
    
    # Get PDF filename without extension
    pdf_filename = Path(pdf_path).stem
    pdf_dirname = sanitize_filename(pdf_filename)
    
    # Create PDF-specific directory structure
    base_pdf_dir = os.path.join(output_dir, pdf_dirname)
    csv_dir = os.path.join(base_pdf_dir, "csv")
    txt_dir = os.path.join(base_pdf_dir, "txt")
    images_dir = os.path.join(base_pdf_dir, "images")
    
    # Create all directories
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    
    return {
        'base': base_pdf_dir,
        'csv': csv_dir,
        'txt': txt_dir,
        'images': images_dir
    }


def parse_page_ranges(pages_str: str) -> Set[int]:
    """
    Parse a string like "1-3,5,7-9" into a set of page numbers
    
    Args:
        pages_str: String representation of page ranges
        
    Returns:
        Set of page numbers
        
    Example:
        "1-3,5,7-9" returns {1, 2, 3, 5, 7, 8, 9}
    """
    if not pages_str:
        return set()
        
    pages = set()
    for part in pages_str.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            pages.update(range(start, end + 1))
        else:
            pages.add(int(part))
    return pages


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to be used as a filename
    
    Args:
        filename: The input string to sanitize
        
    Returns:
        A sanitized filename string
    """
    # Replace any non-alphanumeric character with underscore
    sanitized = re.sub(r'[^\w\-\.]', '_', filename)
    return sanitized 