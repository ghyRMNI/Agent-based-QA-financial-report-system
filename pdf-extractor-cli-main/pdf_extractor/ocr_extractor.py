"""
Module for OCR extraction from PDF images using pytesseract
"""
import os
import io
import fitz
import logging
import pytesseract
from PIL import Image
from typing import List, Set, Dict, Any, Optional
from pathlib import Path

from .utils import ensure_output_dir, sanitize_filename, get_pdf_output_dirs

# Set the tesseract command path if installed in default Windows location
if os.name == 'nt':  # Windows
    # Try common Windows installation paths
    tesseract_paths = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        r"C:\Users\Public\Tesseract-OCR\tesseract.exe"
    ]
    for path in tesseract_paths:
        if os.path.exists(path):
            pytesseract.pytesseract.tesseract_cmd = path
            break


class OCRExtractor:
    """
    Extract text from PDF images using OCR
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the OCR extractor
        
        Args:
            logger: Logger object for logging messages
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_text_with_ocr(self, pdf_path: str, pages: Optional[Set[int]] = None,
                             output_dir: str = "output") -> str:
        """
        Apply OCR to images in a PDF and extract text
        
        Args:
            pdf_path: Path to the PDF file
            pages: Set of page numbers to extract text from (1-indexed).
                  If None, extract from all pages.
            output_dir: Directory to save output
            
        Returns:
            Path to the output text file
        """
        # Check if tesseract is available
        try:
            pytesseract.get_tesseract_version()
        except pytesseract.TesseractNotFoundError:
            self.logger.error("tesseract is not installed or it's not in your PATH. See README file for more information.")
            raise

        # Get PDF-specific output directories
        pdf_dirs = get_pdf_output_dirs(output_dir, pdf_path)
        # Use txt directory for OCR text files
        output_dir = pdf_dirs['txt']
        pdf_filename = Path(pdf_path).stem
        output_file = os.path.join(output_dir, f"{sanitize_filename(pdf_filename)}_ocr.txt")
        
        self.logger.info(f"Applying OCR to {pdf_path}")
        
        try:
            doc = fitz.open(pdf_path)
            
            # Determine which pages to process
            if pages:
                # Convert from 1-indexed to 0-indexed
                page_indices = [p - 1 for p in pages if 0 < p <= len(doc)]
                if len(page_indices) < len(pages):
                    self.logger.warning(f"Some requested pages are out of range. PDF has {len(doc)} pages.")
            else:
                page_indices = range(len(doc))
            
            ocr_text = []
            
            # Process each page
            for i in page_indices:
                page = doc[i]
                page_number = i + 1  # Convert back to 1-indexed
                
                self.logger.info(f"Processing page {page_number} with OCR")
                
                # Get the page as a pixmap (raster image)
                pix = page.get_pixmap(alpha=False)
                
                # Convert pixmap to PIL Image
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                
                # Apply OCR to the image with Chinese language support
                # Try to use chi_sim (Simplified Chinese) and eng (English) languages
                # If Chinese language pack is not installed, fall back to English only
                try:
                    # First try with Chinese + English
                    page_text = pytesseract.image_to_string(img, lang='chi_sim+eng')
                except pytesseract.TesseractError:
                    try:
                        # If chi_sim not available, try chi_tra (Traditional Chinese)
                        page_text = pytesseract.image_to_string(img, lang='chi_tra+eng')
                    except pytesseract.TesseractError:
                        # Fall back to English only
                        self.logger.warning(f"Chinese language pack not found. Using English only for page {page_number}.")
                        page_text = pytesseract.image_to_string(img, lang='eng')
                
                ocr_text.append(f"--- Page {page_number} ---\n")
                ocr_text.append(page_text)
                ocr_text.append("\n\n")
            
            doc.close()
            
            # Write the OCR text to a file
            with open(output_file, "w", encoding="utf-8") as f:
                f.write("".join(ocr_text))
            
            self.logger.info(f"OCR text extraction complete. Output saved to {output_file}")
            return output_file
                
        except Exception as e:
            self.logger.error(f"Error extracting OCR text: {e}")
            raise 