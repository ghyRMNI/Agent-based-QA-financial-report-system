"""
Module for extracting images from PDF files using PyMuPDF
"""
import os
import io
import fitz
import logging
from typing import List, Set, Dict, Any, Optional
from pathlib import Path

from .utils import ensure_output_dir, sanitize_filename, get_pdf_output_dirs


class ImageExtractor:
    """
    Extract images from PDF files using PyMuPDF
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the image extractor
        
        Args:
            logger: Logger object for logging messages
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_images(self, pdf_path: str, pages: Optional[Set[int]] = None,
                      output_dir: str = "output") -> List[str]:
        """
        Extract images from a PDF file
        
        Args:
            pdf_path: Path to the PDF file
            pages: Set of page numbers to extract images from (1-indexed).
                  If None, extract from all pages.
            output_dir: Directory to save output files
            
        Returns:
            List of paths to the saved image files
        """
        # Get PDF-specific output directories
        pdf_dirs = get_pdf_output_dirs(output_dir, pdf_path)
        # Use images directory for image files
        image_dir = pdf_dirs['images']
        
        output_files = []
        image_count = 0
        
        self.logger.info(f"Extracting images from {pdf_path}")
        
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
            
            # Extract images from each page
            for i in page_indices:
                page = doc[i]
                page_number = i + 1  # Convert back to 1-indexed
                
                # Get images from page
                image_list = page.get_images(full=True)
                
                if not image_list:
                    self.logger.info(f"No images found on page {page_number}")
                    continue
                
                self.logger.info(f"Found {len(image_list)} images on page {page_number}")
                
                # Process each image
                for img_idx, img_info in enumerate(image_list):
                    xref = img_info[0]  # Image reference number
                    base_image = doc.extract_image(xref)
                    
                    if base_image:
                        image_bytes = base_image["image"]
                        image_ext = base_image["ext"]
                        
                        # Save the image
                        image_filename = f"page{page_number}_image{img_idx+1}.{image_ext}"
                        image_path = os.path.join(image_dir, image_filename)
                        
                        with open(image_path, "wb") as img_file:
                            img_file.write(image_bytes)
                        
                        output_files.append(image_path)
                        image_count += 1
                        self.logger.debug(f"Saved image to {image_path}")
            
            doc.close()
            
            if image_count > 0:
                self.logger.info(f"Extracted {image_count} images to {image_dir}")
            else:
                self.logger.info("No images found in the PDF")
                
            return output_files
                
        except Exception as e:
            self.logger.error(f"Error extracting images: {e}")
            raise 