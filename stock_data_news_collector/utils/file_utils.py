"""
File utility functions
"""

import os
from typing import Optional

def ensure_directory_exists(directory_path: str) -> bool:
    """
    Ensure directory exists, create if it doesn't
    
    Args:
        directory_path: Path to directory
        
    Returns:
        True if directory exists or was created successfully
    """
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
            print(f"Created directory: {directory_path}")
        return True
    except Exception as e:
        print(f"Failed to create directory {directory_path}: {e}")
        return False

def get_file_extension(filename: str) -> str:
    """
    Get file extension from filename
    
    Args:
        filename: Filename
        
    Returns:
        File extension
    """
    return os.path.splitext(filename)[1].lower()

def is_valid_csv_file(filepath: str) -> bool:
    """
    Check if file is a valid CSV file
    
    Args:
        filepath: Path to file
        
    Returns:
        True if valid CSV file
    """
    return os.path.isfile(filepath) and get_file_extension(filepath) == '.csv'