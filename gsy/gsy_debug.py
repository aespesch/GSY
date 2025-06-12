"""
gsy_debug.py
Debug utilities module for GSY data extraction system.

This module provides debugging functionality including:
- Conditional debug file writing based on debug flag
- Automatic directory creation for debug output
- Unicode encoding handling for various content types
- Structured debug file naming conventions

Version: 1.0.5
"""

import os
import json
import logging
from typing import Any, Union

# Debug file naming patterns for different stages of processing
DEBUG_FILE_PREFIX = {
    'url': 'debug/{data_type}/url_{prog_index:03d}_page{page:02d}_{data_type}.txt',
    'raw_response': 'debug/{data_type}/response_{prog_index:03d}_page{page:02d}_{data_type}.json',
    'parsed_data': 'debug/{data_type}/parsed_{prog_index:03d}_page{page:02d}_{data_type}.json',
    'dataframe': 'debug/{data_type}/dataframe_{prog_index:03d}_page{page:02d}_{data_type}.csv'
}

def dbgWrite(filename: str, content: Any, debug: bool) -> None:
    """
    Write content to a debug file if debug mode is enabled.
    
    This function handles various content types and ensures proper
    directory structure creation. It includes error handling for
    encoding issues and file system errors.
    
    Args:
        filename: Path to the debug file (directories will be created)
        content: Content to write (can be string, dict, list, etc.)
        debug: Debug mode flag - if False, function returns immediately
        
    Returns:
        None
        
    Note:
        - Directories are created automatically if they don't exist
        - Unicode encoding errors are handled with replacement
        - File system errors are logged but don't stop execution
    """
    if not debug:
        return
        
    try:
        # Ensure directory structure exists
        directory = os.path.dirname(filename)
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        # Convert content to string format
        content_str = _format_content(content)
        
        # Write to file with UTF-8 encoding
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content_str)
            
    except UnicodeEncodeError:
        # Fallback for encoding issues - replace problematic characters
        try:
            with open(filename, 'w', encoding='utf-8', errors='replace') as f:
                f.write(str(content))
        except Exception as e:
            logging.warning(f"Debug write failed with encoding fallback for '{filename}': {str(e)}")
            
    except Exception as e:
        # Log other errors but continue execution
        logging.warning(f"Debug write failed for '{filename}': {str(e)}")

def _format_content(content: Any) -> str:
    """
    Format content for debug output based on its type.
    
    Args:
        content: Content to format (any type)
        
    Returns:
        str: Formatted string representation
    """
    # Handle None
    if content is None:
        return "None"
    
    # Handle dictionaries and lists with pretty JSON formatting
    if isinstance(content, (dict, list)):
        try:
            return json.dumps(content, indent=2, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            # Fallback to string representation
            return str(content)
    
    # Handle all other types
    return str(content)