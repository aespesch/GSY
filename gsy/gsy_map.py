"""
gsy_map.py
Program mapping module for GSY data extraction system.

This module manages program group mappings that allow multiple programs
to be consolidated under group names for reporting purposes. The mapping
is defined in an external text file using semicolon-delimited format.

Mapping file format:
    program_name;group_name
    
Example:
    E190;Commercial Aviation
    E195;Commercial Aviation
    KC390;Defense

Version: 1.0.5
"""

import os
import sys
import logging
from pandas import DataFrame
from typing import Dict, Optional

# Constants
MAPPING_FILE = 'program_group.txt'
MAPPING_DELIMITER = ';'

def cfgMap() -> None:
    """
    Verify that the program mapping file exists in the current directory.
    
    This function checks for the required mapping file and provides
    helpful error messages if it's missing. The file is required even
    if empty (for cases where no mapping is needed).
    
    Returns:
        None
        
    Raises:
        SystemExit: If mapping file is not found
    """
    if not os.path.isfile(MAPPING_FILE):
        logging.error(f"Error: {MAPPING_FILE} not found in current directory")
        logging.error("This file is required for program mapping configuration")
        logging.error("To proceed without mapping, create an empty file:")
        logging.error(f"  Windows: type nul > {MAPPING_FILE}")
        logging.error(f"  Linux/Mac: touch {MAPPING_FILE}")
        sys.exit(1)

def getMap(filename: str) -> Dict[str, str]:
    """
    Load program-to-group mappings from a semicolon-delimited text file.
    
    The function reads mappings where each line contains a program name
    and its corresponding group, separated by a semicolon. Empty lines
    and lines without semicolons are ignored.
    
    Args:
        filename: Path to the mapping file
        
    Returns:
        Dict[str, str]: Dictionary mapping program names to group names
        
    Note:
        - Leading/trailing whitespace is stripped from both values
        - Duplicate program names will use the last defined mapping
        - File encoding is assumed to be UTF-8
    """
    program_mapping = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            line_number = 0
            valid_mappings = 0
            
            for line in f:
                line_number += 1
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Skip lines without delimiter
                if MAPPING_DELIMITER not in line:
                    logging.debug(f"Line {line_number} skipped (no delimiter): {line}")
                    continue
                
                # Parse mapping
                parts = line.split(MAPPING_DELIMITER, 1)
                program = parts[0].strip()
                group = parts[1].strip()
                
                # Validate non-empty values
                if not program or not group:
                    logging.warning(f"Line {line_number} has empty values: '{line}'")
                    continue
                
                # Store mapping (overwrites if duplicate)
                if program in program_mapping:
                    logging.warning(f"Duplicate program '{program}' on line {line_number} (overwriting)")
                
                program_mapping[program] = group
                valid_mappings += 1
        
        logging.info(f"Program mapping loaded: {valid_mappings} valid entries from {line_number} lines")
        
        # Log summary of unique groups
        unique_groups = set(program_mapping.values())
        logging.info(f"Unique program groups: {len(unique_groups)}")
        
    except FileNotFoundError:
        logging.error(f"Mapping file not found: {filename}")
        
    except Exception as e:
        logging.error(f"Error reading mapping file: {str(e)}")
        
    return program_mapping

def applyMap(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """
    Apply program-to-group mappings to the 'program' column in a DataFrame.
    
    This function replaces program names with their corresponding group names
    based on the provided mapping. Programs without mappings remain unchanged.
    
    Args:
        df: DataFrame containing a 'program' column
        mapping: Dictionary mapping program names to group names
        
    Returns:
        DataFrame: New DataFrame with mapped program values
        
    Note:
        - Original DataFrame is not modified
        - Case-sensitive matching is used
        - Unmapped programs retain their original names
    """
    # Check if program column exists
    if 'program' not in df.columns:
        logging.warning("'program' column not found in DataFrame - skipping mapping")
        return df
    
    # Create a copy to preserve original
    updated_df = df.copy()
    
    # Get unique programs before mapping
    original_programs = updated_df['program'].unique()
    original_count = len(original_programs)
    logging.info(f"Programs before mapping: {original_count} unique values")
    
    # Apply mapping using pandas replace method
    updated_df['program'] = updated_df['program'].replace(mapping)
    
    # Get unique programs after mapping
    new_programs = updated_df['program'].unique()
    new_count = len(new_programs)
    
    # Calculate mapping statistics
    mapped_programs = sum(1 for prog in original_programs if prog in mapping)
    reduction_pct = ((original_count - new_count) / original_count * 100) if original_count > 0 else 0
    
    logging.info(f"Programs after mapping: {new_count} unique values")
    logging.info(f"Mapped {mapped_programs} programs (reduction: {reduction_pct:.1f}%)")
    
    # Log unmapped programs if in debug mode
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        unmapped = [prog for prog in original_programs if prog not in mapping and prog == prog]  # NaN check
        if unmapped:
            logging.debug(f"Unmapped programs ({len(unmapped)}): {', '.join(unmapped[:10])}")
            if len(unmapped) > 10:
                logging.debug(f"... and {len(unmapped) - 10} more")
    
    return updated_df