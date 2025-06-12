"""
gsy_data.py
Data retrieval module for GSY data extraction system.

This module provides functionality for:
- Making HTTP requests with retry logic and error handling
- Parsing JSON responses with invalid character handling
- Retrieving program lists from the API
- Fetching GTP data with pagination support
- Progress tracking and error reporting

Dependencies:
- requests: For HTTP communication
- pandas: For data manipulation

Version: 1.0.5
"""

import json
import logging
import time
import random
import sys
from pandas import DataFrame, concat
from requests import get, RequestException
from typing import Dict, Optional, Any, List
from .gsy_debug import dbgWrite, DEBUG_FILE_PREFIX
from .gsy_cfg import INITIAL_TIMEOUT, MAX_RETRIES

# API configuration constants
PAGE_SIZE = 200  # Max number of records per page

# Retry mechanism constants
RETRY_BACKOFF_BASE = 1.5  # Exponential backoff base
RETRY_BACKOFF_JITTER = 0.5  # Random jitter to prevent thundering herd
MAX_RETRY_DELAY = 30  # Maximum delay between retries in seconds

def dfGetData(url: str, timeout: int = INITIAL_TIMEOUT, max_retries: int = MAX_RETRIES, 
              silent: bool = False) -> Optional[Any]:
    """
    Execute HTTP GET request with retry logic and comprehensive error handling.
    
    Implements exponential backoff with jitter for failed requests and provides
    detailed error diagnostics for troubleshooting.
    
    Args:
        url: Target URL for the GET request
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        silent: If True, suppress detailed error logging
        
    Returns:
        Optional[Any]: Response object on success, None on failure
    """
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            if retry_count > 0:
                # Calculate retry timeout with exponential backoff
                retry_timeout = timeout * (RETRY_BACKOFF_BASE ** retry_count)
                
                # Add random jitter to avoid synchronized retries
                jitter = random.uniform(-RETRY_BACKOFF_JITTER, RETRY_BACKOFF_JITTER)
                retry_timeout = max(1, retry_timeout * (1 + jitter))
                
                logging.info(f"Retry attempt {retry_count}/{max_retries} with timeout {retry_timeout:.1f}s...")
                
                # Apply exponential backoff delay
                delay = min(MAX_RETRY_DELAY, 2 ** retry_count)
                time.sleep(delay)
                
                response = get(url, timeout=retry_timeout)
            else:
                response = get(url, timeout=timeout)
                
            response.raise_for_status()  # Check for HTTP errors
            
            # Log success for retried requests
            if retry_count > 0:
                logging.info(f"Successfully retrieved data after {retry_count + 1} attempts")
            
            return response
            
        except RequestException as e:
            last_error = e
            retry_count += 1
            
            # Log detailed error information
            if not silent and (retry_count == max_retries or retry_count == 1):
                logging.error(f"Error accessing URL: {url}")
                logging.error(f"Error details: {str(e)}")
                
                # Provide specific error diagnostics
                error_msg = str(e)
                if "RemoteDisconnected" in error_msg or "ConnectionReset" in error_msg:
                    logging.error("Connection was interrupted by the server.")
                    logging.error("Possible causes: network instability, server overload, or insufficient timeout.")
                elif "ConnectTimeout" in error_msg or "ReadTimeout" in error_msg:
                    logging.error("Request timed out. The server is taking too long to respond.")
                    logging.error("Consider increasing API_TIMEOUT in .env file.")
                elif "ConnectionError" in error_msg:
                    logging.error("Connection error. Check network connectivity and VPN status.")
            
            # Inform about retry attempts
            if retry_count < max_retries and not silent:
                logging.info(f"Will retry in {min(MAX_RETRY_DELAY, 2 ** retry_count)} seconds... ({retry_count}/{max_retries})")
    
    # All retries exhausted
    if not silent:
        logging.error(f"Failed to retrieve data after {max_retries} attempts")
        logging.error(f"Last error: {str(last_error)}")
    
    return None

def parseJSON(response: Optional[Any]) -> Optional[Dict]:
    """
    Parse JSON response with error handling for invalid control characters.
    
    Attempts to parse JSON and handles common issues like invalid ASCII
    control characters that can occur in some responses.
    
    Args:
        response: HTTP response object containing JSON data
        
    Returns:
        Optional[Dict]: Parsed JSON data or None/empty dict on error
    """
    if response is None:
        return None
    
    try:
        return response.json()
    except json.JSONDecodeError as e:
        logging.warning(f"JSON decode error: {str(e)}")
        
        try:
            # Attempt to clean invalid control characters
            text = response.text
            
            # Remove ASCII control characters (0-31) except allowed ones
            allowed_chars = {9, 10, 13}  # Tab, LF, CR
            for i in range(32):
                if i not in allowed_chars:
                    text = text.replace(chr(i), '')
            
            return json.loads(text)
        except (json.JSONDecodeError, UnicodeError) as e:
            logging.error(f"Could not parse JSON after cleanup: {str(e)}")
            return {}

def dfGetProgram(api_base_url: str, api_key: str, debug: bool, 
                 timeout: int = INITIAL_TIMEOUT, 
                 max_retries: int = MAX_RETRIES) -> DataFrame:
    """
    Retrieve the complete list of programs from the API.
    
    This function is critical for the data extraction process as it provides
    the program list used to fetch detailed data. It includes comprehensive
    error handling and diagnostic information.
    
    Args:
        api_base_url: Base URL for API endpoints
        api_key: Authentication key for API access
        debug: Enable debug file writing
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts
        
    Returns:
        DataFrame: Program list with columns: programID, programCode, program
        
    Raises:
        SystemExit: On critical errors that prevent continuation
    """
    url_program = f'{api_base_url}/groundTestProposalAPI.cfc?method=getProgramList&key={api_key}'
    dbgWrite('debug/ProgramList_url.txt', url_program, debug)
    
    logging.info("Fetching program list...")
    response = dfGetData(url_program, timeout=timeout, max_retries=max_retries)
    
    # Handle connection failure
    if response is None:
        logging.error("*" * 55)
        logging.error("*** ERROR: Unable to retrieve program list data ***")
        logging.error("*" * 55)
        logging.error("Possible causes:")
        logging.error("1. VPN connection is not active or stable")
        logging.error("2. Server is experiencing high load or maintenance")
        logging.error("3. Network connectivity issues")
        logging.error(f"4. Insufficient timeout (current: {timeout}s)")
        logging.error("")
        logging.error("Troubleshooting steps:")
        logging.error("1. Verify VPN connection to Embraer network")
        logging.error("2. Increase API_TIMEOUT in .env file (e.g., API_TIMEOUT=300)")
        logging.error("3. Increase MAX_RETRIES in .env file (e.g., MAX_RETRIES=10)")
        logging.error("4. Try again during off-peak hours")
        sys.exit(1)
    
    # Detect HTML response (authentication/access issues)
    if "<html" in response.text.lower():
        logging.error("*" * 55)
        logging.error("*** ERROR: Received HTML response instead of JSON ***")
        logging.error("*" * 55)
        logging.error("This indicates an authentication or access issue.")
        logging.error("Please connect to Embraer network using FortiClient VPN.")
        logging.error("Debug information saved to: ./debug/authentication.html")
        dbgWrite('debug/authentication.html', response.text, True)
        sys.exit(2)
        
    # Detect API error response
    if "ERROR" in response.text:
        logging.error("*" * 55)
        logging.error("*** ERROR: API returned an error message ***")
        logging.error("*" * 55)
        logging.error("This typically indicates an API key issue.")
        logging.error("Common causes:")
        logging.error("- Using production API key in QAS environment")
        logging.error("- Using QAS API key in production environment")
        logging.error("- Expired or invalid API key")
        logging.error("")
        logging.error("API Error Message:")
        logging.error(response.text)
        sys.exit(3)

    dbgWrite('debug/ProgramList.txt', response.text, debug)    

    # Parse JSON response
    json_data = parseJSON(response)
    dbgWrite('debug/ProgramList.json', json_data, debug)
    
    # Handle different JSON structures
    if isinstance(json_data, dict) and 'programs' in json_data:
        program_table = DataFrame(json_data['programs'])
        logging.info(f"Found {len(program_table)} programs")
    elif isinstance(json_data, list):
        program_table = DataFrame(json_data)
        logging.info(f"Found {len(program_table)} programs")
    else:
        logging.error("Unexpected JSON structure in program list response")
        logging.error(f"Received: {type(json_data)}")
        if json_data:
            logging.error(f"Keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'N/A'}")
        raise Exception("Could not parse program list from API response")
        
    return program_table

def dfGetGSYData(url_base: str, date_filter: str, data_type: str, 
             program_table: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """
    Retrieve GSY data for all programs with pagination support.
    
    Iterates through all programs in the program table and fetches their
    associated data within the specified date range. Handles pagination
    for large datasets and provides progress tracking.
    
    Args:
        url_base: Base URL for the specific data type endpoint
        date_filter: JSON-formatted date range filter
        data_type: Type identifier (BDI or TechRep)
        program_table: DataFrame containing program information
        config: Configuration dictionary with settings
        
    Returns:
        DataFrame: Combined data from all programs
    """
    logging.info(f"Fetching {data_type} data...")
    result_df = DataFrame()
    
    # Initialize progress tracking
    total_programs = len(program_table)
    successful_programs = 0
    failed_programs = 0
    
    # Process each program
    for idx, prog in enumerate(program_table.index):
        # Extract program information
        program_id = program_table.loc[prog, "programID"]
        program_code = program_table.loc[prog, "programCode"]
        program_description = program_table.loc[prog, "program"]
        
        # Use programName if available, otherwise use programID
        program_name = program_table.loc[prog, "programName"] \
            if "programName" in program_table.columns else program_id
        
        # Log progress
        progress_msg = f"[{idx+1:02d}/{total_programs:02d}] {program_name:02d} - {program_code} - {program_description}"
        logging.info(progress_msg)
        
        # Initialize pagination
        page = 1
        url_complete = f"{url_base}{program_id}{date_filter}&page={page}"
        
        # Debug: Save request URL
        debug_file = DEBUG_FILE_PREFIX['url'].format(
            prog_index=idx, page=page, data_type=data_type
        )
        dbgWrite(debug_file, url_complete, config['debug'])
        
        # Make initial request
        response = dfGetData(
            url_complete, 
            timeout=config['timeout'], 
            max_retries=config['max_retries'],
            silent=False
        )
        
        if response is None:
            failed_programs += 1
            logging.warning(f"[{idx+1:02d}/{total_programs}] Failed to retrieve data for {program_name}")
            continue
        
        # Debug: Save raw response
        debug_file = DEBUG_FILE_PREFIX['raw_response'].format(
            prog_index=idx, page=page, data_type=data_type
        )
        dbgWrite(debug_file, response.text if response else "No response", config['debug'])
        
        # Parse JSON response
        json_data = parseJSON(response)
        if not json_data:
            failed_programs += 1
            logging.warning(f"[{idx+1:02d}/{total_programs}] Failed to parse JSON for {program_name}")
            continue
            
        # Debug: Save parsed data
        debug_file = DEBUG_FILE_PREFIX['parsed_data'].format(
            prog_index=idx, page=page, data_type=data_type
        )
        dbgWrite(debug_file, json_data, config['debug'])
        
        # Extract data from various JSON structures
        current_data = _extract_data_from_json(json_data)
        if current_data is None:
            failed_programs += 1
            logging.warning(f"[{idx+1:02d}/{total_programs}] Unexpected JSON structure for {program_name}")
            continue
            
        # Convert to DataFrame
        current_df = _create_dataframe(current_data)
      
        # Debug: Save DataFrame
        debug_file = DEBUG_FILE_PREFIX['dataframe'].format(
            prog_index=idx, page=page, data_type=data_type
        )
        if config['debug'] and not current_df.empty:
            current_df.to_csv(debug_file, index=False)
        
        if current_df.empty:
            if config['debug']:
                logging.info(f"[{idx+1:02d}/{total_programs}] No data found for {program_name}")
            continue
        
        successful_programs += 1
        result_df = concat([result_df, current_df], ignore_index=True, sort=True)
        
        # Handle pagination
        result_df = _handle_pagination(
            result_df, current_df, url_base, program_id, date_filter,
            idx, total_programs, program_name, config
        )
    
    # Log final statistics
    logging.info(f"{data_type} data retrieval complete:")
    logging.info(f"  Successful: {successful_programs}/{total_programs} programs")
    logging.info(f"  Failed: {failed_programs}/{total_programs} programs")
    logging.info(f"  Total records: {len(result_df):,}")
    
    return result_df

def _extract_data_from_json(json_data: Any) -> Optional[Any]:
    """
    Extract data from various JSON response structures.
    
    Args:
        json_data: Parsed JSON data
        
    Returns:
        Optional[Any]: Extracted data or None if structure is unexpected
    """
    # Handle list response
    if isinstance(json_data, list) and len(json_data) > 0:
        return json_data[0]
    
    # Handle dictionary with known data keys
    if isinstance(json_data, dict):
        data_keys = ['GTPs','docs', 'documents', 'data', 'items']
        for key in data_keys:
            if key in json_data:
                return json_data[key]
        
        # Return dictionary itself if no known keys found
        return json_data
    
    return None

def _create_dataframe(data: Any) -> DataFrame:
    """
    Create DataFrame from data, handling both list and dict inputs.
    
    Args:
        data: Input data (list or dict)
        
    Returns:
        DataFrame: Created DataFrame
    """
    if isinstance(data, list):
        return DataFrame(data)
    else:
        return DataFrame([data])

def _handle_pagination(result_df: DataFrame, current_df: DataFrame, 
                      url_base: str, program_id: str, date_filter: str,
                      prog_idx: int, total_programs: int, program_name: str,
                      config: Dict[str, Any]) -> DataFrame:
    """
    Handle pagination for API responses with more than PAGE_SIZE records.
    
    Args:
        result_df: Main result DataFrame
        current_df: Current page DataFrame
        url_base: Base URL for requests
        program_id: Current program ID
        date_filter: Date filter string
        prog_idx: Program index for progress display
        total_programs: Total number of programs
        program_name: Program name for logging
        config: Configuration dictionary
        
    Returns:
        DataFrame: Updated result DataFrame with all pages
    """
    page = 1
    
    while len(current_df) == PAGE_SIZE:
        page += 1
        logging.info(f"[{prog_idx+1:02d}/{total_programs}] Fetching page {page}")
        
        # Request next page (silent mode to reduce log noise)
        response = dfGetData(
            f"{url_base}{program_id}{date_filter}&page={page}",
            timeout=config['timeout'],
            max_retries=config['max_retries'],
            silent=True
        )
        
        if response is None:
            logging.warning(f"Failed to retrieve page {page} for {program_name}")
            break
        
        # Process response
        json_data = parseJSON(response)
        if not json_data:
            logging.warning(f"Failed to parse JSON for page {page} of {program_name}")
            break
            
        current_data = _extract_data_from_json(json_data)
        if current_data is None:
            logging.warning(f"Unexpected structure for page {page} of {program_name}")
            break
        
        current_df = _create_dataframe(current_data)
        
        if current_df.empty:
            break
        
        # Add page data to results
        result_df = concat([result_df, current_df], ignore_index=True)
    
    return result_df