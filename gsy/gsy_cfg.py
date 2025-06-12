"""
gsy_cfg.py
Configuration module for GSY data extraction system.

This module handles all configuration aspects including:
- Logging system setup with dynamic log filename
- Environment variable loading from .env file
- Configuration validation
- Default value management

Dependencies:
- python-dotenv: For loading environment variables
- dateutil: For date manipulation

Version: 1.0.6
"""

import os
import sys
import logging
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, Any

# Default configuration constants
INITIAL_TIMEOUT = 180
MAX_RETRIES = 5
DEFAULT_DATE_FORMAT = '%Y/%m/%d'
QAS_BASE_URL = 'https://ft-qas.embraer.com.br/components/systemTest/gtp'
PROD_BASE_URL = 'https://ft.embraer.com.br/components/systemTest/gtp'

def cfgLog() -> None:
    """
    Configure the logging system with file and console handlers.
    
    Sets up logging to write to both a log file (named after the main script)
    and console output with UTF-8 encoding support and timestamp formatting.
    The log filename is dynamically generated based on the main script name.
    """
    # Get the main script name from sys.argv[0]
    main_script = os.path.basename(sys.argv[0])
    
    # Replace the extension with .log
    log_filename = os.path.splitext(main_script)[0] + '.log'
    
    # If no extension was found or script name is empty, use a default
    if not log_filename or log_filename == '.log':
        log_filename = 'application.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Log the log file being used (optional, for debugging)
    logging.info(f"Logging to file: {log_filename}")
    
def validate_date_format(date_string: str, field_name: str) -> datetime:
    """
    Validate date string format and convert to datetime object.
    
    Args:
        date_string: Date string to validate (expected format: YYYY/MM/DD)
        field_name: Name of the field being validated (for error messages)
    
    Returns:
        datetime: Parsed datetime object
        
    Raises:
        SystemExit: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, DEFAULT_DATE_FORMAT)
    except ValueError:
        logging.error(f"Error: Invalid {field_name} format. '{date_string}' is not in YYYY/MM/DD format.")
        sys.exit(1)
    
def cfgEnv() -> Dict[str, Any]:
    """
    Load and validate environment variables from .env file.
    
    This function:
    1. Locates and loads the .env file
    2. Validates required configuration values
    3. Applies defaults where appropriate
    4. Performs validation checks
    
    Returns:
        Dict[str, Any]: Configuration dictionary containing:
            - api_key: API authentication key
            - date_ini: Start date for data retrieval
            - date_end: End date for data retrieval
            - api_base_url: Base URL for API calls
            - environment: QAS or Production
            - debug: Debug mode flag
            - timeout: API request timeout in seconds
            - max_retries: Maximum retry attempts for failed requests
    
    Raises:
        SystemExit: If required configuration is missing or invalid
    """
    # Locate .env file
    dotenv_path = find_dotenv()

    # Validate .env file exists
    if not dotenv_path:
        logging.error("Error: .env file not found. Please create a .env file with required environment variables.")
        logging.error("Required variables: API_KEY, DATE_INI")
        logging.error("Optional variables: DATE_END, GET_DATA_FROM_QAS, DEBUG, API_TIMEOUT, MAX_RETRIES")
        sys.exit(1)

    # Load environment variables
    load_dotenv(dotenv_path)
    
    # Validate and load API key (required)
    api_key = os.getenv('API_KEY', '').strip()
    if not api_key:
        logging.error('Error: API_KEY is not defined in the .env file')
        logging.error('Please add: API_KEY=your_api_key_here')
        sys.exit(1)

    # Determine environment and API base URL
    get_data_from_qas = os.getenv('GET_DATA_FROM_QAS', 'True').lower() == 'true'
    if get_data_from_qas:
        environment = 'QAS'
        api_base_url = QAS_BASE_URL
    else:
        environment = 'Production'
        api_base_url = PROD_BASE_URL

    # Load and validate date range
    date_ini = os.getenv('DATE_INI', '2010/01/01')
    date_ini_obj = validate_date_format(date_ini, 'DATE_INI')
    
    # Set default end date if not provided (3 months from today)
    date_end = os.getenv('DATE_END', '').strip()
    if not date_end:
        date_end_default = datetime.now() + relativedelta(months=3)
        date_end = date_end_default.strftime(DEFAULT_DATE_FORMAT)
    
    date_end_obj = validate_date_format(date_end, 'DATE_END')
    
    # Validate date range logic
    if date_ini_obj > date_end_obj:
        logging.error(f"Error: Start date ({date_ini}) must be before or equal to end date ({date_end}).")
        sys.exit(1)

    # Load and validate timeout setting
    timeout = os.getenv('API_TIMEOUT', str(INITIAL_TIMEOUT))
    try:
        timeout = int(timeout)
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
    except ValueError:
        logging.warning(f"Warning: Invalid API_TIMEOUT value '{timeout}'. Using default: {INITIAL_TIMEOUT}s")
        timeout = INITIAL_TIMEOUT

    # Load and validate max retries setting
    max_retries = os.getenv('MAX_RETRIES', str(MAX_RETRIES))
    try:
        max_retries = int(max_retries)
        if max_retries < 0:
            raise ValueError("Max retries cannot be negative")
    except ValueError:
        logging.warning(f"Warning: Invalid MAX_RETRIES value '{max_retries}'. Using default: {MAX_RETRIES}")
        max_retries = MAX_RETRIES

    # Load debug flag
    debug = os.getenv('DEBUG', 'False').lower() in ('true', '1', 'yes', 'on')

    # Return validated configuration
    return {
        'api_key': api_key,
        'date_ini': date_ini,
        'date_end': date_end,
        'api_base_url': api_base_url,
        'environment': environment,
        'debug': debug,
        'timeout': timeout,
        'max_retries': max_retries
    }