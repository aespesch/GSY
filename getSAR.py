"""
getSAR.py
Main script for fetching SAR (Safety Analysis Report) data.

This script orchestrates the SAR data extraction process, utilizing modular components
to retrieve data from the SAR system.

The script retrieves data through SAR's API within a specified date range and exports
it in CSV and Excel formats.

Requirements:
- API_KEY must be set in .env file for authentication
- Date range (DATE_INI, DATE_END) must be defined in .env file
- VPN connection to Embraer's network is required

Output:
- getSAR.csv: Raw data in CSV format
- getSAR.xlsm: Formatted data with macros
- getSAR.log: Processing log file

"""

import os
import logging
from datetime import datetime
import pandas as pd  # Import pandas properly at the top
from pandas import DataFrame

# Script version and system type - MUST be defined before gsy imports
SCRIPT_VERSION = "2.0.2"  # Updated for pandas 2.0 compatibility and ColdFusion date format fix
SCRIPT_SYSTEM = "SAR"
SCRIPT_ENV = "Production"  # SAR only uses Production

# Import all necessary functions from the gsy package
# IMPORTANT: SCRIPT_SYSTEM must be defined before these imports
from gsy import (
    cfgLog, cfgEnv, cfgConn,
    dfGetProgram, dfGetData, parseJSON,
    dbgWrite,
    dfReorder, dfCheck, dfExportExcel,
    checkExcel
)


def dfGetSARData(url_base: str, date_filter: str, program_table: DataFrame, config: dict) -> DataFrame:
    """
    Retrieve SAR data for all programs with pagination support.
    
    This is a SAR-specific version of dfGetGSYData that handles the unique
    JSON structure returned by the SAR API.
    
    Args:
        url_base: Base URL for SAR endpoint
        date_filter: JSON-formatted date range filter
        program_table: DataFrame containing program information
        config: Configuration dictionary
        
    Returns:
        DataFrame: Combined SAR data from all programs
    """
    logging.info("Fetching SAR data...")
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
        
        # Log progress
        progress_msg = f"[{idx+1:02d}/{total_programs:02d}] {program_id} - {program_code} - {program_description}"
        logging.info(progress_msg)
        
        # Initialize pagination
        page = 1
        url_complete = f"{url_base}{program_id}{date_filter}&page={page}"
        
        # Debug: Save request URL
        if config['debug']:
            dbgWrite(f'debug/SAR/url_{idx:03d}_page{page:02d}_SAR.txt', url_complete, True)
        
        # Make initial request
        response = dfGetData(
            url_complete, 
            timeout=config['timeout'], 
            max_retries=config['max_retries'],
            silent=False
        )
        
        if response is None:
            failed_programs += 1
            logging.warning(f"[{idx+1:02d}/{total_programs}] Failed to retrieve data for {program_id}")
            continue
        
        # Debug: Save raw response
        if config['debug']:
            dbgWrite(f'debug/SAR/response_{idx:03d}_page{page:02d}_SAR.json', response.text, True)
        
        # Parse JSON response
        json_data = parseJSON(response)
        if not json_data:
            failed_programs += 1
            logging.warning(f"[{idx+1:02d}/{total_programs}] Failed to parse JSON for {program_id}")
            continue
        
        # Extract SAR data from JSON structure
        sar_data = None
        
        # Handle different JSON structures specific to SAR
        if isinstance(json_data, dict):
            # Look for SAR-specific keys
            for key in ['SARList', 'sarList', 'sars', 'data', 'results']:
                if key in json_data:
                    sar_data = json_data[key]
                    logging.debug(f"Found data in key '{key}' for program {program_id}")
                    break
            # Smart fallback for single-key dictionaries
            if sar_data is None and len(json_data) == 1:
                first_key = list(json_data.keys())[0]
                sar_data = json_data[first_key]
                logging.info(f"Using data from single key '{first_key}' for program {program_id}")
        elif isinstance(json_data, list) and len(json_data) > 0:
            sar_data = json_data[0]
        else:
            sar_data = json_data
        
        # Convert to DataFrame
        if sar_data is None or (isinstance(sar_data, list) and len(sar_data) == 0):
            logging.debug(f"No data found for program ID: {program_id}")
            continue
        
        current_df = DataFrame(sar_data) if isinstance(sar_data, list) else DataFrame([sar_data])
        
        if current_df.empty:
            logging.debug(f"Empty DataFrame for program ID: {program_id}")
            continue
        
        # Debug: Save DataFrame
        if config['debug'] and not current_df.empty:
            current_df.to_csv(f'debug/SAR/dataframe_{idx:03d}_page{page:02d}_SAR.csv', index=False)
        
        successful_programs += 1
        records_in_program = len(current_df)
        
        # Use pd.concat instead of append for pandas 2.0+ compatibility
        if result_df.empty:
            result_df = current_df
        else:
            result_df = pd.concat([result_df, current_df], ignore_index=True)
        
        # Handle pagination - SAR returns 200 records per page
        while len(current_df) == 200:
            page += 1
            url_complete = f"{url_base}{program_id}{date_filter}&page={page}"
            
            response = dfGetData(url_complete, timeout=config['timeout'], max_retries=config['max_retries'], silent=True)
            
            if response is None:
                break
            
            json_data = parseJSON(response)
            if not json_data:
                break
            
            # Extract SAR data for pagination
            if isinstance(json_data, dict):
                for key in ['SARList', 'sarList', 'sars', 'data', 'results']:
                    if key in json_data:
                        sar_data = json_data[key]
                        break
                if sar_data is None and len(json_data) == 1:
                    first_key = list(json_data.keys())[0]
                    sar_data = json_data[first_key]
            elif isinstance(json_data, list) and len(json_data) > 0:
                sar_data = json_data[0]
            else:
                sar_data = json_data
            
            if sar_data is None or (isinstance(sar_data, list) and len(sar_data) == 0):
                break
            
            current_df = DataFrame(sar_data) if isinstance(sar_data, list) else DataFrame([sar_data])
            
            if not current_df.empty:
                if config['debug']:
                    current_df.to_csv(f'debug/SAR/dataframe_{idx:03d}_page{page:02d}_SAR.csv', index=False)
                # Use pd.concat for pagination data as well
                result_df = pd.concat([result_df, current_df], ignore_index=True)
                records_in_program += len(current_df)
        
        logging.info(f"Program {program_id} ({program_code}): {records_in_program} records found")
    
    # Log final statistics
    logging.info(f"SAR data retrieval complete:")
    logging.info(f"  Successful: {successful_programs}/{total_programs} programs")
    logging.info(f"  Failed: {failed_programs}/{total_programs} programs")
    logging.info(f"  Total records: {len(result_df):,}")
    
    return result_df


def corrige_datas_sar(df: DataFrame) -> DataFrame:
    """
    Correct date format in SAR data.
    
    Converts dates from ColdFusion timestamp format "{ts 'YYYY-MM-DD HH:MM:SS'}" 
    to "DD/MM/YYYY" format in the estimatedDate column (third column).
    
    Args:
        df: DataFrame with SAR data
        
    Returns:
        DataFrame: Data with corrected date format
    """
    logging.info("Correcting date format in SAR data...")
    
    # Create a copy to avoid modifying original
    df_copy = df.copy()
    
    # Find the estimatedDate column (should be the third column)
    if len(df_copy.columns) > 2:
        date_column = df_copy.columns[2]
        dates_corrected = 0
        
        # Process each row
        for idx in df_copy.index:
            try:
                original_value = df_copy.loc[idx, date_column]
                
                if pd.notna(original_value) and isinstance(original_value, str):
                    # Handle ColdFusion timestamp format: {ts '2023-02-07 00:00:00'}
                    if original_value.startswith("{ts '") and original_value.endswith("'}"):
                        # Extract the date portion from the timestamp
                        # Remove the "{ts '" prefix and "'}" suffix
                        timestamp_str = original_value[5:-2]  # Gets '2023-02-07 00:00:00'
                        
                        # Split to get just the date part
                        date_part = timestamp_str.split(' ')[0]  # Gets '2023-02-07'
                        
                        # Split the date components
                        date_components = date_part.split('-')
                        if len(date_components) == 3:
                            # Reformat to DD/MM/YYYY
                            year = date_components[0]
                            month = date_components[1]
                            day = date_components[2]
                            df_copy.loc[idx, date_column] = f"{day}/{month}/{year}"
                            dates_corrected += 1
                    
                    # Also handle other possible date formats as fallback
                    elif ' ' in original_value and '-' in original_value:
                        # Handle format like "T 2025-12-31" (original logic)
                        parts = original_value.split(' ')
                        if len(parts) > 1:
                            date_part = parts[1]
                            date_components = date_part.split('-')
                            if len(date_components) == 3:
                                # Reformat to DD/MM/YYYY
                                df_copy.loc[idx, date_column] = f"{date_components[2]}/{date_components[1]}/{date_components[0]}"
                                dates_corrected += 1
                                
            except Exception as e:
                logging.debug(f"Could not parse date in row {idx}: {e}")
        
        logging.info(f"Date correction complete. {dates_corrected} dates corrected")
    
    return df_copy


def main():
    """
    Main function that orchestrates the entire SAR data retrieval and processing workflow.
    """
    start_time = datetime.now()
    
    # ===============================================================
    # Configuration Phase
    # ===============================================================
    
    # Initialize logging system
    cfgLog()
    logging.info(f'========= SAR DATA EXTRACTION (Ver {SCRIPT_VERSION}) ==========')
    logging.info('This script only works on-site or with a VPN connection to Embraer network')
    logging.info('=' * 74)

    # Load environment configuration
    config = cfgEnv()

    # Output file constants
    CSV_FILE = 'getSAR.csv'
    EXCEL_FILE = 'getSAR.xlsm'

    # Check and handle existing Excel file
    checkExcel(EXCEL_FILE)

    if config["debug"]:
        logging.info('>' * 30 + ' DEBUG MODE ' + '<' * 30)             
        logging.info('=' * 73)
        # Create debug directories if needed
        os.makedirs('debug', exist_ok=True)
        os.makedirs('debug/SAR', exist_ok=True)
    
    # Build API URLs for SAR
    # Note: SAR uses a different endpoint path than GTP
    sar_base_url = config["api_base_url"].replace('/gtp', '/sar')
    url_sar = f'{sar_base_url}/sarAPI.cfc?method=getSarList&key={config["api_key"]}&filter={{"programID":"'
    date_filter = f'","dateini":"{config["date_ini"]}","dateEnd":"{config["date_end"]}"' + '}'    

    # Log configuration details
    logging.info(f'Start date: {config["date_ini"]}')
    logging.info(f'End date: {config["date_end"]}')
    logging.info(f'Environment: {config["environment"]}')
    logging.info(f'Base URL: {config["api_base_url"]}')
    logging.info(f'Timeout: {config["timeout"]}s')
    logging.info(f'Max retries: {config["max_retries"]}')
    logging.info("Processing may take several minutes. Please wait...")
    
    # Test network connectivity
    cfgConn(config['api_base_url'], config['api_key'], config)
    
    # ===============================================================
    # Data Retrieval Phase
    # ===============================================================
 
    # Retrieve program list from API
    program_table = dfGetProgram(
        config['api_base_url'], 
        config['api_key'], 
        config['debug'],
        config['timeout'], 
        config['max_retries']
    )
    
    # Fetch SAR data for all programs
    final_data = dfGetSARData(url_sar, date_filter, program_table, config)
    
    # ===============================================================
    # Data Processing Phase
    # ===============================================================
    
    # Apply SAR-specific date corrections
    final_data = corrige_datas_sar(final_data)
    
    # ===============================================================
    # Export Phase
    # ===============================================================
    
    # Export to CSV format
    logging.info(f"Saving {len(final_data)} records to CSV file: {CSV_FILE}")
    final_data.to_csv(CSV_FILE, index=False, encoding='utf-8')

    # Prepare data for Excel export
    final_data = dfReorder(final_data)
    
    # Export to Excel format with formatting
    dfExportExcel(final_data, EXCEL_FILE, config["api_key"])
    
    # ===============================================================
    # Summary Report
    # ===============================================================
    
    # Calculate and display execution time
    total_time = datetime.now() - start_time
    logging.info(f"Processing completed successfully!")
    logging.info(f"Total records: {len(final_data):,}")
    logging.info(f"Total execution time: {str(total_time).split('.')[0]}")


if __name__ == "__main__":
    main()