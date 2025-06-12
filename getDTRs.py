"""
getDTRs.py
Main script for fetching BDI (Integrated Database) and Technical Reports data.

This script orchestrates the GSY data extraction process, utilizing modular components
to retrieve data from the GTP (Ground Test Proposal) system and Technical Reports.

The script retrieves data through BDI's API within a specified date range and exports
it in CSV and Excel formats. The Excel format includes program mapping transformations
based on program_group.txt rules.

Requirements:
- API_KEY must be set in .env file for authentication
- Date range (DATE_INI, DATE_END) must be defined in .env file
- VPN connection to Embraer's network is required
- program_group.txt file must exist for mapping functionality

Output:
- BDI_TechReports.csv: Raw data in CSV format
- BDI_TechReports.xlsm: Formatted data with macros
- getDTRs.log: Processing log file

"""

import os
import logging
from datetime import datetime
from pandas import concat

# Script version and system type - MUST be defined before gsy imports
SCRIPT_VERSION = "0.0.5"
SCRIPT_SYSTEM = "DTR"
SCRIPT_ENV = "QAS" #QAS or Production

# Import all necessary functions from the gsy package
# IMPORTANT: SCRIPT_SYSTEM must be defined before these imports
from gsy import (
    cfgLog, cfgEnv, cfgMap, cfgConn,
    dfGetProgram, dfGetGSYData,
    getMap, applyMap,
    dfReorder, dfCheck, dfExportExcel,
    checkExcel
)

def main():
    """
    Main function that orchestrates the entire data retrieval and processing workflow.
    
    This function:
    1. Configures the environment and logging
    2. Retrieves program list from API
    3. Fetches BDI and Technical Reports data
    4. Applies program mapping transformations
    5. Exports data in CSV and Excel formats
    """
    start_time = datetime.now()
    
    # ===============================================================
    # Configuration Phase
    # ===============================================================
    
    # Initialize logging system
    cfgLog()
    logging.info(f'========= BDI AND TECHNICAL REPORTS DATA EXTRACTION (Ver {SCRIPT_VERSION}) ==========')
    logging.info('This script only works on-site or with a VPN connection to Embraer network')
    logging.info('=' * 74)
    
    # Load environment configuration
    config = cfgEnv()

    # Output file constants
    CSV_FILE = 'BDI_TechReports.csv'
    EXCEL_FILE = 'BDI_TechReports.xlsm'
    
    # Check for required mapping file
    cfgMap()

    # Check and handle existing Excel file
    checkExcel(EXCEL_FILE)

    if config["debug"]:
        logging.info('>' * 30 + ' DEBUG MODE ' + '<' * 30)             
        logging.info('=' * 73)
        # Create debug directories if needed
        os.makedirs('debug', exist_ok=True)
        os.makedirs('debug/TechRep', exist_ok=True)
        os.makedirs('debug/BDI', exist_ok=True)
    
    # Build API URLs
    url_bdi = f'{config["api_base_url"]}/groundTestProposalAPI.cfc?method=getBDIDocs&key={config["api_key"]}&filter={{"programID":"'
    url_tecrep = f'{config["api_base_url"]}/groundTestProposalAPI.cfc?method=getTechRepDocs&key={config["api_key"]}&filter={{"programID":"'
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
    
    # Fetch BDI data for all programs
    bdi_data = dfGetGSYData(url_bdi, date_filter, "BDI", program_table, config)
    
    # Fetch Technical Reports data for all programs
    tech_rep_data = dfGetGSYData(url_tecrep, date_filter, "TechRep", program_table, config)
    
    # ===============================================================
    # Data Processing Phase
    # ===============================================================
    
    # Combine both data sources
    logging.info("Combining BDI and Technical Reports data...")
    final_data = concat([bdi_data, tech_rep_data], ignore_index=True)
    
    # Apply program group mapping
    logging.info("Applying program group mapping...")
    program_mapping = getMap('program_group.txt')
    final_data = applyMap(final_data, program_mapping)

    # Validate data and fill error messages
    dfCheck(final_data)
    
    # Merge date fields if necessary
    if 'gtpFinishedDate' in final_data.columns and 'grtpFinishedDate' in final_data.columns:
        logging.info("Merging gtpFinishedDate fields...")
        # Use grtpFinishedDate to fill empty gtpFinishedDate values
        mask = final_data['gtpFinishedDate'].isna() & final_data['grtpFinishedDate'].notna()
        final_data.loc[mask, 'gtpFinishedDate'] = final_data.loc[mask, 'grtpFinishedDate']
        
        # Remove redundant column
        logging.info("Removing grtpFinishedDate column...")
        final_data = final_data.drop(columns=['grtpFinishedDate'])
    
    # ===============================================================
    # Export Phase
    # ===============================================================
    
    # Export to CSV format
    logging.info(f"Saving {len(final_data)} records to CSV file: {CSV_FILE}")
    final_data.to_csv(CSV_FILE, index=False, encoding='utf-8')

    # Export to Excel format with formatting
    final_data = dfReorder(final_data)
    dfExportExcel(final_data, EXCEL_FILE, config["api_key"])
    
    # ===============================================================
    # Summary Report
    # ===============================================================
    
    logging.info("=" * 38)
    logging.info("Data Summary:")
    logging.info(f"BDI records: {len(bdi_data):,}")
    logging.info(f"Technical Reports records: {len(tech_rep_data):,}")
    logging.info(f"TOTAL records: {len(final_data):,}")
    logging.info("=" * 38)
    
    # Calculate and display execution time
    total_time = datetime.now() - start_time
    logging.info(f"Processing completed successfully!")
    logging.info(f"Total execution time: {str(total_time).split('.')[0]}")

if __name__ == "__main__":
    main()