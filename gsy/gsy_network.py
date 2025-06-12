"""
gsy_network.py
Network connectivity and diagnostics module for GSY data extraction system.

This module provides network connectivity testing and diagnostic capabilities
to help troubleshoot connection issues before starting the main data
extraction process. It performs quick and comprehensive connection tests
with detailed feedback.

Version: 1.0.5
"""

import logging
from typing import Dict, Any
from urllib.parse import urlparse
from .gsy_data import dfGetData

# Network test configuration
QUICK_TEST_TIMEOUT = 20  # Seconds for initial quick test
QUICK_TEST_RETRIES = 1   # Minimal retries for quick test

def cfgConn(api_base_url: str, api_key: str, config: Dict[str, Any]) -> None:
    """
    Test API connectivity and provide detailed diagnostic information.
    
    This function performs a two-stage connectivity test:
    1. Quick test with minimal timeout to get fast feedback
    2. Full test with configured settings if quick test fails
    
    The function provides detailed diagnostics to help users troubleshoot
    connection issues, including VPN status, timeout settings, and server
    availability.
    
    Args:
        api_base_url: Base URL for API endpoints
        api_key: API authentication key
        config: Configuration dictionary containing timeout and retry settings
    
    Returns:
        None
        
    Note:
        This function logs diagnostic information but does not raise exceptions.
        The goal is to provide helpful feedback for troubleshooting.
    """
    logging.info("Testing API connectivity...")
    
    # Build test URL using program list endpoint
    test_url = f'{api_base_url}/groundTestProposalAPI.cfc?method=getProgramList&key={api_key}'
    
    # Extract domain for diagnostics
    parsed_url = urlparse(api_base_url)
    domain = parsed_url.netloc
    
    # Stage 1: Quick connectivity test
    logging.info(f"Performing quick connectivity test to {domain}...")
    quick_response = dfGetData(
        test_url, 
        timeout=QUICK_TEST_TIMEOUT, 
        max_retries=QUICK_TEST_RETRIES, 
        silent=True
    )
 
    if quick_response is not None:
        logging.info("✓ Connection test successful (quick response)")
        logging.info(f"  Server responded within {QUICK_TEST_TIMEOUT} seconds")
        _log_response_details(quick_response)
        return
    
    # Stage 2: Full connectivity test with configured settings
    logging.info("Quick test failed. Trying with full timeout settings...")
    logging.info(f"  Timeout: {config['timeout']} seconds")
    logging.info(f"  Max retries: {config['max_retries']}")
    
    full_response = dfGetData(
        test_url, 
        timeout=config['timeout'], 
        max_retries=config['max_retries'], 
        silent=True
    )
    
    if full_response is not None:
        logging.info("✓ Connection test successful with extended timeout")
        logging.info("  → Network is functional but may be experiencing delays")
        logging.info("  → Consider the following:")
        logging.info("    • Server might be under heavy load")
        logging.info("    • Network latency might be high")
        logging.info("    • VPN connection might be slow")
        _log_response_details(full_response)
        return
    
    # Both tests failed - provide comprehensive diagnostics
    logging.warning("✗ Connection test failed")
    _provide_diagnostics(api_base_url, domain, config)

def _log_response_details(response) -> None:
    """
    Log details about successful response for diagnostics.
    
    Args:
        response: HTTP response object
    """
    try:
        # Log response status
        logging.info(f"  HTTP Status: {response.status_code}")
        
        # Log response time if available
        if hasattr(response, 'elapsed'):
            logging.info(f"  Response time: {response.elapsed.total_seconds():.2f} seconds")
        
        # Check content type
        content_type = response.headers.get('Content-Type', 'Unknown')
        logging.info(f"  Content-Type: {content_type}")
        
    except Exception as e:
        logging.debug(f"Could not log response details: {e}")

def _provide_diagnostics(api_base_url: str, domain: str, config: Dict[str, Any]) -> None:
    """
    Provide detailed diagnostic information for connection failures.
    
    Args:
        api_base_url: Base API URL
        domain: Extracted domain name
        config: Configuration dictionary
    """
    logging.info("\nDiagnostic Information:")
    logging.info("=" * 50)
    
    # Connection details
    logging.info("Connection Details:")
    logging.info(f"  • API Domain: {domain}")
    logging.info(f"  • Environment: {config.get('environment', 'Unknown')}")
    logging.info(f"  • Base URL: {api_base_url}")
    
    # Configuration details
    logging.info("\nConfiguration:")
    logging.info(f"  • Timeout: {config['timeout']} seconds")
    logging.info(f"  • Max retries: {config['max_retries']}")
    logging.info(f"  • Debug mode: {'Enabled' if config.get('debug') else 'Disabled'}")
    
    # Common issues and solutions
    logging.info("\nPossible Issues:")
    logging.info("  1. VPN Connection")
    logging.info("     • Ensure FortiClient VPN is connected to Embraer network")
    logging.info("     • Check VPN stability and reconnect if necessary")
    
    logging.info("  2. Server Availability")
    logging.info("     • Server might be under maintenance")
    logging.info("     • Server might be experiencing high load")
    logging.info("     • Try again during off-peak hours (early morning/late evening)")
    
    logging.info("  3. Network Configuration")
    logging.info("     • Check firewall settings")
    logging.info("     • Verify proxy configuration if applicable")
    logging.info("     • Test internet connectivity")
    
    logging.info("  4. API Configuration")
    logging.info("     • Verify API_KEY is correct for the environment")
    logging.info("     • Ensure using correct environment (QAS vs Production)")
    
    # Recommendations
    logging.info("\nRecommended Actions:")
    logging.info("  1. Verify VPN connection is active")
    logging.info("  2. Increase timeout in .env file:")
    logging.info("     API_TIMEOUT=300")
    logging.info("  3. Increase retry attempts:")
    logging.info("     MAX_RETRIES=10")
    logging.info("  4. Contact IT support if problem persists")
    logging.info("  5. Check with colleagues if they're experiencing similar issues")
    
    logging.info("=" * 50)
