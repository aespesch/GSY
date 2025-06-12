"""
__init__.py
GSY Package - Data extraction tools for BDI and Technical Reports.

This package provides modular components for extracting and processing data from
Embraer's BDI (Base de Dados Integrada) and Technical Reports systems.

Modules:
- gsy_cfg: Configuration and environment management
- gsy_map: Program mapping functionality
- gsy_debug: Debug file writing utilities
- gsy_data: API communication and data retrieval
- gsy_network: Network connectivity testing
- gsy_export: Data export and Excel formatting

Version: 1.0.5
"""

__version__ = "1.0.5"

# Configuration functions
from .gsy_cfg import cfgLog, cfgEnv

# Mapping functions
from .gsy_map import cfgMap, getMap, applyMap

# Debug functions
from .gsy_debug import dbgWrite

# Data retrieval functions
from .gsy_data import dfGetData, parseJSON, dfGetProgram, dfGetGSYData

# Network functions
from .gsy_network import cfgConn

# Export functions
from .gsy_export import dfReorder, dfCheck, dfExportExcel, checkExcel

__all__ = [
    # Configuration
    'cfgLog',
    'cfgEnv',
    # Mapping
    'cfgMap',
    'getMap',
    'applyMap',
    # Debug
    'dbgWrite',
    # Data retrieval
    'dfGetData',
    'parseJSON',
    'dfGetProgram',
    'dfGetGSYData',
    # Network
    'cfgConn',
    # Export
    'dfReorder',
    'dfCheck',
    'dfExportExcel',
    'checkExcel'
]