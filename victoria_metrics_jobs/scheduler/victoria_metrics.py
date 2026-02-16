#!/usr/bin/env python3
"""
Victoria Metrics manager for providing Victoria Metrics URL.
"""

import logging
from typing import Dict, Any


class VictoriaMetricsManager:
    """Provides Victoria Metrics URL from configuration."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Victoria Metrics manager.
        
        Args:
            config: Victoria Metrics configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._base_url = self._build_base_url()
    
    def _build_base_url(self) -> str:
        """Build Victoria Metrics base URL from config.
        
        Returns:
            Victoria Metrics base URL
        """
        url = self.config.get('url', 'http://localhost:8428')
        # Ensure URL doesn't end with slash
        return url.rstrip('/')
    
    def get_base_url(self) -> str:
        """Get the Victoria Metrics base URL.
        
        Returns:
            Victoria Metrics base URL
        """
        return self._base_url
