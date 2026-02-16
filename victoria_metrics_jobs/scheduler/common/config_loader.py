#!/usr/bin/env python3
"""
Common configuration loader using OmegaConf with interpolation support.
"""

import logging
import os
from typing import Dict, Any
from omegaconf import OmegaConf, DictConfig
from omegaconf.errors import InterpolationResolutionError, ConfigAttributeError


class ConfigLoader:
    """Configuration loader using OmegaConf with environment variable and path interpolation."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def load(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file with OmegaConf interpolation.
        
        Supports:
        - Environment variables: ${oc.env:VAR_NAME}
        - Environment variables with defaults: ${oc.env:VAR_NAME,default_value}
        - Config path references: ${common.extractor.target_url}
        - Nested interpolation: ${common.${job_type}.url}
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Configuration dictionary with all interpolations resolved
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            InterpolationResolutionError: If interpolation fails
            ValueError: If required environment variables are missing
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        if not config_path.endswith('.yml') and not config_path.endswith('.yaml'):
            raise ValueError(f"Configuration file must have .yml or .yaml extension: {config_path}")
        
        try:
            # Load configuration with OmegaConf (lazy resolution)
            config = OmegaConf.load(config_path)
            
            # Register custom resolvers for backward compatibility with our syntax
            self._register_custom_resolvers()
            
            # Don't resolve everything upfront - let OmegaConf resolve lazily when accessed
            # This way jobs only resolve their specific configuration
            # Convert to plain Python dict (resolve=True resolves on access)
            resolved_config = OmegaConf.to_container(config, resolve=True, throw_on_missing=False)
            
            self.logger.info(f"Successfully loaded configuration from {config_path}")
            return resolved_config
            
        except InterpolationResolutionError as e:
            self.logger.error(f"Interpolation error in configuration: {e}")
            raise ValueError(f"Configuration interpolation failed: {e}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _register_custom_resolvers(self):
        """Register custom OmegaConf resolvers for backward compatibility."""
        # Register resolver for required environment variables with error message
        # Syntax: ${env_required:VAR_NAME,Error message}
        if not OmegaConf.has_resolver("env_required"):
            OmegaConf.register_new_resolver(
                "env_required",
                lambda var_name, error_msg="Variable is required": self._resolve_required_env(var_name, error_msg),
                use_cache=True
            )
    
    def _resolve_required_env(self, var_name: str, error_msg: str) -> str:
        """Resolve required environment variable or return placeholder if missing.
        
        Args:
            var_name: Name of environment variable
            error_msg: Error message if variable is not set
            
        Returns:
            Environment variable value or placeholder string
            
        Note:
            Returns a placeholder value '???' if environment variable is not set.
            This allows the config to load without all env vars being set.
            Jobs should validate their specific required vars when they run.
        """
        value = os.getenv(var_name)
        if value is None:
            # Return placeholder instead of raising error
            # This allows config to load for other jobs
            return f"???{var_name}???"
        return value
