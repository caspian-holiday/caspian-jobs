#!/usr/bin/env python3
"""
Result utilities for functional error handling in jobs.
Provides a Result type for functional programming patterns.
"""

from __future__ import annotations

from typing import TypeVar, Callable, Any, Union
from dataclasses import dataclass

T = TypeVar('T')
E = TypeVar('E')


@dataclass
class Result:
    """Result type for functional error handling.
    
    This class provides a functional approach to error handling by wrapping
    either a success value or an error. It supports common functional operations
    like map, and_then, and map_err.
    """
    value: Any = None
    error: Any = None
    
    def __post_init__(self):
        """Validate that either value or error is set, but not both."""
        if self.value is not None and self.error is not None:
            raise ValueError("Result cannot have both value and error")
        if self.value is None and self.error is None:
            raise ValueError("Result must have either value or error")
    
    @property
    def is_ok(self) -> bool:
        """Check if this is a successful result."""
        return self.error is None
    
    @property
    def is_err(self) -> bool:
        """Check if this is an error result."""
        return self.error is not None
    
    def map(self, func: Callable[[T], Any]) -> 'Result':
        """Apply a function to the value if this is a success.
        
        Args:
            func: Function to apply to the value
            
        Returns:
            New Result with transformed value or same error
        """
        if self.is_ok:
            try:
                return Result(value=func(self.value))
            except Exception as e:
                return Result(error=e)
        return self
    
    def map_err(self, func: Callable[[E], Any]) -> 'Result':
        """Apply a function to the error if this is an error.
        
        Args:
            func: Function to apply to the error
            
        Returns:
            New Result with transformed error or same value
        """
        if self.is_err:
            return Result(error=func(self.error))
        return self
    
    def and_then(self, func: Callable[[T], 'Result']) -> 'Result':
        """Chain operations that return Results.
        
        Args:
            func: Function that takes a value and returns a Result
            
        Returns:
            Result from the function or the current error
        """
        if self.is_ok:
            return func(self.value)
        return self
    
    def unwrap(self) -> T:
        """Unwrap the value, raising the error if this is an error result.
        
        Returns:
            The wrapped value
            
        Raises:
            The wrapped error if this is an error result
        """
        if self.is_ok:
            return self.value
        raise self.error
    
    def unwrap_or(self, default: T) -> T:
        """Unwrap the value or return a default if this is an error.
        
        Args:
            default: Default value to return if this is an error
            
        Returns:
            The wrapped value or the default
        """
        if self.is_ok:
            return self.value
        return default
    
    def unwrap_or_else(self, func: Callable[[E], T]) -> T:
        """Unwrap the value or compute a default from the error.
        
        Args:
            func: Function to compute default from error
            
        Returns:
            The wrapped value or computed default
        """
        if self.is_ok:
            return self.value
        return func(self.error)


def Ok(value: T) -> Result:
    """Create a successful Result.
    
    Args:
        value: The success value
        
    Returns:
        Result containing the value
    """
    return Result(value=value)


def Err(error: E) -> Result:
    """Create an error Result.
    
    Args:
        error: The error value
        
    Returns:
        Result containing the error
    """
    return Result(error=error)


def try_catch(func: Callable[[], T]) -> Result:
    """Execute a function and wrap the result or exception in a Result.
    
    Args:
        func: Function to execute
        
    Returns:
        Result containing the return value or exception
    """
    try:
        return Ok(func())
    except Exception as e:
        return Err(e)
