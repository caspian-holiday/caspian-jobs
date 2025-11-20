#!/usr/bin/env python3
"""
Entry point for running apex collector job as a module.
Usage: python -m jobs.apex_collector
"""

if __name__ == "__main__":
    from .apex_collector import main
    main()
