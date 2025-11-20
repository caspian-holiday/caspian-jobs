#!/usr/bin/env python3
"""
Entry point for running extractor job as a module.
Usage: python -m jobs.extractor
"""

if __name__ == "__main__":
    from .extractor import main
    main()
