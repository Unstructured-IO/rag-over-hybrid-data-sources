#!/usr/bin/env python3
"""
Runner script for the Hybrid RAG Pipeline.
This script imports and executes the pipeline from hybrid_rag_pipeline.py
"""

import sys
import os

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from hybrid_rag_pipeline import main
    
    if __name__ == "__main__":
        print("🚀 Starting Hybrid RAG Pipeline Runner")
        print("=" * 50)
        main()
        
except ImportError as e:
    print(f"❌ Error importing pipeline: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error running pipeline: {e}")
    sys.exit(1)
