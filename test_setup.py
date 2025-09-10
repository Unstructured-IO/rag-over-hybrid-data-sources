#!/usr/bin/env python3
"""
Setup Verification Script for Hybrid RAG Pipeline

This script tests that all dependencies and environment variables 
are properly configured before running the main pipeline.
"""

import os
import sys
from dotenv import load_dotenv

def test_dependencies():
    """Test that all required Python packages are installed."""
    print("🔍 Testing Python Dependencies...")
    
    dependencies = [
        ("unstructured_client", "unstructured-client"),
        ("dotenv", "python-dotenv"),
        ("os", "built-in"),
        ("sys", "built-in"),
        ("time", "built-in"),
        ("json", "built-in"),
        ("pathlib", "built-in")
    ]
    
    all_good = True
    for module, package in dependencies:
        try:
            __import__(module)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - Run: pip install {package}")
            all_good = False
    
    return all_good

def test_environment_variables():
    """Test that all required environment variables are configured."""
    print("\n🔍 Testing Environment Variables...")
    
    load_dotenv()
    
    required_vars = {
        "AWS_ACCESS_KEY_ID": "AWS Access Key ID",
        "AWS_SECRET_ACCESS_KEY": "AWS Secret Access Key", 
        "UNSTRUCTURED_API_KEY": "Unstructured API Key",
        "ELASTICSEARCH_HOST": "Elasticsearch Host URL",
        "ELASTICSEARCH_API_KEY": "Elasticsearch API Key"
    }
    
    optional_vars = {
        "AWS_REGION": "us-east-1",
        "S3_SOURCE_BUCKET": "example-data-bose-headphones",
        "S3_DESTINATION_BUCKET": "example-data-bose-headphones", 
        "S3_OUTPUT_PREFIX": "output/",
        "ELASTICSEARCH_INDEX": "sales-records"
    }
    
    all_good = True
    
    print("  Required Variables:")
    for var, description in required_vars.items():
        value = os.getenv(var, "NOT_SET")
        if value == "NOT_SET":
            print(f"    ❌ {var}: NOT SET")
            all_good = False
        elif value.startswith("your-"):
            print(f"    ⚠️  {var}: PLACEHOLDER (update with real {description})")
            all_good = False
        else:
            print(f"    ✅ {var}: CONFIGURED")
    
    print("  Optional Variables:")
    for var, default in optional_vars.items():
        value = os.getenv(var, default)
        print(f"    ℹ️  {var}: {value}")
    
    return all_good

def test_script_syntax():
    """Test that the main script has valid syntax."""
    print("\n🔍 Testing Script Syntax...")
    
    try:
        import ast
        with open("hybrid_rag_pipeline.py", "r") as f:
            source = f.read()
        ast.parse(source)
        print("  ✅ hybrid_rag_pipeline.py syntax is valid")
        return True
    except SyntaxError as e:
        print(f"  ❌ Syntax error in hybrid_rag_pipeline.py: {e}")
        return False
    except FileNotFoundError:
        print("  ❌ hybrid_rag_pipeline.py not found")
        return False

def main():
    """Run all setup verification tests."""
    print("🚀 Hybrid RAG Pipeline - Setup Verification")
    print("=" * 50)
    
    # Run all tests
    deps_ok = test_dependencies()
    env_ok = test_environment_variables() 
    syntax_ok = test_script_syntax()
    
    print("\n" + "=" * 50)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 50)
    
    if deps_ok and env_ok and syntax_ok:
        print("🎉 ALL TESTS PASSED!")
        print("\n✅ Your environment is ready to run the hybrid RAG pipeline.")
        print("🚀 Run: python hybrid_rag_pipeline.py")
    else:
        print("⚠️  SOME TESTS FAILED")
        print("\n📝 Next steps:")
        
        if not deps_ok:
            print("   1. Install missing dependencies: pip install -r requirements.txt")
        
        if not env_ok:
            print("   2. Update .env file with your actual credentials:")
            print("      • AWS_ACCESS_KEY_ID=your-actual-aws-access-key")
            print("      • AWS_SECRET_ACCESS_KEY=your-actual-aws-secret-key") 
            print("      • UNSTRUCTURED_API_KEY=your-actual-unstructured-api-key")
            print("      • ELASTICSEARCH_HOST=your-actual-elasticsearch-host")
            print("      • ELASTICSEARCH_API_KEY=your-actual-elasticsearch-api-key")
        
        if not syntax_ok:
            print("   3. Fix syntax errors in hybrid_rag_pipeline.py")
        
        print("\n   Then run this test again: python test_setup.py")

if __name__ == "__main__":
    main() 