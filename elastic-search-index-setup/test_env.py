#!/usr/bin/env python3
"""
Test script to verify .env file loading and API key import
"""

import os
from dotenv import load_dotenv

print("🔍 Testing .env file loading...")
print("=" * 50)

# Load environment variables from base directory (one level up)
print("📁 Loading .env from: ../.env")
result = load_dotenv(dotenv_path="../.env")
print(f"✅ load_dotenv() returned: {result}")

# Check if API key is loaded
api_key = os.getenv('ELASTIC_API_KEY')
username = os.getenv('ELASTIC_USERNAME')
password = os.getenv('ELASTIC_PASSWORD')

print(f"\n🔑 Environment Variables:")
print(f"   ELASTIC_API_KEY exists: {api_key is not None}")
if api_key:
    # Show first and last 10 characters for security
    masked_key = f"{api_key[:10]}...{api_key[-10:]}" if len(api_key) > 20 else api_key
    print(f"   ELASTIC_API_KEY: {masked_key}")
    print(f"   ELASTIC_API_KEY length: {len(api_key)} characters")
else:
    print(f"   ELASTIC_API_KEY: None")

print(f"   ELASTIC_USERNAME: {username}")
print(f"   ELASTIC_PASSWORD: {'***' if password else None}")

# Test basic connection setup (without actually connecting)
print(f"\n🔧 Connection Test Setup:")
try:
    from elasticsearch import Elasticsearch
    
    if api_key:
        print("   ✅ Would use API key authentication")
        print(f"   🌐 Endpoint: https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443")
        
        # Create client object (but don't test connection yet)
        es = Elasticsearch(
            "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
            api_key=api_key,
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )
        print("   ✅ Elasticsearch client created successfully")
        
        # Now test the actual connection
        print("\n🔍 Testing actual connection...")
        try:
            info = es.info()
            print(f"   ✅ Connection successful!")
            print(f"   📊 Cluster name: {info['cluster_name']}")
            print(f"   🔢 Version: {info['version']['number']}")
        except Exception as e:
            print(f"   ❌ Connection failed: {e}")
            print(f"   🔍 Error type: {type(e).__name__}")
        
    else:
        print("   ❌ No API key found - cannot test connection")
        
except ImportError:
    print("   ❌ elasticsearch package not installed")
except Exception as e:
    print(f"   ❌ Error setting up client: {e}")

print(f"\n" + "=" * 50) 