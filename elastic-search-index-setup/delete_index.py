#!/usr/bin/env python3
"""
Simple script to delete the sales-records index
"""

import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

# Load environment variables from base directory (one level up)
load_dotenv(dotenv_path="../.env")

# Get API key
api_key = os.getenv('ELASTIC_API_KEY')

# Create client
es = Elasticsearch(
    "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
    api_key=api_key,
    request_timeout=60
)

print("🗑️ Deleting sales-records index...")

try:
    # Check if index exists
    if es.indices.exists(index="sales-records"):
        # Delete the index
        es.indices.delete(index="sales-records")
        print("✅ Successfully deleted sales-records index")
    else:
        print("ℹ️ Index sales-records does not exist")
        
except Exception as e:
    print(f"❌ Failed to delete index: {e}")
    print(f"   Error type: {type(e).__name__}") 