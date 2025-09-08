#!/usr/bin/env python3
"""
Simple script to check if data exists in the sales-records index
Uses only basic read operations that work with limited API key permissions
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

print("🔍 Checking sales-records index...")

try:
    # Simple document count
    count_response = es.count(index="sales-records")
    total_docs = count_response['count']
    print(f"📊 Total documents: {total_docs}")
    
    if total_docs > 0:
        print("\n✅ SUCCESS! Data has been indexed.")
        
        # Get a few sample documents
        print("\n📋 Sample documents:")
        search_response = es.search(
            index="sales-records",
            body={"size": 3, "_source": ["customer_name", "product_model", "price", "retailer", "interaction_text"]},
        )
        
        for i, hit in enumerate(search_response['hits']['hits'], 1):
            doc = hit['_source']
            print(f"\n📄 Document {i}:")
            print(f"   👤 Customer: {doc.get('customer_name', 'N/A')}")
            print(f"   🎧 Product: {doc.get('product_model', 'N/A')}")
            print(f"   💰 Price: ${doc.get('price', 'N/A')}")
            print(f"   🏪 Retailer: {doc.get('retailer', 'N/A')}")
            text = doc.get('interaction_text', 'N/A')
            print(f"   📝 Text: {text[:100]}...")
        
        print(f"\n🎉 Your synthetic Bose sales data is ready!")
        print(f"🔗 Ready to use as Elasticsearch source connector in Unstructured Workflow")
        
    else:
        print("❌ No documents found. The indexing may have failed.")
        
except Exception as e:
    print(f"❌ Error: {e}") 