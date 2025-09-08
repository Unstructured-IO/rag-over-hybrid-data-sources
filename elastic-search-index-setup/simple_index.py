#!/usr/bin/env python3
"""
Simple indexing script to test individual document indexing
"""

import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
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

print("🔍 Testing individual document indexing...")

# Generate one simple test document
fake = Faker(['en_US'])
Faker.seed(42)

test_doc = {
    "id": str(uuid.uuid4()),
    "timestamp": datetime.now().isoformat(),
    "customer_name": fake.name(),
    "sales_representative": "Test Rep",
    "product_line": "SoundSport",
    "product_model": "SoundSport Free",
    "price": 149,
    "retailer": "Best Buy",
    "location_city": "New York, NY",
    "region": "Northeast",
    "interaction_type": "purchase_inquiry",
    "interaction_text": f"Test customer {fake.name()} inquired about SoundSport Free for $149.",
    "text": f"Test customer {fake.name()} inquired about SoundSport Free for $149.",
    "quarter": "Q4",
    "year": 2024,
    "month": "September",
    "day_of_week": "Sunday"
}

try:
    # Try to index one document
    response = es.index(
        index="sales-records",
        id=test_doc["id"],
        body=test_doc
    )
    print(f"✅ Successfully indexed test document!")
    print(f"   Document ID: {response['_id']}")
    print(f"   Result: {response['result']}")
    
    # Verify it's there
    count_response = es.count(index="sales-records")
    print(f"📊 Total documents in index: {count_response['count']}")
    
except Exception as e:
    print(f"❌ Failed to index test document: {e}")
    print(f"   Error type: {type(e).__name__}") 