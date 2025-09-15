#!/usr/bin/env python3
"""
Elasticsearch Index Preprocessing Tool

This script handles Elasticsearch index management for the Hybrid RAG Pipeline:
- Creates synthetic sales data for testing
- Manages index creation and deletion
- Downloads and uploads index data locally
- Provides backup and restore functionality

ELASTICSEARCH API KEY SETUP REQUIREMENTS:
========================================
Before running this script, you must create an Elasticsearch API key with the following permissions:

{
  "sales-records-full-access": {
    "cluster": [],
    "indices": [
      {
        "names": [
          "sales-records",
          "sales-records-consolidated",
          "customer-support"
        ],
        "privileges": [
          "create_index",
          "delete_index",
          "manage",
          "write",
          "read",
          "view_index_metadata",
          "monitor"
        ],
        "allow_restricted_indices": false
      }
    ],
    "applications": [],
    "run_as": [],
    "metadata": {},
    "transient_metadata": {
      "enabled": true
    }
  }
}

SETUP INSTRUCTIONS:
==================
1. Log into your Elasticsearch deployment dashboard
2. Go to Security > API Keys
3. Create a new API key with the above permissions
4. Set the following environment variables in your .env file:
   - ELASTICSEARCH_HOST=https://your-deployment.es.io:443
   - ELASTICSEARCH_API_KEY=your-api-key-here

USAGE:
======
- Run as standalone: python elasticsearch_index_preprocessing.py
- Import functions: from elasticsearch_index_preprocessing import run_elasticsearch_preprocessing

"""
import os
import sys
import time

import json
import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from faker import Faker
from dotenv import load_dotenv


# Import Elasticsearch for preprocessing
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk, BulkIndexError
except ImportError:
    print("❌ elasticsearch package not found. Install with: pip install elasticsearch")
    sys.exit(1)


# Elasticsearch Configuration
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")  # Updated to use consolidated index


# ============================================================================
# ELASTICSEARCH PREPROCESSING FUNCTIONS
# ============================================================================

def get_elasticsearch_client():
    """Get Elasticsearch client with API key authentication"""
    return Elasticsearch(
        ELASTICSEARCH_HOST,
        api_key=ELASTICSEARCH_API_KEY,
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True
    )

def delete_elasticsearch_indices():
    """Delete the contents of sales-records, sales-records-consolidated, and customer-support indices"""
    print("🗑️ Deleting Elasticsearch indices...")
    es = get_elasticsearch_client()
    
    indices_to_delete = ["sales-records", "sales-records-consolidated", "customer-support"]
    
    for index_name in indices_to_delete:
        try:
            if es.indices.exists(index=index_name):
                es.indices.delete(index=index_name)
                print(f"  ✅ Successfully deleted {index_name} index")
            else:
                print(f"  ℹ️ Index {index_name} does not exist")
        except Exception as e:
            print(f"  ❌ Failed to delete index {index_name}: {e}")

class BoseSalesDataGenerator:
    """Generate NER-rich synthetic sales data for Bose products"""
    
    def __init__(self):
        self.fake = Faker(['en_US'])
        Faker.seed(42)  # For reproducible data
        
        # Bose product data
        self.products = {
            'SoundSport': {
                'models': ['SoundSport Free', 'SoundSport Wireless', 'SoundSport Pulse'],
                'price_range': (129, 199),
                'category': 'Sports Earbuds',
                'features': ['sweat-resistant', 'secure fit', 'wireless', 'noise isolation']
            },
            'OpenAudio': {
                'models': ['OpenAudio Sport', 'OpenAudio Ultra', 'OpenAudio Pro'],
                'price_range': (149, 249),
                'category': 'Open-Ear Audio',
                'features': ['open-ear design', 'situational awareness', 'comfortable fit', 'premium audio']
            },
            'QuietComfort': {
                'models': ['QuietComfort 45', 'QuietComfort Ultra', 'QuietComfort Earbuds'],
                'price_range': (199, 429),
                'category': 'Noise Cancelling',
                'features': ['world-class noise cancellation', 'premium comfort', 'long battery life', 'crystal clear calls']
            }
        }
        
        # Rich entity data for NER
        self.retailers = [
            "Best Buy", "Target", "Amazon", "Walmart", "Costco", "B&H Photo",
            "Guitar Center", "Sam's Club", "Newegg", "Adorama", "Crutchfield"
        ]
        
        self.sales_reps = [
            "Jennifer Martinez", "Michael Chen", "Sarah Johnson", "David Rodriguez",
            "Emily Wilson", "Robert Taylor", "Lisa Anderson", "James Thompson",
            "Maria Garcia", "Christopher Lee", "Amanda Davis", "Daniel Brown"
        ]
        
        self.regions = [
            "Northeast", "Southeast", "Midwest", "Southwest", "West Coast",
            "Pacific Northwest", "Mountain West", "Great Lakes", "Mid-Atlantic", "Gulf Coast"
        ]
        
        self.cities = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
            "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "Austin, TX",
            "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Charlotte, NC", "Seattle, WA",
            "Denver, CO", "Washington, DC", "Boston, MA", "Nashville, TN", "Detroit, MI"
        ]
        
        self.interaction_types = [
            "purchase_inquiry", "product_comparison", "pricing_discussion", 
            "sales_consultation", "order_processing", "upsell_opportunity",
            "customer_preferences", "warranty_inquiry", "bulk_order_request",
            "promotional_campaign", "seasonal_sale", "loyalty_program_enrollment"
        ]
    
    def generate_sales_record(self) -> Dict[str, Any]:
        """Generate a single NER-rich sales record"""
        
        # Select random product
        product_line = random.choice(list(self.products.keys()))
        product_info = self.products[product_line]
        model = random.choice(product_info['models'])
        price = random.randint(*product_info['price_range'])
        
        # Generate rich entities for NER extraction
        customer_name = self.fake.name()
        sales_rep = random.choice(self.sales_reps)
        retailer = random.choice(self.retailers)
        city = random.choice(self.cities)
        region = random.choice(self.regions)
        
        # Generate realistic interaction text with rich named entities
        interaction_type = random.choice(self.interaction_types)
        
        # Create contextual sales interaction text
        interaction_texts = {
            "purchase_inquiry": f"Customer {customer_name} from {city} called to inquire about purchasing the {model}. Sales rep {sales_rep} provided detailed product information and quoted ${price}. Customer is comparing with similar products at {retailer}.",
            
            "product_comparison": f"{sales_rep} helped {customer_name} compare the {model} against competitors. Discussed the superior noise cancellation technology and ${price} price point. Customer mentioned they saw it at {retailer} for a higher price.",
            
            "sales_consultation": f"Consultation session with {customer_name} in {region} region. {sales_rep} recommended the {model} based on customer's active lifestyle needs. Discussed ${price} pricing and available financing options through {retailer}.",
            
            "order_processing": f"Order processed for {customer_name}: 2 units of {model} at ${price} each. Shipping to {city}. Sales rep {sales_rep} confirmed delivery timeline and warranty coverage. Partner retailer: {retailer}.",
            
            "promotional_campaign": f"Q4 promotional campaign in {region}: {sales_rep} contacted {customer_name} about special pricing on {model}. Limited time offer at ${price - 20} (originally ${price}). Customer interested, will visit {retailer} this weekend."
        }
        
        # Select appropriate interaction text or generate generic one
        if interaction_type in interaction_texts:
            interaction_text = interaction_texts[interaction_type]
        else:
            interaction_text = f"{sales_rep} assisted {customer_name} with {interaction_type.replace('_', ' ')} for {model}. Discussed ${price} pricing and availability at {retailer} in {city}."
        
        # Generate timestamp from 2016 onwards (products launched after 2015)
        start_date = datetime(2016, 1, 1)
        random_date = self.fake.date_time_between(start_date=start_date, end_date='now')
        
        return {
            "id": str(uuid.uuid4()),
            "timestamp": random_date.isoformat(),
            "customer_name": customer_name,
            "sales_representative": sales_rep,
            "product_line": product_line,
            "product_model": model,
            "product_category": product_info['category'],
            "price": price,
            "retailer": retailer,
            "location_city": city,
            "region": region,
            "interaction_type": interaction_type,
            "interaction_text": interaction_text,
            "text": interaction_text,  # Duplicate for semantic_text field
            "quarter": f"Q{random_date.month//3 + 1}",
            "year": random_date.year,
            "month": random_date.strftime("%B"),
            "day_of_week": random_date.strftime("%A"),
            "customer_segment": random.choice(["Consumer", "Business", "Education", "Government"]),
            "sales_channel": random.choice(["Direct", "Retail Partner", "Online", "Phone"]),
            "lead_source": random.choice(["Website", "Advertisement", "Referral", "Trade Show", "Cold Call"]),
            "deal_stage": random.choice(["Prospect", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]),
            "revenue_potential": price * random.randint(1, 5),  # Potential for multiple units
            "customer_priority": random.choice(["High", "Medium", "Low"]),
            "follow_up_required": random.choice([True, False]),
            "notes": f"Additional context: Customer expressed interest in {product_line} series. {sales_rep} to follow up within 48 hours."
        }

def create_nonconsolidated_index():
    """Create the non-consolidated sales-records index with synthetic data"""
    print("🔧 Creating non-consolidated sales-records index...")
    es = get_elasticsearch_client()
    index_name = "sales-records"
    
    try:
        # Define mapping optimized for NER entities and search
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "ner_analyzer": {
                            "type": "standard",
                            "stopwords": "_none_"  # Keep all words for NER
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "customer_name": {
                        "type": "text",
                        "analyzer": "ner_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "sales_representative": {
                        "type": "text",
                        "analyzer": "ner_analyzer", 
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "product_line": {"type": "keyword"},
                    "product_model": {"type": "keyword"},
                    "product_category": {"type": "keyword"},
                    "price": {"type": "float"},
                    "retailer": {
                        "type": "text",
                        "analyzer": "ner_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "location_city": {
                        "type": "text",
                        "analyzer": "ner_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "region": {"type": "keyword"},
                    "interaction_type": {"type": "keyword"},
                    "interaction_text": {
                        "type": "text",
                        "analyzer": "ner_analyzer"  # Full text optimized for NER
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "ner_analyzer"  # Duplicate text field for compatibility
                    },
                    "quarter": {"type": "keyword"},
                    "year": {"type": "integer"},
                    "month": {"type": "keyword"},
                    "day_of_week": {"type": "keyword"},
                    "customer_segment": {"type": "keyword"},
                    "sales_channel": {"type": "keyword"},
                    "lead_source": {"type": "keyword"},
                    "deal_stage": {"type": "keyword"},
                    "revenue_potential": {"type": "float"},
                    "customer_priority": {"type": "keyword"},
                    "follow_up_required": {"type": "boolean"},
                    "notes": {
                        "type": "text",
                        "analyzer": "ner_analyzer"
                    }
                }
            }
        }
        
        # Create index
        es.indices.create(index=index_name, body=mapping)
        print(f"  ✅ Created index: {index_name}")
        
        # Generate and index synthetic data
        generator = BoseSalesDataGenerator()
        print("  🔄 Generating 100 synthetic sales records...")
        
        records = []
        for i in range(100):
            records.append(generator.generate_sales_record())
        
        print(f"  ✅ Generated {len(records)} records")
        
        # Bulk index
        print("  📤 Bulk indexing records...")
        actions = []
        for record in records:
            action = {
                "_index": index_name,
                "_id": record["id"],
                "_source": record
            }
            actions.append(action)
        
        success_count, failed_items = bulk(
            es, 
            actions, 
            chunk_size=100,
            request_timeout=60,
            max_retries=3,
            initial_backoff=2,
            max_backoff=600
        )
        
        print(f"  ✅ Successfully indexed {success_count} records")
        
        # Refresh index to make documents searchable immediately
        es.indices.refresh(index=index_name)
        print(f"  🔄 Refreshed index to make documents searchable")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating non-consolidated index: {e}")
        return False

class ConsolidatedBoseSalesDataGenerator:
    """Generate consolidated sales data optimized for RAG processing"""
    
    def __init__(self):
        self.fake = Faker(['en_US'])
        Faker.seed(42)  # For reproducible data
        
        # Bose product data
        self.products = {
            'SoundSport': {
                'models': ['SoundSport Free', 'SoundSport Wireless', 'SoundSport Pulse'],
                'price_range': (129, 199),
                'category': 'Sports Earbuds',
                'features': ['sweat-resistant', 'secure fit', 'wireless', 'noise isolation']
            },
            'OpenAudio': {
                'models': ['OpenAudio Sport', 'OpenAudio Ultra', 'OpenAudio Pro'],
                'price_range': (149, 249),
                'category': 'Open-Ear Audio',
                'features': ['open-ear design', 'situational awareness', 'comfortable fit', 'premium audio']
            },
            'QuietComfort': {
                'models': ['QuietComfort 45', 'QuietComfort Ultra', 'QuietComfort Earbuds'],
                'price_range': (199, 429),
                'category': 'Noise Cancelling',
                'features': ['world-class noise cancellation', 'premium comfort', 'long battery life', 'crystal clear calls']
            }
        }
        
        # Rich entity data for NER
        self.retailers = [
            "Best Buy", "Target", "Amazon", "Walmart", "Costco", "B&H Photo",
            "Guitar Center", "Sam's Club", "Newegg", "Adorama", "Crutchfield"
        ]
        
        self.sales_reps = [
            "Jennifer Martinez", "Michael Chen", "Sarah Johnson", "David Rodriguez",
            "Emily Wilson", "Robert Taylor", "Lisa Anderson", "James Thompson",
            "Maria Garcia", "Christopher Lee", "Amanda Davis", "Daniel Brown"
        ]
        
        self.regions = [
            "Northeast", "Southeast", "Midwest", "Southwest", "West Coast",
            "Pacific Northwest", "Mountain West", "Great Lakes", "Mid-Atlantic", "Gulf Coast"
        ]
        
        self.cities = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
            "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "Austin, TX",
            "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Charlotte, NC", "Seattle, WA",
            "Denver, CO", "Washington, DC", "Boston, MA", "Nashville, TN", "Detroit, MI"
        ]
        
        self.interaction_types = [
            "purchase_inquiry", "product_comparison", "pricing_discussion", 
            "sales_consultation", "order_processing", "upsell_opportunity",
            "customer_preferences", "warranty_inquiry", "bulk_order_request",
            "promotional_campaign", "seasonal_sale", "loyalty_program_enrollment"
        ]

    def consolidate_from_source_record(self, source_record: dict) -> dict:
        """Convert a source record to consolidated format"""
        
        # Extract data from source record
        product_line = source_record['product_line']
        product_info = self.products.get(product_line, {'category': 'Unknown', 'features': []})
        
        # Parse timestamp
        timestamp = datetime.fromisoformat(source_record['timestamp'].replace('Z', '+00:00'))
        
        # Generate features based on product line
        if product_line in self.products:
            features = random.sample(self.products[product_line]['features'], k=random.randint(1, 3))
        else:
            features = ['premium quality', 'reliable performance']
        
        # Create detailed interaction text based on the original interaction
        interaction_details = self._generate_interaction_text(
            source_record['interaction_type'], 
            source_record['customer_name'], 
            source_record['sales_representative'], 
            source_record['product_model'], 
            source_record['price'], 
            source_record['retailer'], 
            source_record['location_city'], 
            features
        )
        
        # Create consolidated text field with ALL context
        consolidated_text = f"""SALES RECORD - {timestamp.strftime('%B %d, %Y')}

Customer Information:
- Name: {source_record['customer_name']}
- Location: {source_record['location_city']}
- Segment: {source_record['customer_segment']}
- Lead Source: {source_record['lead_source']}

Product Details:
- Product Line: {source_record['product_line']}
- Model: {source_record['product_model']}
- Category: {source_record['product_category']}
- Price: ${source_record['price']}
- Key Features: {', '.join(features)}

Sales Information:
- Sales Representative: {source_record['sales_representative']}
- Retailer: {source_record['retailer']}
- Region: {source_record['region']}
- Channel: {source_record['sales_channel']}
- Deal Stage: {source_record['deal_stage']}
- Interaction Type: {source_record['interaction_type'].replace('_', ' ').title()}

Conversation Summary:
{interaction_details}

Temporal Context:
- Date: {timestamp.strftime('%B %d, %Y')}
- Quarter: {source_record['quarter']} {source_record['year']}
- Day of Week: {source_record['day_of_week']}

Revenue Information:
- Unit Price: ${source_record['price']}
- Potential Deal Value: ${source_record['revenue_potential']}
- Priority: {source_record['customer_priority']}""".strip()
        
        # Return consolidated document
        return {
            "id": str(uuid.uuid4()),
            "timestamp": source_record['timestamp'],
            "document_type": "sales_record",
            "product_line": source_record['product_line'],
            "region": source_record['region'],
            "consolidated_text": consolidated_text,
            "record_date": timestamp.strftime('%Y-%m-%d'),
            "quarter": source_record['quarter'],
            "year": source_record['year']
        }
    
    def _generate_interaction_text(self, interaction_type, customer_name, sales_rep, model, price, retailer, city, features):
        """Generate detailed interaction text based on type"""
        
        interaction_templates = {
            "purchase_inquiry": f"Customer {customer_name} from {city} contacted {sales_rep} to inquire about purchasing the {model}. The customer was particularly interested in the {', '.join(features[:2])} features. {sales_rep} provided detailed product specifications and quoted ${price}. Customer mentioned they had seen similar products at {retailer} but was impressed with the Bose quality and features.",
            
            "product_comparison": f"{sales_rep} conducted a comprehensive product comparison session with {customer_name}. The customer was deciding between the {model} and competitor products. Key selling points discussed included {', '.join(features)} which differentiate Bose from competitors. The ${price} price point was justified through superior audio quality and build reliability. Customer appreciated the detailed comparison and is considering the purchase.",
            
            "sales_consultation": f"In-depth consultation with {customer_name} in the {city} area. {sales_rep} assessed customer needs and recommended the {model} based on their lifestyle and audio preferences. Highlighted features included {', '.join(features)}. Discussed ${price} pricing structure and available financing options. Customer showed strong interest and requested follow-up information.",
            
            "order_processing": f"Order successfully processed for {customer_name}: {random.randint(1, 3)} units of {model} at ${price} each. {sales_rep} confirmed shipping details to {city} and explained warranty coverage. Customer opted for expedited shipping and was provided with tracking information. Partnership with {retailer} ensured competitive pricing and reliable delivery.",
            
            "promotional_campaign": f"Q{random.randint(1,4)} promotional outreach to {customer_name}. {sales_rep} presented special pricing on {model} - limited time offer at ${price - random.randint(10, 30)} (regularly ${price}). Emphasized exclusive features: {', '.join(features)}. Customer expressed interest and plans to visit {retailer} location this weekend to experience the product firsthand.",
        }
        
        return interaction_templates.get(
            interaction_type, 
            f"{sales_rep} assisted {customer_name} with {interaction_type.replace('_', ' ')} regarding {model}. Discussed ${price} pricing and key features: {', '.join(features)}. Customer interaction was positive and follow-up scheduled."
        )

def create_consolidated_index():
    """Pull from sales-records and create consolidated sales-records-consolidated index"""
    print("🔧 Creating consolidated sales-records-consolidated index...")
    es = get_elasticsearch_client()
    source_index = "sales-records"
    target_index = "sales-records-consolidated"
    
    try:
        # Check if source index exists
        if not es.indices.exists(index=source_index):
            print(f"  ❌ Source index {source_index} does not exist")
            return False
        
        # Create target index mapping
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "document_type": {"type": "keyword"},
                    "product_line": {"type": "keyword"},
                    "region": {"type": "keyword"},
                    "consolidated_text": {
                        "type": "text",
                        "analyzer": "standard"  # This is the field Unstructured will process
                    },
                    "record_date": {"type": "date"},
                    "quarter": {"type": "keyword"},
                    "year": {"type": "integer"}
                }
            }
        }
        
        es.indices.create(index=target_index, body=mapping)
        print(f"  ✅ Created consolidated index: {target_index}")
        
        # Fetch all records from source index
        print("  🔄 Fetching records from source index...")
        response = es.search(
            index=source_index,
            body={
                "query": {"match_all": {}},
                "size": 1000  # Adjust based on expected data size
            }
        )
        
        source_records = [hit['_source'] for hit in response['hits']['hits']]
        print(f"  📥 Retrieved {len(source_records)} records from {source_index}")
        
        # Convert to consolidated format
        generator = ConsolidatedBoseSalesDataGenerator()
        consolidated_records = []
        
        for record in source_records:
            consolidated_record = generator.consolidate_from_source_record(record)
            consolidated_records.append(consolidated_record)
        
        print(f"  🔄 Converted {len(consolidated_records)} records to consolidated format")
        
        # Bulk index consolidated records
        print("  📤 Bulk indexing consolidated records...")
        actions = []
        for record in consolidated_records:
            actions.append({
                "_index": target_index,
                "_id": record["id"],
                "_source": record
            })
        
        success_count, failed_items = bulk(es, actions, chunk_size=50)
        print(f"  ✅ Successfully indexed {success_count} consolidated records")
        
        # Refresh the target index to make documents searchable
        es.indices.refresh(index=target_index)
        print(f"  🔄 Refreshed consolidated index to make documents searchable")
        
        # Verify results
        count_response = es.count(index=target_index)
        total_docs = count_response['count']
        print(f"  📊 Total documents in consolidated index: {total_docs}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating consolidated index: {e}")
        return False

def run_elasticsearch_preprocessing():
    """Run all Elasticsearch preprocessing steps"""
    print("🚀 Starting Elasticsearch preprocessing...")
    print("=" * 60)
    
    # Step 1: Delete existing indices
    delete_elasticsearch_indices()
    
    # Step 2: Create non-consolidated index with synthetic data
    if not create_nonconsolidated_index():
        print("❌ Failed to create non-consolidated index")
        return False
    
    # Step 3: Create consolidated index from the non-consolidated data
    # Step 3: Create consolidated index from the non-consolidated data
    if not create_consolidated_index():
        print("❌ Failed to create consolidated index")
        return False
    
    # Step 4: Create customer-support index
    if not create_customer_support_index():
        print("❌ Failed to create customer-support index")
        return False
    print(f"✅ Pipeline will now use consolidated index: {ELASTICSEARCH_INDEX}")
    return True

def download_index_locally(index_name, output_file="index_data.json"):
    """Download all documents from an Elasticsearch index to a local JSON file"""
    print(f"📥 Downloading index '{index_name}' to local file...")
    es = get_elasticsearch_client()
    
    try:
        # Check if index exists
        if not es.indices.exists(index=index_name):
            print(f"  ❌ Index {index_name} does not exist")
            return False
        
        # Get all documents using scroll API for large datasets
        print("  🔄 Fetching documents using scroll API...")
        
        documents = []
        scroll_response = es.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "size": 1000  # Batch size
            },
            scroll='5m'  # Keep scroll context alive for 5 minutes
        )
        
        scroll_id = scroll_response['_scroll_id']
        documents.extend([hit['_source'] for hit in scroll_response['hits']['hits']])
        
        # Continue scrolling until no more documents
        while len(scroll_response['hits']['hits']) > 0:
            scroll_response = es.scroll(
                scroll_id=scroll_id,
                scroll='5m'
            )
            documents.extend([hit['_source'] for hit in scroll_response['hits']['hits']])
        
        # Clear the scroll context
        es.clear_scroll(scroll_id=scroll_id)
        
        # Save to local file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "index_name": index_name,
                "document_count": len(documents),
                "documents": documents
            }, f, indent=2, ensure_ascii=False)
        
        print(f"  ✅ Downloaded {len(documents)} documents to {output_file}")
        return True
        
    except Exception as e:
        print(f"  ❌ Error downloading index: {e}")
        return False

def upload_index_from_file(input_file, new_index_name, mapping=None):
    """Create a new index and upload documents from a local JSON file"""
    print(f"📤 Creating new index '{new_index_name}' from local file...")
    es = get_elasticsearch_client()
    
    try:
        # Load data from file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        documents = data.get('documents', [])
        original_count = data.get('document_count', len(documents))
        
        print(f"  📄 Loaded {len(documents)} documents from {input_file}")
        
        # Delete index if it exists
        if es.indices.exists(index=new_index_name):
            print(f"  🗑️ Deleting existing index: {new_index_name}")
            es.indices.delete(index=new_index_name)
        
        # Create index with mapping
        if mapping:
            print(f"  🔧 Creating index with custom mapping...")
            es.indices.create(index=new_index_name, body=mapping)
        else:
            print(f"  🔧 Creating index with default mapping...")
            # Use a basic mapping similar to the consolidated index
            default_mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "document_type": {"type": "keyword"},
                        "consolidated_text": {
                            "type": "text",
                            "analyzer": "standard"
                        }
                    }
                }
            }
            es.indices.create(index=new_index_name, body=default_mapping)
        
        # Bulk index documents
        print(f"  📤 Bulk indexing {len(documents)} documents...")
        actions = []
        for i, doc in enumerate(documents):
            actions.append({
                "_index": new_index_name,
                "_id": doc.get('id', str(uuid.uuid4())),
                "_source": doc
            })
        
        success_count, failed_items = bulk(es, actions, chunk_size=100)
        
        # Refresh index
        es.indices.refresh(index=new_index_name)
        
        # Verify upload
        count_response = es.count(index=new_index_name)
        final_count = count_response['count']
        
        print(f"  ✅ Successfully uploaded {success_count} documents")
        print(f"  📊 Final document count in {new_index_name}: {final_count}")
        
        if final_count == original_count:
            print(f"  ✅ Upload verification successful!")
        else:
            print(f"  ⚠️ Document count mismatch: expected {original_count}, got {final_count}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error uploading index: {e}")
        return False

def backup_and_restore_index(source_index, target_index, backup_file="index_backup.json"):
    """Complete backup and restore workflow"""
    print(f"🔄 Backing up '{source_index}' and restoring to '{target_index}'...")
    
    # Step 1: Download source index
    if not download_index_locally(source_index, backup_file):
        return False
    
    # Step 2: Upload to target index
    if not upload_index_from_file(backup_file, target_index):
        return False
    
    print(f"✅ Successfully backed up and restored index!")
    return True

def create_customer_support_index():
    """Create the customer-support index for storing customer service interactions"""
    print("🔧 Creating customer-support index...")
    es = get_elasticsearch_client()
    index_name = "customer-support"
    
    try:
        # Define mapping for customer support data
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "support_analyzer": {
                            "type": "standard",
                            "stopwords": "_none_"  # Keep all words for better search
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "customer_id": {"type": "keyword"},
                    "customer_name": {
                        "type": "text",
                        "analyzer": "support_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "support_agent": {
                        "type": "text",
                        "analyzer": "support_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "issue_type": {"type": "keyword"},
                    "priority": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "product_line": {"type": "keyword"},
                    "product_model": {"type": "keyword"},
                    "conversation_text": {
                        "type": "text",
                        "analyzer": "support_analyzer"
                    },
                    "resolution": {
                        "type": "text",
                        "analyzer": "support_analyzer"
                    },
                    "satisfaction_rating": {"type": "integer"},
                    "follow_up_required": {"type": "boolean"},
                    "tags": {"type": "keyword"},
                    "escalated": {"type": "boolean"},
                    "resolution_time_hours": {"type": "float"}
                }
            }
        }
        
        # Check if index exists before creating
        if es.indices.exists(index=index_name):
            print(f"  ℹ️ Index {index_name} already exists, skipping creation")
        else:
            es.indices.create(index=index_name, body=mapping)
            print(f"  ✅ Created index: {index_name}")        
        # Refresh index to make it immediately available
        es.indices.refresh(index=index_name)
        print(f"  🔄 Refreshed index to make it searchable")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating customer-support index: {e}")
        return False

    """Create the customer-support index for storing customer service interactions"""
    print("🔧 Creating customer-support index...")
    es = get_elasticsearch_client()
    index_name = "customer-support"
    
    try:
        # Define mapping for customer support data
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "support_analyzer": {
                            "type": "standard",
                            "stopwords": "_none_"  # Keep all words for better search
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "customer_id": {"type": "keyword"},
                    "customer_name": {
                        "type": "text",
                        "analyzer": "support_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "support_agent": {
                        "type": "text",
                        "analyzer": "support_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "issue_type": {"type": "keyword"},
                    "priority": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "product_line": {"type": "keyword"},
                    "product_model": {"type": "keyword"},
                    "conversation_text": {
                        "type": "text",
                        "analyzer": "support_analyzer"
                    },
                    "resolution": {
                        "type": "text",
                        "analyzer": "support_analyzer"
                    },
                    "satisfaction_rating": {"type": "integer"},
                    "follow_up_required": {"type": "boolean"},
                    "tags": {"type": "keyword"},
                    "escalated": {"type": "boolean"},
                    "resolution_time_hours": {"type": "float"}
                }
            }
        }
        
        # Create index
        es.indices.create(index=index_name, body=mapping)
        print(f"  ✅ Created index: {index_name}")
        
        # Refresh index to make it immediately available
        es.indices.refresh(index=index_name)
        print(f"  🔄 Refreshed index to make it searchable")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating customer-support index: {e}")
        return False

if __name__ == "__main__":
    # Example usage
    print("🔧 Elasticsearch Index Preprocessing Tool")
    print("=" * 50)
    
    # Run the standard preprocessing
    if run_elasticsearch_preprocessing():
        print("\n" + "=" * 50)
        print("✅ Standard preprocessing completed!")
        
        # Example: Download the consolidated index
        print("\n📥 Example: Downloading consolidated index...")
        download_index_locally("sales-records-consolidated", "consolidated_backup.json")
        
        # Example: Create a copy of the index
        print("\n📤 Example: Creating index copy...")
        upload_index_from_file("consolidated_backup.json", "sales-records-consolidated-copy")
        
        print("\n✅ All operations completed!")
    else:
        print("❌ Standard preprocessing failed!")
