#!/usr/bin/env python3
"""
Elasticsearch Setup Script for Bose Sales Data
Creates and populates an Elasticsearch index with NER-rich synthetic sales data
for use as a source connector in the Unstructured Workflow Endpoint.

Designed for Elastic Cloud deployment with .env configuration.
"""

import os
import json
import time
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from faker import Faker
from dotenv import load_dotenv

# Load environment variables from base directory (one level up)
load_dotenv(dotenv_path="../.env")

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk, BulkIndexError
except ImportError:
    print("❌ elasticsearch package not found. Install with: pip install elasticsearch")
    exit(1)

class ElasticsearchConfig:
    """Configuration for Elasticsearch connection and data setup"""
    
    def __init__(self):
        # Load from environment variables
        self.cloud_id = os.getenv('ELASTIC_CLOUD_ID')
        self.username = os.getenv('ELASTIC_USERNAME') 
        self.password = os.getenv('ELASTIC_PASSWORD')
        self.api_key = os.getenv('ELASTIC_API_KEY')
        
        # Index configuration
        self.index_name = "sales-records"
        self.num_synthetic_records = 100
        
        # Validate required credentials
        self._validate_credentials()
    
    def _validate_credentials(self):
        """Validate that required credentials are provided"""
        # Either username/password OR api_key must be provided
        if not ((self.username and self.password) or self.api_key):
            raise ValueError("Either ELASTIC_USERNAME/ELASTIC_PASSWORD or ELASTIC_API_KEY must be provided in .env file")
    
    def get_client(self) -> Elasticsearch:
        """Create and return Elasticsearch client for Elastic Cloud"""
        if self.api_key:
            # Use API key authentication (recommended for production)
            return Elasticsearch(
                "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
                api_key=self.api_key,
                request_timeout=60,
                max_retries=3,
                retry_on_timeout=True
            )
        else:
            # Use username/password authentication
            return Elasticsearch(
                "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
                basic_auth=(self.username, self.password),
                request_timeout=60,
                max_retries=3,
                retry_on_timeout=True
            )

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
                'category': 'Sports Earbuds'
            },
            'OpenAudio': {
                'models': ['OpenAudio Sport', 'OpenAudio Ultra', 'OpenAudio Pro'],
                'price_range': (149, 249),
                'category': 'Open-Ear Audio'
            },
            'QuietComfort': {
                'models': ['QuietComfort 45', 'QuietComfort Ultra', 'QuietComfort Earbuds'],
                'price_range': (199, 429),
                'category': 'Noise Cancelling'
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
    
    def generate_bulk_data(self, num_records: int) -> List[Dict[str, Any]]:
        """Generate bulk sales data for Elasticsearch"""
        print(f"🔄 Generating {num_records} synthetic sales records...")
        
        records = []
        for i in range(num_records):
            if i % 100 == 0 and i > 0:
                print(f"  Generated {i}/{num_records} records...")
            records.append(self.generate_sales_record())
        
        print(f"✅ Generated {len(records)} records successfully")
        return records

class ElasticsearchManager:
    """Manage Elasticsearch operations for Bose sales data"""
    
    def __init__(self, config: ElasticsearchConfig):
        self.config = config
        self.es = config.get_client()
        self.index_name = config.index_name
    
    def test_connection(self) -> bool:
        """Test Elasticsearch connection by checking index access"""
        try:
            print("🔧 Testing Elasticsearch connection...")
            # Test connection by checking if we can access our specific index
            # This works with index-specific API keys
            exists = self.es.indices.exists(index=self.index_name)
            print(f"✅ Successfully connected to Elasticsearch")
            print(f"   Index '{self.index_name}' exists: {exists}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def create_index_mapping(self) -> bool:
        """Create index with optimized mapping for NER and search"""
        try:
            print(f"🔧 Creating index: {self.index_name}")
            
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
            
            # Check if index exists, if so, just update mapping
            if self.es.indices.exists(index=self.index_name):
                print(f"  ✅ Index '{self.index_name}' already exists, updating mapping...")
                try:
                    # Try to update mapping for existing index
                    self.es.indices.put_mapping(index=self.index_name, body=mapping["mappings"])
                    print(f"✅ Updated mapping for existing index: {self.index_name}")
                except Exception as e:
                    print(f"  ⚠️ Could not update mapping (this is often okay): {e}")
                    print(f"  ✅ Using existing index: {self.index_name}")
                return True
            else:
                # Create new index only if it doesn't exist
                self.es.indices.create(index=self.index_name, body=mapping)
                print(f"✅ Created new index: {self.index_name}")
                return True
            
        except Exception as e:
            print(f"❌ Error creating index: {e}")
            return False
    
    def bulk_index_data(self, records: List[Dict[str, Any]]) -> bool:
        """Bulk index sales data into Elasticsearch"""
        try:
            print(f"📤 Bulk indexing {len(records)} records...")
            
            # Prepare bulk data
            actions = []
            for record in records:
                action = {
                    "_index": self.index_name,
                    "_id": record["id"],
                    "_source": record
                }
                actions.append(action)
            
            # Execute bulk index
            success_count, failed_items = bulk(
                self.es, 
                actions, 
                chunk_size=100,
                request_timeout=60,
                max_retries=3,
                initial_backoff=2,
                max_backoff=600
            )
            
            print(f"✅ Successfully indexed {success_count} records")
            
            if failed_items:
                print(f"⚠️ Failed to index {len(failed_items)} records")
                for item in failed_items[:3]:  # Show first 3 failures
                    if 'index' in item:
                        error_info = item['index']
                        print(f"   - Document ID: {error_info.get('_id', 'unknown')}")
                        print(f"     Error: {error_info.get('error', {}).get('reason', 'unknown error')}")
                    else:
                        print(f"   - {item}")
            
            return success_count > 0
            
        except BulkIndexError as e:
            print(f"❌ Bulk indexing error: {e}")
            return False
        except Exception as e:
            print(f"❌ Error during bulk indexing: {e}")
            return False
    
    def verify_data(self) -> Dict[str, Any]:
        """Verify indexed data and return statistics"""
        try:
            print("🔍 Verifying indexed data...")
            
            # Refresh index to ensure all data is searchable
            self.es.indices.refresh(index=self.index_name)
            
            # Get basic stats
            count_response = self.es.count(index=self.index_name)
            total_docs = count_response['count']
            
            print(f"📊 Total documents: {total_docs}")
            
            if total_docs == 0:
                return {"total_docs": 0}
            
            # Sample a few documents
            sample_response = self.es.search(
                index=self.index_name,
                body={"size": 3, "sort": [{"timestamp": {"order": "desc"}}]}
            )
            
            print("📋 Sample documents:")
            for i, hit in enumerate(sample_response['hits']['hits'], 1):
                source = hit['_source']
                print(f"   {i}. {source['customer_name']} - {source['product_model']} - ${source['price']}")
                print(f"      {source['interaction_text'][:100]}...")
            
            # Aggregation stats
            agg_response = self.es.search(
                index=self.index_name,
                body={
                    "size": 0,
                    "aggs": {
                        "by_product": {
                            "terms": {"field": "product_line", "size": 10}
                        },
                        "by_retailer": {
                            "terms": {"field": "retailer.keyword", "size": 5}
                        },
                        "avg_price": {
                            "avg": {"field": "price"}
                        }
                    }
                }
            )
            
            stats = {
                "total_docs": total_docs,
                "products": agg_response['aggregations']['by_product']['buckets'],
                "retailers": agg_response['aggregations']['by_retailer']['buckets'],
                "avg_price": round(agg_response['aggregations']['avg_price']['value'], 2)
            }
            
            print(f"📈 Statistics:")
            print(f"   Average price: ${stats['avg_price']}")
            print(f"   Top products: {', '.join([b['key'] for b in stats['products'][:3]])}")
            print(f"   Top retailers: {', '.join([b['key'] for b in stats['retailers'][:3]])}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
            return {"error": str(e)}

def main():
    """Main execution function"""
    print("🚀 Starting Elasticsearch Setup for Bose Sales Data")
    print("=" * 60)
    
    try:
        # Initialize configuration
        print("⚙️ Loading configuration...")
        config = ElasticsearchConfig()
        print(f"   Index name: {config.index_name}")
        print(f"   Records to generate: {config.num_synthetic_records}")
        
        # Initialize Elasticsearch manager
        es_manager = ElasticsearchManager(config)
        
        # Test connection
        if not es_manager.test_connection():
            print("❌ Cannot proceed without valid Elasticsearch connection")
            return False
        
        # Create index with mapping
        if not es_manager.create_index_mapping():
            print("❌ Failed to create index mapping")
            return False
        
        # Generate synthetic data
        data_generator = BoseSalesDataGenerator()
        sales_records = data_generator.generate_bulk_data(config.num_synthetic_records)
        
        # Index data
        if not es_manager.bulk_index_data(sales_records):
            print("❌ Failed to index data")
            return False
        
        # Verify results
        stats = es_manager.verify_data()
        
        print("\n" + "=" * 60)
        print("🎉 SETUP COMPLETE!")
        print("=" * 60)
        print(f"✅ Elasticsearch index '{config.index_name}' created successfully")
        print(f"✅ {stats.get('total_docs', 0)} sales records indexed")
        print(f"📊 Ready for use as Unstructured Workflow source connector")
        print("\nNext steps:")
        print("1. Use this index as an Elasticsearch source connector")
        print("2. Configure NER enrichment workflow node")
        print("3. Process through your hybrid RAG pipeline")
        
        return True
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 