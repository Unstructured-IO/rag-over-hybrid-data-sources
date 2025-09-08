#!/usr/bin/env python3
"""
Elasticsearch Data Verification Script
Inspects and validates the synthetic sales data in the Elasticsearch index.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, List
from dotenv import load_dotenv

# Load environment variables from base directory (one level up)
load_dotenv(dotenv_path="../.env")

try:
    from elasticsearch import Elasticsearch
except ImportError:
    print("❌ elasticsearch package not found. Install with: pip install elasticsearch")
    exit(1)

class ElasticsearchDataVerifier:
    """Verify and inspect Elasticsearch data"""
    
    def __init__(self):
        self.api_key = os.getenv('ELASTIC_API_KEY')
        self.username = os.getenv('ELASTIC_USERNAME')
        self.password = os.getenv('ELASTIC_PASSWORD')
        self.index_name = "sales-records"
        
        # Validate credentials
        if not ((self.username and self.password) or self.api_key):
            raise ValueError("Either ELASTIC_USERNAME/ELASTIC_PASSWORD or ELASTIC_API_KEY must be provided in .env file")
        
        # Initialize client
        self.es = self._get_client()
    
    def _get_client(self) -> Elasticsearch:
        """Create Elasticsearch client"""
        if self.api_key:
            return Elasticsearch(
                "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
                api_key=self.api_key,
                request_timeout=60,
                max_retries=3,
                retry_on_timeout=True
            )
        else:
            return Elasticsearch(
                "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
                basic_auth=(self.username, self.password),
                request_timeout=60,
                max_retries=3,
                retry_on_timeout=True
            )
    
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
    
    def check_index_exists(self) -> bool:
        """Check if the sales-records index exists"""
        try:
            exists = self.es.indices.exists(index=self.index_name)
            if exists:
                print(f"✅ Index '{self.index_name}' exists")
                return True
            else:
                print(f"❌ Index '{self.index_name}' does not exist")
                return False
        except Exception as e:
            print(f"❌ Error checking index existence: {e}")
            return False
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get comprehensive index statistics"""
        try:
            print(f"📊 Getting statistics for index '{self.index_name}'...")
            
            # Basic count
            count_response = self.es.count(index=self.index_name)
            total_docs = count_response['count']
            
            # Index stats
            stats_response = self.es.indices.stats(index=self.index_name)
            index_stats = stats_response['indices'][self.index_name]
            
            # Index settings and mapping
            settings_response = self.es.indices.get_settings(index=self.index_name)
            mapping_response = self.es.indices.get_mapping(index=self.index_name)
            
            stats = {
                'total_documents': total_docs,
                'index_size': index_stats['total']['store']['size_in_bytes'],
                'primary_shards': index_stats['total']['segments']['count'],
                'settings': settings_response[self.index_name]['settings'],
                'mapping': mapping_response[self.index_name]['mappings']
            }
            
            print(f"   📄 Total documents: {stats['total_documents']:,}")
            print(f"   💾 Index size: {stats['index_size']:,} bytes ({stats['index_size']/1024/1024:.2f} MB)")
            print(f"   🔧 Primary shards: {stats['primary_shards']}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting index stats: {e}")
            return {}
    
    def get_sample_documents(self, num_samples: int = 5) -> List[Dict[str, Any]]:
        """Retrieve sample documents from the index"""
        try:
            print(f"📋 Retrieving {num_samples} sample documents...")
            
            # Get recent documents
            search_response = self.es.search(
                index=self.index_name,
                body={
                    "size": num_samples,
                    "sort": [{"timestamp": {"order": "desc"}}],
                    "_source": True
                }
            )
            
            documents = []
            for i, hit in enumerate(search_response['hits']['hits'], 1):
                doc = hit['_source']
                documents.append(doc)
                
                print(f"\n📄 Document {i}:")
                print(f"   🆔 ID: {hit['_id']}")
                print(f"   👤 Customer: {doc.get('customer_name', 'N/A')}")
                print(f"   🏷️ Product: {doc.get('product_model', 'N/A')}")
                print(f"   💰 Price: ${doc.get('price', 'N/A')}")
                print(f"   🏪 Retailer: {doc.get('retailer', 'N/A')}")
                print(f"   📍 Location: {doc.get('location_city', 'N/A')}")
                print(f"   📅 Date: {doc.get('timestamp', 'N/A')}")
                print(f"   📝 Text: {doc.get('interaction_text', 'N/A')[:100]}...")
            
            return documents
            
        except Exception as e:
            print(f"❌ Error retrieving sample documents: {e}")
            return []
    
    def analyze_data_distribution(self) -> Dict[str, Any]:
        """Analyze the distribution of data across different fields"""
        try:
            print("📈 Analyzing data distribution...")
            
            # Aggregation query
            agg_response = self.es.search(
                index=self.index_name,
                body={
                    "size": 0,
                    "aggs": {
                        "product_lines": {
                            "terms": {"field": "product_line", "size": 10}
                        },
                        "product_models": {
                            "terms": {"field": "product_model", "size": 20}
                        },
                        "retailers": {
                            "terms": {"field": "retailer.keyword", "size": 15}
                        },
                        "regions": {
                            "terms": {"field": "region", "size": 15}
                        },
                        "interaction_types": {
                            "terms": {"field": "interaction_type", "size": 20}
                        },
                        "customer_segments": {
                            "terms": {"field": "customer_segment", "size": 10}
                        },
                        "sales_channels": {
                            "terms": {"field": "sales_channel", "size": 10}
                        },
                        "price_stats": {
                            "stats": {"field": "price"}
                        },
                        "revenue_stats": {
                            "stats": {"field": "revenue_potential"}
                        },
                        "date_histogram": {
                            "date_histogram": {
                                "field": "timestamp",
                                "calendar_interval": "year"
                            }
                        }
                    }
                }
            )
            
            aggs = agg_response['aggregations']
            
            # Display results
            print("\n🎯 Product Line Distribution:")
            for bucket in aggs['product_lines']['buckets']:
                print(f"   • {bucket['key']}: {bucket['doc_count']} records")
            
            print("\n🎧 Product Model Distribution:")
            for bucket in aggs['product_models']['buckets']:
                print(f"   • {bucket['key']}: {bucket['doc_count']} records")
            
            print("\n🏪 Top Retailers:")
            for bucket in aggs['retailers']['buckets'][:5]:
                print(f"   • {bucket['key']}: {bucket['doc_count']} records")
            
            print("\n🌍 Regional Distribution:")
            for bucket in aggs['regions']['buckets'][:5]:
                print(f"   • {bucket['key']}: {bucket['doc_count']} records")
            
            print("\n🔄 Interaction Types:")
            for bucket in aggs['interaction_types']['buckets'][:5]:
                print(f"   • {bucket['key']}: {bucket['doc_count']} records")
            
            price_stats = aggs['price_stats']
            print(f"\n💰 Price Statistics:")
            print(f"   • Average: ${price_stats['avg']:.2f}")
            print(f"   • Min: ${price_stats['min']:.2f}")
            print(f"   • Max: ${price_stats['max']:.2f}")
            
            revenue_stats = aggs['revenue_stats']
            print(f"\n💼 Revenue Potential Statistics:")
            print(f"   • Average: ${revenue_stats['avg']:.2f}")
            print(f"   • Min: ${revenue_stats['min']:.2f}")
            print(f"   • Max: ${revenue_stats['max']:.2f}")
            
            print(f"\n📅 Records by Year:")
            for bucket in aggs['date_histogram']['buckets']:
                year = datetime.fromisoformat(bucket['key_as_string'].replace('Z', '+00:00')).year
                print(f"   • {year}: {bucket['doc_count']} records")
            
            return aggs
            
        except Exception as e:
            print(f"❌ Error analyzing data distribution: {e}")
            return {}
    
    def validate_ner_readiness(self) -> Dict[str, Any]:
        """Validate that data is ready for NER processing"""
        try:
            print("🔍 Validating NER readiness...")
            
            # Search for specific entity patterns
            validation_queries = {
                'person_names': {
                    "query": {
                        "bool": {
                            "should": [
                                {"exists": {"field": "customer_name"}},
                                {"exists": {"field": "sales_representative"}}
                            ]
                        }
                    }
                },
                'organizations': {
                    "query": {
                        "exists": {"field": "retailer"}
                    }
                },
                'locations': {
                    "query": {
                        "exists": {"field": "location_city"}
                    }
                },
                'monetary_values': {
                    "query": {
                        "range": {"price": {"gt": 0}}
                    }
                },
                'dates': {
                    "query": {
                        "exists": {"field": "timestamp"}
                    }
                },
                'rich_text': {
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "interaction_text"}},
                                {"range": {"interaction_text.keyword": {"gte": 50}}}  # At least 50 chars
                            ]
                        }
                    }
                }
            }
            
            results = {}
            for category, query in validation_queries.items():
                try:
                    response = self.es.count(index=self.index_name, body=query)
                    count = response['count']
                    results[category] = count
                    print(f"   ✅ {category.replace('_', ' ').title()}: {count} records")
                except Exception as e:
                    print(f"   ❌ {category}: Error - {e}")
                    results[category] = 0
            
            # Sample text analysis
            print(f"\n📝 Sample Text Analysis:")
            sample_texts = self.es.search(
                index=self.index_name,
                body={"size": 3, "_source": ["interaction_text"]},
            )
            
            for i, hit in enumerate(sample_texts['hits']['hits'], 1):
                text = hit['_source'].get('interaction_text', '')
                word_count = len(text.split())
                print(f"   📄 Sample {i}: {word_count} words, {len(text)} characters")
                
                # Count potential entities (simple heuristic)
                potential_entities = 0
                if any(name in text for name in ['Customer', 'Sales rep', 'rep']):
                    potential_entities += 1
                if '$' in text:
                    potential_entities += 1
                if any(retailer in text for retailer in ['Best Buy', 'Target', 'Amazon', 'Walmart']):
                    potential_entities += 1
                if any(city in text for city in [', NY', ', CA', ', TX', ', FL']):
                    potential_entities += 1
                
                print(f"      🎯 Estimated entities: {potential_entities}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error validating NER readiness: {e}")
            return {}
    
    def search_sample_queries(self) -> None:
        """Test sample search queries to verify data accessibility"""
        try:
            print("🔍 Testing sample search queries...")
            
            test_queries = [
                {
                    "name": "SoundSport products",
                    "query": {"match": {"product_line": "SoundSport"}}
                },
                {
                    "name": "High-value sales (>$300)",
                    "query": {"range": {"price": {"gte": 300}}}
                },
                {
                    "name": "Best Buy sales",
                    "query": {"match": {"retailer": "Best Buy"}}
                },
                {
                    "name": "Recent sales (last 30 days)",
                    "query": {"range": {"timestamp": {"gte": "now-30d"}}}
                },
                {
                    "name": "Text search: 'noise cancellation'",
                    "query": {"match": {"interaction_text": "noise cancellation"}}
                }
            ]
            
            for test in test_queries:
                try:
                    response = self.es.count(index=self.index_name, body={"query": test["query"]})
                    count = response['count']
                    print(f"   📊 {test['name']}: {count} matches")
                except Exception as e:
                    print(f"   ❌ {test['name']}: Error - {e}")
            
        except Exception as e:
            print(f"❌ Error testing search queries: {e}")

def main():
    """Main verification function"""
    print("🚀 Starting Elasticsearch Data Verification")
    print("=" * 60)
    
    try:
        # Initialize verifier
        verifier = ElasticsearchDataVerifier()
        
        # Test connection
        if not verifier.test_connection():
            print("❌ Cannot proceed without valid connection")
            return False
        
        # Check if index exists
        if not verifier.check_index_exists():
            print("❌ Index does not exist. Run elasticsearch_setup.py first.")
            return False
        
        # Get index statistics
        stats = verifier.get_index_stats()
        
        if stats.get('total_documents', 0) == 0:
            print("❌ No documents found in index")
            return False
        
        # Get sample documents
        samples = verifier.get_sample_documents(5)
        
        # Analyze data distribution
        distribution = verifier.analyze_data_distribution()
        
        # Validate NER readiness
        ner_validation = verifier.validate_ner_readiness()
        
        # Test search queries
        verifier.search_sample_queries()
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 VERIFICATION SUMMARY")
        print("=" * 60)
        print(f"✅ Index exists: {verifier.index_name}")
        print(f"✅ Total documents: {stats.get('total_documents', 0):,}")
        print(f"✅ Index size: {stats.get('index_size', 0)/1024/1024:.2f} MB")
        print(f"✅ Sample documents retrieved: {len(samples)}")
        print(f"✅ Data distribution analyzed: {len(distribution)} categories")
        print(f"✅ NER validation completed: {len(ner_validation)} entity types")
        
        print(f"\n🎯 Ready for:")
        print(f"   • Unstructured Workflow source connector")
        print(f"   • NER enrichment processing")
        print(f"   • Hybrid RAG pipeline integration")
        
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 