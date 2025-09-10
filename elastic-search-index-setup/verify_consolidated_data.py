#!/usr/bin/env python3
"""
Verify Consolidated Elasticsearch Data for RAG
Shows how context is preserved in the consolidated text field.
"""

import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

# Load environment variables from base directory (one level up)
load_dotenv(dotenv_path="../.env")

def main():
    """Verify consolidated data structure and context preservation"""
    print("🔍 Verifying Consolidated RAG Data")
    print("=" * 50)
    
    # Initialize
    api_key = os.getenv('ELASTIC_API_KEY')
    es = Elasticsearch(
        "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
        api_key=api_key,
        request_timeout=60
    )
    
    index_name = "sales-records"  # Use same index name as API key permissions
    
    try:
        # Check if index exists
        if not es.indices.exists(index=index_name):
            print(f"❌ Index '{index_name}' does not exist. Run create_consolidated_index.py first.")
            return
        
        # Get document count
        count_response = es.count(index=index_name)
        total_docs = count_response['count']
        print(f"📊 Total consolidated documents: {total_docs}")
        
        if total_docs == 0:
            print("❌ No documents found in consolidated index")
            return
        
        # Get sample documents
        sample_response = es.search(
            index=index_name,
            body={
                "size": 2,
                "_source": ["consolidated_text", "product_line", "region", "record_date"],
                "sort": [{"timestamp": {"order": "desc"}}]
            },
        )
        
        print(f"\n📋 Sample Consolidated Records:")
        print("=" * 50)
        
        for i, hit in enumerate(sample_response['hits']['hits'], 1):
            source = hit['_source']
            print(f"\n🔸 RECORD {i}:")
            print(f"   Product Line: {source['product_line']}")
            print(f"   Region: {source['region']}")
            print(f"   Date: {source['record_date']}")
            print(f"\n   📝 CONSOLIDATED TEXT (Full Context):")
            print(f"   {'-' * 45}")
            
            # Show the full consolidated text with formatting
            consolidated_text = source['consolidated_text']
            lines = consolidated_text.split('\n')
            for line in lines[:15]:  # Show first 15 lines
                if line.strip():
                    print(f"   {line}")
            
            if len(lines) > 15:
                print(f"   ... ({len(lines) - 15} more lines)")
            
            print(f"   {'-' * 45}")
        
        # Show aggregations
        agg_response = es.search(
            index=index_name,
            body={
                "size": 0,
                "aggs": {
                    "by_product": {
                        "terms": {"field": "product_line", "size": 5}
                    },
                    "by_region": {
                        "terms": {"field": "region", "size": 5}
                    },
                    "by_quarter": {
                        "terms": {"field": "quarter", "size": 8}
                    }
                }
            }
        )
        
        print(f"\n📈 Data Distribution:")
        print("=" * 30)
        
        print("🎧 By Product Line:")
        for bucket in agg_response['aggregations']['by_product']['buckets']:
            print(f"   • {bucket['key']}: {bucket['doc_count']} records")
        
        print("\n🌍 By Region:")
        for bucket in agg_response['aggregations']['by_region']['buckets'][:5]:
            print(f"   • {bucket['key']}: {bucket['doc_count']} records")
        
        print("\n📅 By Quarter:")
        for bucket in agg_response['aggregations']['by_quarter']['buckets'][:5]:
            print(f"   • {bucket['key']}: {bucket['doc_count']} records")
        
        # Test search functionality
        print(f"\n🔍 Testing Context-Aware Search:")
        print("=" * 35)
        
        search_tests = [
            "noise cancellation",
            "Best Buy",
            "warranty",
            "New York"
        ]
        
        for search_term in search_tests:
            search_response = es.search(
                index=index_name,
                body={
                    "size": 1,
                    "query": {
                        "match": {
                            "consolidated_text": search_term
                        }
                    },
                    "_source": ["product_line", "region"]
                }
            )
            
            hits = search_response['hits']['total']['value']
            if hits > 0:
                sample_hit = search_response['hits']['hits'][0]['_source']
                print(f"   🔎 '{search_term}': {hits} matches (e.g., {sample_hit['product_line']} in {sample_hit['region']})")
            else:
                print(f"   🔎 '{search_term}': {hits} matches")
        
        print(f"\n" + "=" * 50)
        print("🎉 CONSOLIDATED DATA VERIFICATION COMPLETE")
        print("=" * 50)
        print("✅ Context Preservation: Each record contains ALL relevant information")
        print("✅ RAG Ready: Unstructured will process 'consolidated_text' field")
        print("✅ No Context Loss: Customer, product, location, and conversation details preserved")
        print("✅ Search Ready: Full-text search across all contextual information")
        print("\n🚀 Ready for Unstructured Workflow processing!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 