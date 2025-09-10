#!/usr/bin/env python3
"""
Customer Support Index Verification Script

This script analyzes the metadata field of all documents in the customer-support index
to verify that both S3 and Elasticsearch sources are represented in the processed data.

It will:
1. Connect to the Elasticsearch customer-support index
2. Retrieve all documents and their metadata
3. Analyze the data_source-url field to identify source types
4. Provide a summary of source distribution
5. Verify both S3 and Elasticsearch sources are present
"""

import os
import sys
from collections import defaultdict, Counter
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import json

# Load environment variables
load_dotenv()

# Configuration
print("🔧 Loading configuration...")

# Elasticsearch Configuration
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", "example-data-bose-headphones")

# Validation
REQUIRED_VARS = {
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    sys.exit(1)

print("✅ All required configuration values loaded successfully")

# Initialize Elasticsearch client
print("🔧 Initializing Elasticsearch client...")
try:
    es = Elasticsearch(
        hosts=[ELASTICSEARCH_HOST],
        api_key=ELASTICSEARCH_API_KEY,
        verify_certs=True
    )
    
    # Test connection with a simple search instead of cluster info
    test_response = es.search(
        index="customer-support",
        body={"query": {"match_all": {}}, "size": 1}
    )
    print(f"✅ Connected to Elasticsearch successfully")
    
except Exception as e:
    print(f"❌ Failed to connect to Elasticsearch: {e}")
    sys.exit(1)

def analyze_customer_support_index():
    """
    Analyze all documents in the customer-support index and categorize by source.
    
    Returns:
        dict: Analysis results with source counts and metadata
    """
    print("\n🔍 Analyzing customer-support index...")
    print("-" * 60)
    
    try:
        # Check if index exists and get total count
        try:
            count_response = es.count(index="customer-support")
            total_docs = count_response['count']
            print(f"📊 Total documents in customer-support index: {total_docs}")
        except Exception as e:
            print(f"❌ Could not get document count: {e}")
            return None
        
        if total_docs == 0:
            print("⚠️ No documents found in customer-support index")
            return None
        
        # Scroll through all documents
        print("🔄 Retrieving all documents...")
        
        # Initialize scroll
        scroll_response = es.search(
            index="customer-support",
            scroll='2m',
            size=100,
            body={
                "query": {"match_all": {}},
                "_source": ["metadata", "record_id", "text", "type"]
            }
        )
        
        scroll_id = scroll_response['_scroll_id']
        hits = scroll_response['hits']['hits']
        
        # Analysis containers
        source_analysis = defaultdict(list)
        source_counts = Counter()
        metadata_samples = defaultdict(list)
        file_types = Counter()
        languages = Counter()
        
        processed_docs = 0
        
        while hits:
            for hit in hits:
                processed_docs += 1
                doc_id = hit['_id']
                source = hit['_source']
                
                # Extract metadata
                metadata = source.get('metadata', {})
                record_id = source.get('record_id', 'unknown')
                doc_type = source.get('type', 'unknown')
                
                # Analyze data source URL
                data_source_url = metadata.get('data_source-url', 'unknown')
                
                # Categorize by source type
                if 's3' in data_source_url.lower() or S3_SOURCE_BUCKET in data_source_url:
                    source_type = 'S3'
                    source_key = 'S3_PDFs'
                elif 'elasticsearch' in data_source_url.lower() or 'sales-records' in data_source_url:
                    source_type = 'Elasticsearch'
                    source_key = 'Elasticsearch_Sales'
                else:
                    source_type = 'Unknown'
                    source_key = 'Unknown'
                
                # Store analysis data
                source_analysis[source_key].append({
                    'doc_id': doc_id,
                    'record_id': record_id,
                    'data_source_url': data_source_url,
                    'filename': metadata.get('filename', 'unknown'),
                    'filetype': metadata.get('filetype', 'unknown'),
                    'type': doc_type
                })
                
                source_counts[source_key] += 1
                
                # Store metadata samples (limit to 3 per source type)
                if len(metadata_samples[source_key]) < 3:
                    metadata_samples[source_key].append(metadata)
                
                # Count file types and languages
                file_types[metadata.get('filetype', 'unknown')] += 1
                languages_list = metadata.get('languages', [])
                for lang in languages_list:
                    languages[lang] += 1
                
                if processed_docs % 50 == 0:
                    print(f"  📄 Processed {processed_docs}/{total_docs} documents...")
            
            # Get next batch
            try:
                scroll_response = es.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = scroll_response['_scroll_id']
                hits = scroll_response['hits']['hits']
            except Exception as e:
                print(f"  ⚠️ Scroll error (likely reached end): {e}")
                break
        
        # Clear scroll
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except:
            pass  # Ignore clear scroll errors
        
        print(f"✅ Successfully analyzed {processed_docs} documents")
        
        return {
            'total_docs': total_docs,
            'processed_docs': processed_docs,
            'source_counts': dict(source_counts),
            'source_analysis': dict(source_analysis),
            'metadata_samples': dict(metadata_samples),
            'file_types': dict(file_types),
            'languages': dict(languages)
        }
        
    except Exception as e:
        print(f"❌ Error analyzing customer-support index: {e}")
        return None

def print_analysis_results(results):
    """Print detailed analysis results"""
    if not results:
        return
    
    print("\n" + "=" * 80)
    print("📊 CUSTOMER SUPPORT INDEX ANALYSIS RESULTS")
    print("=" * 80)
    
    # Overall statistics
    print(f"\n📈 OVERALL STATISTICS:")
    print(f"  Total Documents: {results['total_docs']}")
    print(f"  Processed Documents: {results['processed_docs']}")
    
    # Source distribution
    print(f"\n🔍 SOURCE DISTRIBUTION:")
    source_counts = results['source_counts']
    for source_type, count in source_counts.items():
        percentage = (count / results['total_docs']) * 100
        print(f"  {source_type}: {count} documents ({percentage:.1f}%)")
    
    # Verification results
    print(f"\n✅ SOURCE VERIFICATION:")
    has_s3 = 'S3_PDFs' in source_counts
    has_elasticsearch = 'Elasticsearch_Sales' in source_counts
    
    print(f"  S3 PDFs Source: {'✅ PRESENT' if has_s3 else '❌ MISSING'}")
    print(f"  Elasticsearch Sales Source: {'✅ PRESENT' if has_elasticsearch else '❌ MISSING'}")
    
    if has_s3 and has_elasticsearch:
        print(f"  🎉 SUCCESS: Both S3 and Elasticsearch sources are represented!")
    else:
        print(f"  ⚠️ WARNING: Not all expected sources are present")
    
    # File type distribution
    print(f"\n📄 FILE TYPE DISTRIBUTION:")
    for file_type, count in results['file_types'].items():
        percentage = (count / results['total_docs']) * 100
        print(f"  {file_type}: {count} documents ({percentage:.1f}%)")
    
    # Language distribution
    print(f"\n🌐 LANGUAGE DISTRIBUTION:")
    for language, count in results['languages'].items():
        percentage = (count / results['total_docs']) * 100
        print(f"  {language}: {count} documents ({percentage:.1f}%)")
    
    # Sample metadata for each source type
    print(f"\n🔍 SAMPLE METADATA BY SOURCE TYPE:")
    for source_type, samples in results['metadata_samples'].items():
        print(f"\n  📁 {source_type} ({len(samples)} samples):")
        for i, metadata in enumerate(samples, 1):
            print(f"    Sample {i}:")
            print(f"      Filename: {metadata.get('filename', 'N/A')}")
            print(f"      Filetype: {metadata.get('filetype', 'N/A')}")
            print(f"      Data Source URL: {metadata.get('data_source-url', 'N/A')}")
            print(f"      Languages: {metadata.get('languages', 'N/A')}")
            if 'data_source-record_locator-index_name' in metadata:
                print(f"      Index Name: {metadata['data_source-record_locator-index_name']}")
            if 'data_source-record_locator-document_id' in metadata:
                print(f"      Document ID: {metadata['data_source-record_locator-document_id']}")

def save_detailed_report(results, filename="customer_support_analysis_report.json"):
    """Save detailed analysis results to a JSON file"""
    if not results:
        return
    
    try:
        from datetime import datetime
        
        # Prepare data for JSON serialization
        report_data = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_documents': results['total_docs'],
            'processed_documents': results['processed_docs'],
            'source_distribution': results['source_counts'],
            'file_type_distribution': results['file_types'],
            'language_distribution': results['languages'],
            'source_verification': {
                's3_pdfs_present': 'S3_PDFs' in results['source_counts'],
                'elasticsearch_sales_present': 'Elasticsearch_Sales' in results['source_counts'],
                'both_sources_present': ('S3_PDFs' in results['source_counts'] and 
                                       'Elasticsearch_Sales' in results['source_counts'])
            },
            'detailed_analysis': results['source_analysis'],
            'metadata_samples': results['metadata_samples']
        }
        
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        print(f"\n💾 Detailed report saved to: {filename}")
        
    except Exception as e:
        print(f"⚠️ Could not save detailed report: {e}")

def main():
    """Main execution function"""
    print("🚀 Starting Customer Support Index Verification")
    print("=" * 60)
    
    # Analyze the index
    results = analyze_customer_support_index()
    
    if results:
        # Print results
        print_analysis_results(results)
        
        # Save detailed report
        save_detailed_report(results)
        
        # Final verification
        print(f"\n🎯 FINAL VERIFICATION:")
        has_s3 = 'S3_PDFs' in results['source_counts']
        has_elasticsearch = 'Elasticsearch_Sales' in results['source_counts']
        
        if has_s3 and has_elasticsearch:
            print("✅ VERIFICATION PASSED: Both S3 and Elasticsearch sources are present in customer-support index")
            return True
        else:
            print("❌ VERIFICATION FAILED: Not all expected sources are present")
            return False
    else:
        print("❌ VERIFICATION FAILED: Could not analyze customer-support index")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
