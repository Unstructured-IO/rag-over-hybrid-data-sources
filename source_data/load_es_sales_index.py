#!/usr/bin/env python3
"""
Elasticsearch Sales Index Data Loader

This script provides functionality to:
1. Download the sales-records-consolidated index from Elasticsearch and save as a zip file
2. Load data from a zip file into the sales-records-consolidated index

Usage:
    # Download index to zip file
    python load_es_sales_index.py download --output source_data/sales_data.zip
    
    # Load data from zip file to index
    python load_es_sales_index.py load --input source_data/sales_data.zip
"""

import os
import sys
import json
import zipfile
import argparse
from pathlib import Path
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk

# Load environment variables
load_dotenv()

def get_elasticsearch_client():
    """Initialize and return Elasticsearch client."""
    host = os.getenv("ELASTICSEARCH_HOST")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")
    
    if not host or not api_key:
        raise ValueError("ELASTICSEARCH_HOST and ELASTICSEARCH_API_KEY must be set in .env file")
    
    return Elasticsearch(
        host,
        api_key=api_key,
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True
    )

def download_index(output_path: str, index_name: str = "sales-records-consolidated"):
    """Download Elasticsearch index data and save as zip file."""
    print(f"🔄 Downloading index '{index_name}'...")
    
    try:
        es = get_elasticsearch_client()
        
        # Check if index exists
        if not es.indices.exists(index=index_name):
            raise ValueError(f"❌ Index '{index_name}' does not exist")
        
        # Get index mapping
        mapping_response = es.indices.get_mapping(index=index_name)
        mapping = mapping_response.body if hasattr(mapping_response, 'body') else mapping_response
        
        # Get all documents
        documents = []
        for doc in scan(es, index=index_name, query={"query": {"match_all": {}}}):
            documents.append({
                "_id": doc["_id"],
                "_source": doc["_source"]
            })
        
        print(f"📊 Found {len(documents)} documents")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to zip file
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Save mapping
            zipf.writestr('mapping.json', json.dumps(mapping, indent=2))
            
            # Save documents
            zipf.writestr('documents.json', json.dumps(documents, indent=2))
        
        print(f"✅ Successfully saved {len(documents)} documents to {output_path}")
        
    except Exception as e:
        print(f"❌ Error downloading index: {e}")
        sys.exit(1)

def load_index(input_path: str, index_name: str = "sales-records-consolidated"):
    """Load data from zip file into Elasticsearch index."""
    print(f"🔄 Loading data into index '{index_name}'...")
    
    try:
        input_path = Path(input_path)
        if not input_path.exists():
            raise ValueError(f"❌ Input file '{input_path}' does not exist")
        
        es = get_elasticsearch_client()
        
        # Delete existing index if it exists
        if es.indices.exists(index=index_name):
            print(f"🗑️ Deleting existing index '{index_name}'...")
            es.indices.delete(index=index_name)
        
        # Load data from zip file
        with zipfile.ZipFile(input_path, 'r') as zipf:
            # Load mapping
            with zipf.open('mapping.json') as f:
                mapping_data = json.loads(f.read().decode('utf-8'))
            
            # Load documents
            with zipf.open('documents.json') as f:
                documents = json.loads(f.read().decode('utf-8'))
        
        print(f"📊 Loaded {len(documents)} documents from zip file")
        
        # Create index with mapping
        index_mapping = mapping_data[index_name] if index_name in mapping_data else mapping_data[list(mapping_data.keys())[0]]
        es.indices.create(index=index_name, body=index_mapping)
        print(f"🔧 Created index '{index_name}' with mapping")
        
        # Prepare documents for bulk insert
        def generate_docs():
            for doc in documents:
                yield {
                    "_index": index_name,
                    "_id": doc["_id"],
                    "_source": doc["_source"]
                }
        
        # Bulk insert documents
        success_count, failed_items = bulk(es, generate_docs(), chunk_size=100)
        print(f"📝 Inserted {success_count} documents")
        
        if failed_items:
            print(f"⚠️ Failed to insert {len(failed_items)} documents")
        
        # Refresh index
        es.indices.refresh(index=index_name)
        
        # Verify data was loaded
        count_response = es.count(index=index_name)
        count_data = count_response.body if hasattr(count_response, 'body') else count_response
        doc_count = count_data['count']
        
        if doc_count > 0:
            print(f"✅ Successfully loaded {doc_count} documents into '{index_name}' index")
        else:
            print(f"❌ Index '{index_name}' is empty after loading")
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error loading index: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Elasticsearch Sales Index Data Loader")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download index to zip file')
    download_parser.add_argument('--output', '-o', required=True, 
                                help='Output zip file path (e.g., source_data/sales_data.zip)')
    download_parser.add_argument('--index', '-i', default='sales-records-consolidated',
                                help='Index name to download (default: sales-records-consolidated)')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load data from zip file to index')
    load_parser.add_argument('--input', '-i', required=True,
                            help='Input zip file path (e.g., source_data/sales_data.zip)')
    load_parser.add_argument('--index', '-x', default='sales-records-consolidated',
                            help='Index name to load into (default: sales-records-consolidated)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'download':
        download_index(args.output, args.index)
    elif args.command == 'load':
        load_index(args.input, args.index)

if __name__ == "__main__":
    main() 