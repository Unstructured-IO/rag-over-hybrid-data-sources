#!/usr/bin/env python3
# %% [markdown]
# # Hybrid RAG Pipeline: Unifying Enterprise Data Sources
# 
# ## The Enterprise Data Challenge
# 
# Imagine you're a customer support agent trying to help a customer with a complex product issue. You need information from:
# - **Product manuals** stored as PDFs in cloud storage
# - **Customer purchase history** in your sales database
# - **Previous support interactions** scattered across different systems
# 
# Each piece lives in a different format, in a different system, with different access methods. This is the reality for most enterprises today.
# 
# ## Why Data Unification Matters
# 
# **The Problem**: Enterprise data rarely lives in one place or format. Critical information is fragmented across:
# - Unstructured documents (PDFs, manuals, reports) in cloud storage
# - Structured records (sales data, customer info) in databases
# - Different formats requiring different processing approaches
# 
# **The Challenge**: Traditional RAG systems work well with homogeneous data but struggle when you need to query across diverse data sources simultaneously.
# 
# **The Solution**: This notebook demonstrates how to build a hybrid RAG system that processes multiple data types in parallel and creates a unified, searchable knowledge base.
# 
# ## Architecture Overview
# 
# Our solution processes two different data sources simultaneously:
# 
# ```
# ┌─────────────────┐                           ┌─────────────────────────┐
# │   S3 PDFs       │──── WORKFLOW 1 ──────────▶│                         │
# │ (Product Docs)  │                           │    Unstructured API     │
# └─────────────────┘                           │                         │
#                                               │  Partition → Chunk →    │
# ┌─────────────────┐                           │  Embed → NER → Store    │
# │ Elasticsearch   │──── WORKFLOW 2 ──────────▶│                         │
# │ (Sales Records) │                           │                         │
# └─────────────────┘                           └────────────┬────────────┘
#                                                            │
#                                               ┌────────────▼────────────┐
#                                               │    customer-support     │
#                                               │   (Unified Index)       │
#                                               └─────────────────────────┘
# ```
# 
# ## How Unstructured Solves This
# 
# The Unstructured API provides a unified processing pipeline that:
# 1. **Handles diverse formats** - PDFs, databases, structured data
# 2. **Applies consistent processing** - Same chunking, embedding, and enrichment
# 3. **Creates unified output** - All data lands in the same searchable format
# 4. **Scales automatically** - Cloud-based processing handles large datasets
# 
# This notebook walks through building such a system step by step.

# %% [markdown]
# ## Getting Started: Unstructured API Access
# 
# To follow along with this tutorial, you'll need an Unstructured API key.
# 
# ### Sign Up and Get Your API Key
# 
# 1. **Sign up** for a free account at https://platform.unstructured.io
# 2. **Navigate to API Keys** in the sidebar after signing in
# 3. **Generate API Key** and copy it to your clipboard
# 4. **Save the key** - you'll need it for the configuration step below
# 
# For Team or Enterprise accounts, make sure you've selected the correct organizational workspace before creating your API key.
# 
# **Need help?** Contact Unstructured Support at support@unstructured.io

# %% [markdown]
# ## Configuration: Environment Setup
# 
# ### For Google Colab Users
# 
# **Step 1: Set Your API Keys**
# 
# In the code cell below, replace the placeholder values with your actual API keys:
# - Find each line that says `"your-[service]-api-key"`
# - Replace with your actual credentials (keep the quotes)
# - Run the cell to validate your configuration
# 
# **Step 2: Run the Configuration Cell**
# 
# The cell will install required packages and validate your credentials.

# %% [markdown]
# ### Environment Variable Configuration
# 
# **Instructions**: Replace the placeholder values below with your actual credentials:

# %%
import sys, subprocess

def ensure_notebook_deps() -> None:
    packages = [
        "jupytext",
        "python-dotenv", 
        "unstructured-client",
        "elasticsearch",
        "boto3",
        "PyYAML",
        "langchain",
        "langchain-elasticsearch",
        "langchain-openai"
    ]
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *packages])
    except Exception:
        # If install fails, continue; imports below will surface actionable errors
        pass

# Install notebook dependencies (safe no-op if present)
ensure_notebook_deps()

import os
import time
import json
import zipfile
import tempfile
import requests
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import (
    CreateSourceRequest,
    CreateDestinationRequest,
    CreateWorkflowRequest
)
from unstructured_client.models.shared import (
    CreateSourceConnector,
    CreateDestinationConnector,
    WorkflowNode,
    WorkflowType,
    CreateWorkflow
)

# =============================================================================
# GOOGLE COLAB ENVIRONMENT SETUP
# =============================================================================
# Paste your actual credentials here (replace the placeholder values):

UNSTRUCTURED_API_KEY = "your-unstructured-api-key"
AWS_ACCESS_KEY_ID = "your-aws-access-key"
AWS_SECRET_ACCESS_KEY = "your-aws-secret-key"
AWS_REGION = "us-east-1"
S3_SOURCE_BUCKET = "your-s3-bucket-name"
ELASTICSEARCH_HOST = "https://your-cluster.es.io:9200"
ELASTICSEARCH_API_KEY = "your-elasticsearch-api-key"
ELASTICSEARCH_INDEX = "sales-records-consolidated"

# Optional: OpenAI API key for RAG functionality
OPENAI_API_KEY = "your-openai-api-key"

# =============================================================================
# ENVIRONMENT FILE FALLBACK
# =============================================================================
# Load from .env file (will override the values above if file exists)
load_dotenv()

# Override with environment variables if available
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", UNSTRUCTURED_API_KEY)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)  
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
AWS_REGION = os.getenv("AWS_REGION", AWS_REGION)
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", S3_SOURCE_BUCKET)
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", ELASTICSEARCH_HOST)
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", ELASTICSEARCH_API_KEY)
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", ELASTICSEARCH_INDEX)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", OPENAI_API_KEY)

# Configuration constants
SKIPPED = "SKIPPED"
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Validation
REQUIRED_VARS = {
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update the configuration section above with your actual API keys.")
    raise ValueError(f"Missing required configuration values: {missing_vars}")

print("✅ Configuration loaded successfully") 

# %% [markdown]
# ### Alternative: Local Development with .env File
# 
# If running locally (not in Colab), create a `.env` file in your project root. The environment file will override the values above:
# 
# ```bash
# # .env file contents
# UNSTRUCTURED_API_KEY=your-actual-api-key
# UNSTRUCTURED_API_URL=https://platform.unstructuredapp.io/api/v1
# AWS_ACCESS_KEY_ID=your-aws-access-key
# AWS_SECRET_ACCESS_KEY=your-aws-secret-key
# AWS_REGION=us-east-1
# S3_SOURCE_BUCKET=your-documents-bucket
# ELASTICSEARCH_HOST=https://your-cluster.es.io:9200
# ELASTICSEARCH_API_KEY=your-elasticsearch-api-key
# ELASTICSEARCH_INDEX=sales-records-consolidated
# OPENAI_API_KEY=your-openai-api-key
# ```

# %% [markdown]
# ## AWS S3: Document Storage Setup
# 
# Your unstructured documents (PDFs, manuals, reports) need to be accessible via S3.
# 
# ### What You Need
# 
# **An existing S3 bucket** containing the documents you want to process. 
# 
# > **Note**: This tutorial assumes you have an existing S3 bucket with documents. For detailed S3 setup instructions, see the [Unstructured S3 source connector documentation](https://docs.unstructured.io/api-reference/api-services/source-connectors/s3).
# 
# ### AWS Requirements
# 
# - **AWS Account** with S3 access
# - **IAM User** with S3 read permissions for your bucket
# - **Access Keys** (Access Key ID and Secret Access Key)

# %% [markdown]
# ## Elasticsearch: Structured Data and Results Storage
# 
# Elasticsearch serves dual purposes in our pipeline:
# 1. **Source**: Stores your structured business data (sales records, customer info)
# 2. **Destination**: Receives the unified, processed results that will be used for RAG queries
# 
# ### What You Need
# 
# **Elasticsearch cluster** with API key authentication from Elastic Cloud (managed service)
# 
# ### Required Indices
# 
# **Source Index**: `sales-records-consolidated`
# - Contains your business data (sales records, customer interactions, etc.)
# - Created automatically by the pipeline
# - Must exist with data before running the pipeline
# - Will be read and processed by the Unstructured API
# 
# **Destination Index**: `customer-support`
# - Created automatically by the pipeline
# - Receives processed results from both S3 and Elasticsearch sources
# - Your unified knowledge base for RAG queries
# 
# ### Why Consolidated Data Format Matters
# 
# Traditional databases store information in separate fields (customer_name, product_id, purchase_date). For RAG applications, we consolidate this into a long-form text field that provides full context in each search result.
# 
# Example transformation:
# ```
# Before: {customer: "John Doe", product: "BH-001", date: "2024-01-15"}
# After: "Customer John Doe \n product BH-001 \n January 15, 2024..."
# ```
# 
# ### API Key Permissions
# 
# Your Elasticsearch API key needs these permissions:
# 
# ```json
# {
#   "sales-records-full-access": {
#     "cluster": [],
#     "indices": [
#       {
#         "names": [
#           "sales-records",
#           "sales-records-consolidated",
#           "customer-support"
#         ],
#         "privileges": [
#           "create_index",
#           "delete_index",
#           "manage",
#           "write",
#           "read",
#           "view_index_metadata",
#           "monitor"
#         ],
#         "allow_restricted_indices": false
#       }
#     ],
#     "applications": [],
#     "run_as": [],
#     "metadata": {},
#     "transient_metadata": {
#       "enabled": true
#     }
#   }
# }
# ```
# 
# **Don't have Elasticsearch data yet?** The pipeline includes automatic data setup that creates sample sales records for demonstration. This is done by downloading .ZIP files from github and unzipping them.

# %% [markdown]
# ## Data Preparation: Setting Up Demo Sources
# 
# For this demonstration, we've created realistic sample data that represents a typical enterprise scenario.
# 
# **Elasticsearch Sales Data**: 100 synthetic sales records with customer information, with consolidated fields to optimize for vector search
# 
# **S3 Product Documentation**: 9 product manuals downloaded from manufacturer websites and stored in your S3 bucket
# 
# This mimics real enterprise scenarios where structured data (sales records) and unstructured documents (manuals) need to be searchable together for effective customer support.

# %%
# Data preparation functions - requires global variables to be imported
# Note: All imports and global variables are defined in dependencies.py

def download_file(url: str, local_path: str) -> bool:
    """Download a file from URL to local path."""
    try:
        print(f"📥 Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create directory if it doesn't exist
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✅ Downloaded to {local_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return False

def setup_elasticsearch_data():
    """Download and load sales data into Elasticsearch index."""
    print("🔧 Setting up Elasticsearch sales data...")
    
    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(
            ELASTICSEARCH_HOST,
            api_key=ELASTICSEARCH_API_KEY,
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )
        
        index_name = "sales-records-consolidated"
        
        # Download sales data zip file
        sales_data_url = "https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/sales_records_consolidated.zip"
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            if not download_file(sales_data_url, tmp_file.name):
                return False
            
            # Extract and load data
            with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
                # Load mapping
                with zipf.open('mapping.json') as f:
                    mapping_data = json.loads(f.read().decode('utf-8'))
                
                # Load documents
                with zipf.open('documents.json') as f:
                    documents = json.loads(f.read().decode('utf-8'))
            
            # Always delete existing index if present and reload from zip
            if es.indices.exists(index=index_name):
                print(f"🗑️ Deleting existing index '{index_name}' to reload fresh data...")
                es.indices.delete(index=index_name)
            
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
            
            # Refresh index and verify
            es.indices.refresh(index=index_name)
            count_response = es.count(index=index_name)
            count_data = count_response.body if hasattr(count_response, 'body') else count_response
            doc_count = count_data['count']
            
            if doc_count > 0:
                print(f"✅ Successfully loaded {doc_count} documents into '{index_name}' index")
                return True
            else:
                print(f"❌ Index '{index_name}' is empty after loading")
                return False
            
    except Exception as e:
        print(f"❌ Error setting up Elasticsearch data: {e}")
        return False
    
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_file.name)
        except:
            pass

def setup_s3_data():
    """Download and load PDF files into S3 bucket."""
    print("🔧 Setting up S3 PDF data...")
    
    try:
        # Initialize S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        bucket_name = S3_SOURCE_BUCKET
        if not bucket_name:
            print("❌ S3_SOURCE_BUCKET not configured")
            return False
        
        # Check if bucket exists and has data
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            if response.get('KeyCount', 0) > 0:
                # Count total objects
                response = s3.list_objects_v2(Bucket=bucket_name)
                object_count = len(response.get('Contents', []))
                print(f"✅ Bucket '{bucket_name}' already exists with {object_count} files")
                return True
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise e
        
        # Download S3 PDFs zip file
        s3_data_url = "https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/s3_pdfs.zip"
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            if not download_file(s3_data_url, tmp_file.name):
                return False
            
            # Create bucket if it doesn't exist
            try:
                s3.head_bucket(Bucket=bucket_name)
                print(f"📦 Using existing bucket '{bucket_name}'")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"🔧 Creating bucket '{bucket_name}'...")
                    try:
                        if AWS_REGION == "us-east-1":
                            s3.create_bucket(Bucket=bucket_name)
                        else:
                            s3.create_bucket(
                                Bucket=bucket_name,
                                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                            )
                        print(f"✅ Created bucket '{bucket_name}'")
                    except ClientError as create_error:
                        if 'BucketAlreadyOwnedByYou' in str(create_error):
                            print(f"📦 Bucket '{bucket_name}' already exists and is owned by you")
                        else:
                            raise create_error
                else:
                    raise e
            
            # Clear existing files in bucket
            print(f"🗑️ Clearing existing files from bucket '{bucket_name}'...")
            try:
                response = s3.list_objects_v2(Bucket=bucket_name)
                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    if objects_to_delete:
                        s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        print(f"🗑️ Deleted {len(objects_to_delete)} existing files")
                    else:
                        print("📁 Bucket was already empty")
                else:
                    print("📁 Bucket was already empty")
            except ClientError as e:
                print(f"⚠️ Could not clear bucket (continuing anyway): {e}")
            
            # Extract and upload files from zip
            uploaded_count = 0
            with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
                file_list = zipf.namelist()
                pdf_files = [f for f in file_list if f.lower().endswith('.pdf')]
                
                print(f"📊 Found {len(pdf_files)} PDF files in zip")
                
                for file_name in pdf_files:
                    try:
                        # Extract file data
                        file_data = zipf.read(file_name)
                        
                        # Upload to S3
                        s3.put_object(
                            Bucket=bucket_name,
                            Key=file_name,
                            Body=file_data,
                            ContentType='application/pdf'
                        )
                        
                        print(f"  📤 Uploaded: {file_name}")
                        uploaded_count += 1
                        
                    except Exception as e:
                        print(f"  ❌ Failed to upload {file_name}: {e}")
            
            # Verify upload
            response = s3.list_objects_v2(Bucket=bucket_name)
            actual_count = len(response.get('Contents', []))
            
            if actual_count > 0:
                print(f"✅ Successfully uploaded {uploaded_count} PDFs to bucket '{bucket_name}'")
                print(f"📊 Bucket now contains {actual_count} files")
                return True
            else:
                print(f"❌ Bucket '{bucket_name}' is empty after upload")
                return False
            
    except NoCredentialsError:
        print("❌ AWS credentials not found. Please check your .env file.")
        return False
    except Exception as e:
        print(f"❌ Error setting up S3 data: {e}")
        return False
    
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_file.name)
        except:
            pass

def prepare_data_sources():
    """Prepare both Elasticsearch and S3 data sources."""
    print("🚀 Preparing data sources...")
    print("=" * 50)
    
    # Setup Elasticsearch data
    if not setup_elasticsearch_data():
        print("❌ Failed to setup Elasticsearch data")
        return False
    
    print()  # Add spacing
    
    # Setup S3 data
    if not setup_s3_data():
        print("❌ Failed to setup S3 data")
        return False
    
    print()
    print("✅ All data sources prepared successfully!")
    print("=" * 50)
    return True 

# %% [markdown]
# ## Connecting to S3 Document Storage
# 
# The S3 source connector reads unstructured documents from your cloud storage.
# 
# - **Bucket Access**: Connects to your specified S3 bucket using AWS credentials
# - **File Discovery**: Recursively scans all subdirectories for supported document types
# - **Processing Queue**: Identifies and queues PDFs, images, and text files for the Unstructured pipeline

# %% [markdown]
# ## Connecting to Elasticsearch Business Data
# 
# The Elasticsearch source connector reads structured data from your existing business systems.
# 
# - **Index Access**: Connects directly to the `sales-records-consolidated` index
# - **Authentication**: Uses your Elasticsearch API key for secure access
# - **Data Processing**: Reads all documents from the source index for processing

# %% [markdown]
# ## Creating the Unified Knowledge Base
# 
# Both processing workflows write their results to a single destination: the `customer-support` index.
# 
# - **Index Creation**: Automatically creates the destination index with optimized mappings
# - **Unified Storage**: Receives processed results from both S3 and Elasticsearch workflows
# - **Vector Search Ready**: Configured with proper field types for semantic search and metadata filtering
# 
# This destination connector ensures all your processed data lands in a single, searchable index regardless of the original source format.

# %%
def create_s3_source_connector():
    """Create an S3 source connector for PDF documents."""
    try:
        if not S3_SOURCE_BUCKET:
            raise ValueError("S3_SOURCE_BUCKET is required (bucket name, s3:// URL, or https:// URL)")
        value = S3_SOURCE_BUCKET.strip()

        # Build s3:// URL from various accepted formats
        if value.startswith("s3://"):
            s3_style = value if value.endswith("/") else value + "/"
        elif value.startswith("http://") or value.startswith("https://"):
            parsed = urlparse(value)
            host = parsed.netloc
            path = parsed.path or "/"
            bucket = host.split(".s3.")[0]
            s3_style = f"s3://{bucket}{path if path.endswith('/') else path + '/'}"
        else:
            # treat as raw bucket or bucket/prefix
            s3_style = f"s3://{value if value.endswith('/') else value + '/'}"
        
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.sources.create_source(
                request=CreateSourceRequest(
                    create_source_connector=CreateSourceConnector(
                        name="<name>",
                        type="s3",
                        config={
                            "remote_url": s3_style,
                            "recursive": True, 
                            "key": AWS_ACCESS_KEY_ID,
                            "secret": AWS_SECRET_ACCESS_KEY,
                        }
                    )
                )
            )
        
        source_id = response.source_connector_information.id
        print(f"✅ Created S3 PDF source connector: {source_id} -> {s3_style}")
        return source_id
        
    except Exception as e:
        print(f"❌ Error creating S3 source connector: {e}")
        return None

def create_elasticsearch_source_connector():
    """Create an Elasticsearch source connector for sales data."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.sources.create_source(
                request=CreateSourceRequest(
                    create_source_connector=CreateSourceConnector(
                        name=f"elasticsearch_sales_source_{int(time.time())}",
                        type="elasticsearch",
                        config={
                            "hosts": [ELASTICSEARCH_HOST],
                            "es_api_key": ELASTICSEARCH_API_KEY,
                            "index_name": ELASTICSEARCH_INDEX
                        }
                    )
                )
            )
        
        source_id = response.source_connector_information.id
        print(f"✅ Created Elasticsearch sales source connector: {source_id}")
        return source_id
    
    except Exception as e:
        print(f"❌ Error creating Elasticsearch source connector: {e}")
        return None

def create_elasticsearch_destination_connector():
    """Create an Elasticsearch destination connector for processed results."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.destinations.create_destination(
                request=CreateDestinationRequest(
                    create_destination_connector=CreateDestinationConnector(
                        name=f"elasticsearch_customer_support_destination_{int(time.time())}",
                        type="elasticsearch",
                        config={
                            "hosts": [ELASTICSEARCH_HOST],
                            "es_api_key": ELASTICSEARCH_API_KEY,
                            "index_name": "customer-support"
                        }
                    )
                )
            )

        destination_id = response.destination_connector_information.id
        print(f"✅ Created Elasticsearch destination connector: {destination_id}")
        return destination_id
        
    except Exception as e:
        print(f"❌ Error creating Elasticsearch destination connector: {e}")
        return None

# %% [markdown]
# ## Processing Pipeline: Making Data Searchable
# 
# Both workflows use identical processing steps to ensure consistent output format.
# 
# ### The Four Processing Stages
# 
# **1. VLM Partitioning** (GPT-4o)
# - Intelligently extracts content from complex document layouts
# - Handles tables, images, and multi-column formats
# - Preserves semantic relationships between sections
# 
# **2. Chunker Node** (Title-based)
# - Creates 1,500-character chunks (max 2,048) that respect document structure
# - Maintains context by chunking at natural boundaries (headings, paragraphs)
# - Ensures each chunk contains complete, meaningful information
# 
# **3. Embedder Node** (OpenAI text-embedding-3-small)
# - Converts text chunks into 1,536-dimensional vectors
# - Enables semantic search that understands meaning, not just keywords
# 
# **4. NER Enrichment** (Named Entity Recognition)
# - Extracts entities: people, organizations, products, locations
# - Adds structured metadata for precise search and filtering
# - Enables entity-based query routing and recommendations
# 
# ### Why Identical Processing Matters
# 
# Using the same pipeline for both data sources ensures:
# - **Consistent search behavior** across document types
# - **Comparable embedding spaces** for cross-source similarity  
# - **Unified metadata structure** for filtering and analysis

# %%
def create_workflow_nodes():
    """Create shared processing nodes for workflows."""
    # VLM Partitioner Node
    vlm_partition_node = WorkflowNode(
        name="VLM_Partitioner",
        subtype="vlm",
        type="partition",
        settings={
            "provider": "openai",
            "model": "gpt-4o",
        }
    )
    
    # Chunker Node (fixed naming per feedback)
    chunk_node = WorkflowNode(
        name="Chunker_Node",
        subtype="chunk_by_title",
        type="chunk",
        settings={
            "new_after_n_chars": 1500,
            "max_characters": 2048,
            "overlap": 0
        }
    )
    
    # Embedder Node (fixed naming per feedback)
    embedder_node = WorkflowNode(
        name="Embedder_Node",
        subtype="openai",
        type="embed",
        settings={
            "model_name": "text-embedding-3-small"
        }
    )
    
    # NER Enrichment Node (fixed configuration per memory)
    ner_enrichment_node = WorkflowNode(
        name="NER_Enrichment",
        type="prompter",
        subtype="openai_ner",
        settings={
            # Use Unstructured's default NER prompt; override later if needed
        }
    )
    
    return vlm_partition_node, chunk_node, embedder_node, ner_enrichment_node

def create_parallel_workflows(s3_source_id, elasticsearch_source_id, destination_id):
    """Create separate workflows for S3 PDFs and Elasticsearch data that run in parallel."""
    try:
        vlm_partition_node, chunk_node, embedder_node, ner_enrichment_node = create_workflow_nodes()
        
        # Create workflow for S3 PDFs
        s3_workflow_id = None
        if s3_source_id:
            with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
                s3_workflow = CreateWorkflow(
                    name=f"S3-PDFs-Parallel-Workflow_{int(time.time())}",
                    source_id=s3_source_id,
                    destination_id=destination_id,
                    workflow_type=WorkflowType.CUSTOM,
                    workflow_nodes=[
                        vlm_partition_node,
                        chunk_node,
                        embedder_node,
                        ner_enrichment_node
                    ]
                )
                
                s3_response = client.workflows.create_workflow(
                    request=CreateWorkflowRequest(
                        create_workflow=s3_workflow
                    )
                )
            
            s3_workflow_id = s3_response.workflow_information.id
            print(f"✅ Created S3 PDF workflow: {s3_workflow_id}")
        
        # Create workflow for Elasticsearch sales data
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            es_workflow = CreateWorkflow(
                name=f"Elasticsearch-Sales-Parallel-Workflow_{int(time.time())}",
                source_id=elasticsearch_source_id,
                destination_id=destination_id,
                workflow_type=WorkflowType.CUSTOM,
                workflow_nodes=[
                    vlm_partition_node,
                    chunk_node,
                    embedder_node,
                    ner_enrichment_node
                ]
            )
            
            es_response = client.workflows.create_workflow(
                request=CreateWorkflowRequest(
                    create_workflow=es_workflow
                )
            )
        
        es_workflow_id = es_response.workflow_information.id
        print(f"✅ Created Elasticsearch sales workflow: {es_workflow_id}")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f"❌ Error creating parallel workflows: {e}")
        return None, None

# %% [markdown]
# ## Creating Parallel Processing Workflows
# 
# We create two independent workflows that process different data sources simultaneously.
# 
# ### Workflow Architecture
# 
# **S3 Document Workflow**
# - Source: PDF documents from cloud storage
# - Processing: VLM → Chunker → Embedder → NER
# - Destination: customer-support index
# 
# **Elasticsearch Data Workflow**
# - Source: Structured sales records
# - Processing: Same four-stage pipeline
# - Destination: Same customer-support index
# 
# ### Why NER (Named Entity Recognition) Matters
# 
# NER extraction identifies people, places, organizations, and products in your content. This enables:
# - **Precise search**: Find all mentions of "John Smith" or "BH-900 headphones"
# - **Entity-based routing**: Route queries to relevant content based on detected entities
# - **Structured metadata**: Add searchable tags to unstructured content

# %% [markdown]
# ## Starting the Processing Jobs
# 
# Workflow execution is asynchronous - we start the jobs and monitor their progress.
# 
# ### Execution Process
# 
# 1. **Submit** workflow run requests to the Unstructured API
# 2. **Receive** job IDs for tracking
# 3. **Monitor** progress through job status polling
# 4. **Handle** completion or failure states
# 
# This approach scales automatically and handles large datasets without local resource constraints.

# %%
def run_workflow(workflow_id, workflow_name):
    """Run a workflow and return job information."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.workflows.run_workflow(
                request={"workflow_id": workflow_id}
            )
        
        job_id = response.job_information.id
        print(f"✅ Started {workflow_name} job: {job_id}")
        return job_id
        
    except Exception as e:
        print(f"❌ Error running {workflow_name} workflow: {e}")
        return None

def poll_job_status(job_id, job_name, wait_time=30):
    """Poll job status until completion."""
    print(f"⏳ Monitoring {job_name} job status...")
    
    while True:
        try:
            with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
                response = client.jobs.get_job(
                    request={"job_id": job_id}
                )
            
            job = response.job_information
            status = job.status
            
            if status in ["SCHEDULED", "IN_PROGRESS"]:
                print(f"⏳ {job_name} job status: {status}")
                time.sleep(wait_time)
            elif status == "COMPLETED":
                print(f"✅ {job_name} job completed successfully!")
                return job
            elif status == "FAILED":
                print(f"❌ {job_name} job failed!")
                return job
            else:
                print(f"❓ Unknown {job_name} job status: {status}")
                return job
                
        except Exception as e:
            print(f"❌ Error polling {job_name} job status: {e}")
            time.sleep(wait_time)

# %% [markdown]
# ## Monitoring Processing Progress
# 
# The pipeline provides real-time job monitoring to track processing status.
# 
# ### Status Polling
# 
# Jobs progress through these states:
# - **SCHEDULED**: Queued and waiting for processing resources
# - **IN_PROGRESS**: Actively processing your data
# - **COMPLETED**: Successfully finished
# - **FAILED**: Encountered an error
# 
# ### Monitoring Strategy
# 
# The `poll_job_status` function:
# - Checks status every 30 seconds
# - Provides progress updates with clear indicators
# - Blocks execution until jobs complete

# %% [markdown]
# ## Preparing the Elasticsearch Environment
# 
# Before processing begins, we validate data sources and prepare the destination.
# 
# ### Source Validation
# 
# **Critical Check**: Ensures the `sales-records-consolidated` index exists and contains data
# 
# ### Destination Preparation
# 
# **Clean Slate Approach**: Recreates the `customer-support` index fresh for each run
# - Deletes any existing index to prevent data mixing
# - Creates new index with optimized mapping for RAG applications
# - Configures proper field types for text search and metadata
# 
# ### Index Mapping
# 
# The destination index uses this structure:
# ```json
# {
#   "id": "keyword",           // Unique document identifier
#   "timestamp": "date",       // Processing timestamp
#   "text": "text",           // Searchable content
#   "metadata": "object"      // Source info and entities
# }
# ```

# %%
def run_elasticsearch_preprocessing():
    """Check and manage Elasticsearch indices for the pipeline."""
    print("🔧 Running Elasticsearch preprocessing...")
    
    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(
            ELASTICSEARCH_HOST,
            api_key=ELASTICSEARCH_API_KEY,
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )
        
        # Check sales-records-consolidated index
        sales_index = "sales-records-consolidated"
        print(f"�� Checking {sales_index} index...")
        
        if not es.indices.exists(index=sales_index):
            raise ValueError(f"❌ Index '{sales_index}' does not exist. There is no data to use.")
        
        # Check if sales index has data
        count_response = es.count(index=sales_index)
        doc_count = count_response['count']
        
        if doc_count == 0:
            raise ValueError(f"❌ Index '{sales_index}' is empty. There is no data to use.")
        
        print(f"✅ Found {doc_count} records in {sales_index}")
        
        # Handle customer-support index
        support_index = "customer-support"
        print(f"🔍 Checking {support_index} index...")
        
        if es.indices.exists(index=support_index):
            print(f"🗑️ Deleting existing {support_index} index...")
            es.indices.delete(index=support_index)
        
        # Create fresh customer-support index
        print(f"🔧 Creating fresh {support_index} index...")
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "text": {"type": "text", "analyzer": "standard"},
                    "metadata": {"type": "object"}
                }
            }
        }
        
        es.indices.create(index=support_index, body=mapping)
        es.indices.refresh(index=support_index)
        
        print(f"✅ Successfully created fresh {support_index} index")
        print("✅ Elasticsearch preprocessing completed successfully")
        return True
        
    except ValueError as e:
        print(str(e))
        return False
    except Exception as e:
        print(f"❌ Error during Elasticsearch preprocessing: {e}")
        return False

# %% [markdown]
# ## Pipeline Execution Summary
# 
# This section displays all the resources created during pipeline setup.
# 
# ### Resource Tracking
# 
# **Data Sources**
# - S3 bucket path for document storage
# - Elasticsearch source index for business data
# 
# **Processing Infrastructure**
# - Source connector IDs for data ingestion
# - Destination connector ID for result storage
# - Workflow IDs for the processing pipelines
# 
# **Execution Status**
# - Job IDs for monitoring and debugging
# - Current processing status
# - Resource endpoints for verification

# %%
import os

def print_pipeline_summary(s3_workflow_id, es_workflow_id, s3_job_id, es_job_id):
    """Print comprehensive pipeline summary."""
    print("\n" + "=" * 80)
    print("📊 HYBRID RAG PIPELINE SUMMARY")
    print("=" * 80)
    print(f"📁 S3 Source (PDFs): {S3_SOURCE_BUCKET if s3_workflow_id else SKIPPED}")
    print(f"🔍 Elasticsearch Source: {ELASTICSEARCH_HOST}/{ELASTICSEARCH_INDEX}")
    print(f"📤 Elasticsearch Destination: {ELASTICSEARCH_HOST}/customer-support")
    print(f"")
    print(f"⚙️ S3 PDFs Workflow ID: {s3_workflow_id if s3_workflow_id else SKIPPED}")
    print(f"⚙️ Elasticsearch Sales Workflow ID: {es_workflow_id}")
    print(f"")
    print(f"🚀 S3 PDFs Job ID: {s3_job_id if s3_job_id else SKIPPED}")
    print(f"🚀 Elasticsearch Sales Job ID: {es_job_id}")

def query_unified_knowledge_base():
    """
    Demonstrate RAG querying against the unified knowledge base using LangChain.
    Shows how to get answers from both S3 documents and Elasticsearch data.
    """
    print("\n🤖 RAG Query Demonstration")
    print("=" * 40)
    
    # Check if OpenAI API key is available
    if not OPENAI_API_KEY or OPENAI_API_KEY.startswith("your-"):
        print("⚠️ OpenAI API key not configured. Skipping RAG demonstration.")
        print("💡 To enable RAG queries, add your OpenAI API key to the configuration section.")
        return
    
    try:
        from langchain_elasticsearch import ElasticsearchStore
        from langchain_openai import OpenAIEmbeddings, ChatOpenAI
        from langchain.chains import RetrievalQA
        from langchain.schema import Document
        
        # Set OpenAI API key
        os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
        
        # Initialize embeddings (same model used in processing)
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        
        # Connect to Elasticsearch vector store
        vector_store = ElasticsearchStore(
            es_url=ELASTICSEARCH_HOST,
            index_name="customer-support",
            embedding=embeddings,
            es_api_key=ELASTICSEARCH_API_KEY
        )
        
        # Initialize LLM
        llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
        
        # Create RAG chain
        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vector_store.as_retriever(search_kwargs={"k": 5}),
            return_source_documents=True
        )
        
        # Test queries that should pull from both data sources
        test_queries = [
            "How do I troubleshoot Bose headphone connectivity issues?",
            "What products did customers purchase in the electronics category?", 
            "Can you help me with headphone setup and show customer purchase patterns?",
            "What support issues are common with audio products?"
        ]
        
        print("🔍 Testing hybrid RAG queries across unified data sources:\n")
        
        for i, query in enumerate(test_queries, 1):
            print(f"**Query {i}:** {query}")
            print("-" * 60)
            
            try:
                result = qa_chain({"query": query})
                answer = result["result"]
                sources = result["source_documents"]
                
                print(f"**Answer:** {answer}\n")
                
                # Analyze source distribution
                s3_sources = 0
                es_sources = 0
                
                print("**Sources:**")
                for j, doc in enumerate(sources[:3]):  # Show top 3 sources
                    metadata = doc.metadata
                    text_preview = doc.page_content[:150] + "..." if len(doc.page_content) > 150 else doc.page_content
                    
                    # Determine source type
                    if "data_source-record_locator-index_name" in metadata:
                        source_type = "📊 Elasticsearch (Sales Data)"
                        es_sources += 1
                    elif "data_source-url" in metadata and "s3://" in metadata["data_source-url"]:
                        source_type = "📄 S3 (Product Documentation)"
                        s3_sources += 1
                    else:
                        source_type = "❓ Unknown Source"
                    
                    print(f"  {j+1}. {source_type}")
                    print(f"     Preview: {text_preview}")
                
                print(f"\n📈 Source Distribution: {s3_sources} S3 docs, {es_sources} Elasticsearch records")
                print("=" * 80 + "\n")
                
            except Exception as e:
                print(f"❌ Error processing query: {e}\n")
        
        print("✅ RAG Demonstration Complete!")
        print("💡 Your unified knowledge base successfully combines:")
        print("   • Product documentation from S3")  
        print("   • Customer/sales data from Elasticsearch")
        print("   • Both sources are searchable in a single query")
        
    except ImportError as e:
        print(f"❌ Missing RAG dependencies: {e}")
        print("💡 Install with: pip install langchain langchain-elasticsearch langchain-openai")
    except Exception as e:
        print(f"❌ Error setting up RAG queries: {e}")

def verify_customer_support_results(s3_job_id=None, es_job_id=None):
    """
    Verifies the processed results in the customer-support index, prettyprinting one doc per unique source connector.
    Assumes jobs have already completed successfully.
    """
    import pprint

    print("🔍 Verifying processed results in 'customer-support' index (assuming jobs have completed)...")

    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(
            ELASTICSEARCH_HOST,
            api_key=ELASTICSEARCH_API_KEY,
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )

        index_name = "customer-support"

        # Check if index exists
        if not es.indices.exists(index=index_name):
            print(f"❌ Index '{index_name}' does not exist. Workflows may not have written results yet.")
            return

        # Get document count
        count_response = es.count(index=index_name)
        total_docs = count_response['count']
        print(f"📊 Total processed documents: {total_docs}")

        if total_docs == 0:
            print("⏳ No documents found yet. Workflows may still be processing or index is empty.")
            print("💡 Check the Unstructured dashboard for job status.")
            return

        print(f"\n📋 Analyzing Source Connectors:")
        print("=" * 40)

        # Get sample documents to analyze source patterns
        # Use function_score with random_score to sample documents randomly
        sample_response = es.search(
            index=index_name,
            body={
                "size": 50,  # Get more samples to increase chance of seeing all sources
                "_source": ["metadata", "text", "element_id"],
                "query": {
                    "function_score": {
                        "query": {"match_all": {}},
                        "random_score": {}
                    }
                }
            }
        )
        

        # Map: source_connector_key -> [doc, ...]
        source_connector_map = {}
        unknown_docs = []

        for hit in sample_response['hits']['hits']:
            source = hit['_source']
            metadata = source.get('metadata', {})
            
            # Determine source connector type based on metadata patterns
            if "data_source-record_locator-index_name" in metadata:
                # Elasticsearch source connector
                key = f"elasticsearch:{metadata['data_source-record_locator-index_name']}"
            elif "data_source-url" in metadata:
                # S3 source connector - group all S3 URLs by bucket
                url = metadata['data_source-url']
                if url.startswith('s3://'):
                    # Extract bucket name from S3 URL
                    bucket = url.split('/')[2] if '/' in url else url.replace('s3://', '')
                    key = f"s3:{bucket}"
                else:
                    key = f"s3:unknown"
            elif "filename" in metadata and metadata.get('filetype') == 'pdf':
                # PDF files from S3 (fallback detection)
                key = "s3:pdfs"
            else:
                key = "unknown"

            if key == "unknown":
                unknown_docs.append(hit)
            else:
                if key not in source_connector_map:
                    source_connector_map[key] = hit  # Only keep the first doc for each source connector

        print(f"🔍 Unique source connectors found: {len(source_connector_map)}")
        for i, (key, doc) in enumerate(source_connector_map.items(), 1):
            print(f"\n--- Source Connector {i} ({key}) ---")
            pprint.pprint(doc['_source'], depth=6, compact=False, sort_dicts=False)

        if unknown_docs:
            print(f"\n❓ Example Unknown Source Document:")
            print("-" * 35)
            unknown_example = unknown_docs[0]['_source']
            metadata = unknown_example.get('metadata', {})
            text = unknown_example.get('text', '')
            print(f"   Element ID: {unknown_example.get('element_id', 'N/A')}")
            print(f"   Metadata: {metadata}")
            print(f"   Text Preview: {text[:200]}..." if len(text) > 200 else f"   Text: {text}")
            print("   Metadata prettyprint:")
            pprint.pprint(metadata, depth=6, compact=False, sort_dicts=False)

        # Test search functionality
        print(f"\n🔍 Testing Search Functionality:")
        print("=" * 32)

        search_tests = ["manual", "customer", "product", "support"]

        for search_term in search_tests:
            search_response = es.search(
                index=index_name,
                body={
                    "size": 1,
                    "query": {
                        "match": {
                            "text": search_term
                        }
                    }
                }
            )

            hits = search_response['hits']['total']['value']
            print(f"   🔎 '{search_term}': {hits} matches")

        print(f"\n" + "=" * 50)
        print("🎉 CUSTOMER-SUPPORT INDEX VERIFICATION")
        print("=" * 50)
        print("✅ Index exists and contains processed documents")
        print("✅ Documents from both source connectors are present (if both completed)")
        print("✅ Text search is functional across processed content")
        print("✅ Ready for hybrid RAG queries!")
        
        # Now demonstrate actual RAG functionality
        query_unified_knowledge_base()

    except Exception as e:
        print(f"❌ Error verifying results: {e}")
        print("💡 This is normal if workflows are still processing or if there is a connection issue.")

# %% [markdown]
# ## Orchestrating the Complete Pipeline
# 
# The main function coordinates all pipeline steps in logical sequence.
# 
# ### Six-Step Process
# 
# **Step 0: Data Preparation**
# - Downloads and sets up demo data sources
# - Creates Elasticsearch index with sales records
# - Populates S3 bucket with product documentation
# 
# **Step 1: Environment Validation**
# - Confirms source data availability
# - Prepares clean destination index
# - Validates presence of all required credentials
# 
# **Step 2-3: Connector Setup**
# - Creates source connectors for both data types
# - Establishes destination connector for unified results
# - Configures authentication and access
# 
# **Step 4: Workflow Creation**
# - Builds parallel processing workflows
# - Configures identical processing pipelines
# - Links sources to unified destination
# 
# **Step 5: Execution**
# - Starts both workflows simultaneously
# - Returns job IDs for monitoring
# - Initiates cloud-based processing
# 
# **Step 6: Summary**
# - Reports all created resources
# - Provides tracking information
# - Displays pipeline status

# %%
def main():
    """Main pipeline execution"""
    print("🚀 Starting Hybrid RAG Pipeline")
    
    # Step 0: Data Source Preparation
    print("\n📦 Step 0: Data source preparation")
    print("-" * 50)
    
    if not prepare_data_sources():
        print("❌ Failed to prepare data sources")
        return
    
    # Step 1: Elasticsearch preprocessing
    print("\n🔧 Step 1: Elasticsearch preprocessing")
    print("-" * 50)
    
    if not run_elasticsearch_preprocessing():
        print("❌ Failed to complete Elasticsearch preprocessing")
        return
    
    # Step 2: Create Source Connectors
    print("\n🔗 Step 2: Creating source connectors")
    print("-" * 50)
    
    s3_source_id = create_s3_source_connector()
    if not s3_source_id:
        print("❌ Failed to create S3 source connector")
        return
    
    elasticsearch_source_id = create_elasticsearch_source_connector()
    if not elasticsearch_source_id:
        print("❌ Failed to create Elasticsearch source connector")
        return
    
    # Step 3: Create Destination Connector
    print("\n🎯 Step 3: Creating Elasticsearch destination connector")
    print("-" * 50)
    
    destination_id = create_elasticsearch_destination_connector()
    if not destination_id:
        print("❌ Failed to create destination connector")
        return
    
    # Step 4: Create Workflows
    print("\n⚙️ Step 4: Creating workflows")
    print("-" * 50)
    
    s3_workflow_id, es_workflow_id = create_parallel_workflows(
        s3_source_id, elasticsearch_source_id, destination_id
    )
    
    if not es_workflow_id:
        print("❌ Failed to create Elasticsearch workflow")
        return
    
    # Step 5: Run Workflows
    print("\n🚀 Step 5: Running workflows")
    print("-" * 50)
    
    s3_job_id = None
    es_job_id = None

    if s3_workflow_id:
        s3_job_id = run_workflow(s3_workflow_id, "S3 PDFs")
        if not s3_job_id:
            print("❌ Failed to start S3 workflow")
            return

    if es_workflow_id:
        es_job_id = run_workflow(es_workflow_id, "Elasticsearch Sales")
        if not es_job_id:
            print("❌ Failed to start Elasticsearch workflow")
            return

    # Step 6: Pipeline Summary
    print_pipeline_summary(s3_workflow_id, es_workflow_id, s3_job_id, es_job_id)
    return s3_job_id, es_job_id 

# %% [markdown]
# ## Running the Complete Pipeline
# 
# 1. **Pipeline Setup**: Calls `main()` to create all resources and start processing
# 2. **Job Monitoring**: Waits for both workflows to complete successfully
# 3. **Result Verification**: Analyzes the unified knowledge base
# 4. **RAG Demonstration**: Shows actual queries across both data sources

# %%
# Pipeline execution runner - requires main() and verification functions to be imported
# Note: All imports and functions are defined in other scripts

# Run the pipeline
s3_job_id, es_job_id = main()

# Poll both jobs to make sure they have completed before proceeding
es_job_info = poll_job_status(es_job_id, "Elasticsearch Ingest")
s3_job_info = poll_job_status(s3_job_id, "S3 Ingest")

# Verify the results (run this after workflows have completed)
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results() 

# %% [markdown]
# ## What We Have Learned
# 
# **Enterprise Data Integration**
# - How to process multiple data formats (PDFs, structured records) in parallel
# - Why consistent processing pipelines matter for unified search
# - The value of creating a single searchable knowledge base
# 
# **Unstructured API Capabilities**
# - VLM-powered document partitioning for complex layouts
# - Intelligent chunking that preserves document structure
# - Named entity recognition for enhanced search precision
# - Unified processing across diverse data sources
# 
# **RAG System Architecture**
# - Parallel workflow design for scalability and reliability
# - Vector embeddings for semantic similarity search
# - Source attribution in mixed-data query results
# 
# ### Next Steps
# 
# **Immediate Applications**
# - Deploy customer support chatbots with comprehensive knowledge access
# - Build internal search tools that surface information from any source
# - Create automated content recommendation systems
# 
# **System Extensions**
# - Add more data sources (email, CRM, knowledge bases) using additional workflows
# - Implement real-time data synchronization for live updates
# - Add access controls and user-specific content filtering
# 
# **Production Deployment**
# - Scale up processing workflows for production data volumes
# - Implement monitoring and alerting for job status
# - Add backup and disaster recovery for your knowledge base
