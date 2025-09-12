#!/usr/bin/env python3
# %% [markdown]
# # Hybrid RAG Pipeline: Multi-Source Data Processing with Unstructured API
# 
# This notebook demonstrates processing data from multiple sources (S3 PDFs and Elasticsearch records) 
# using the Unstructured Workflow API and writing results to a unified destination index.
# 
# ## Architecture Overview
# 
# Two parallel workflows process different data sources and write to the same Elasticsearch index:
# 
# ```
# ┌─────────────────┐                           ┌─────────────────────────┐
# │   S3 PDFs       │──── WORKFLOW 1 ──────────▶│                         │
# │ (Documents)     │                           │    Unstructured API     │
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
# ## Processing Pipeline
# 
# Each workflow applies identical processing steps to ensure consistent output format:
# 
# 1. **Partition**: VLM-based document parsing using OpenAI GPT-4o
# 2. **Chunk**: Title-based chunking with 1,500 character trigger, 2,048 character maximum
# 3. **Embed**: Vector embeddings using OpenAI text-embedding-3-small model
# 4. **NER**: Named entity recognition to extract structured metadata
# 5. **Store**: Write processed results to Elasticsearch destination index
# 
# Both workflows write to the same `customer-support` index, creating a unified knowledge base 
# from heterogeneous data sources.
# %%

# %% [markdown]
# ## Unstructured API Key Setup
# ### Sign up, sign in, and get your API key
# 
# If you do not already have an Unstructured account, sign up for free. After you sign up, you are automatically signed in to your new Unstructured Starter account, at https://platform.unstructured.io.
# 
# To sign up for a Team or Enterprise account instead, contact Unstructured Sales, or learn more: https://docs.unstructured.io/ui/account/workspaces#create-an-api-key-for-a-workspace 
# 
# If you have an Unstructured Starter or Team account and are not already signed in, sign in to your account at https://platform.unstructured.io.
# 
# For an Enterprise account, see your Unstructured account administrator for instructions, or email Unstructured Support at support@unstructured.io.
# 
# Get your Unstructured API key:
# 
# a. After you sign in to your Unstructured Starter account, click API Keys on the sidebar.
# 
# For a Team or Enterprise account, before you click API Keys, make sure you have selected the organizational workspace you want to create an API key for. Each API key works with one and only one organizational workspace. Learn more.
# 
# b. Click Generate API Key.
# 
# c. Follow the on-screen instructions to finish generating the key.
# 
# d. Click the Copy icon next to your new key to add the key to your system's clipboard. If you lose this key, simply return and click the Copy icon again.
# %%

# %% [markdown]
# ## AWS S3 Configuration
# 
# This pipeline requires an S3 bucket as a source for documents (PDFs, images, etc.) that will be processed.
# 
# ### AWS Setup Requirements
# 
# You need AWS credentials and an S3 bucket configured. The [AWS S3 source connector documentation](https://docs.unstructured.io/api-reference/workflow/sources/s3) provides general instructions for:
# 
# - Setting up AWS access key ID and secret access key
# - Configuring IAM policies for S3 bucket access
# - Creating and configuring S3 buckets
# - Setting up proper bucket permissions
# 
# ### Required S3 Bucket for This Pipeline
# 
# You'll need to create an S3 bucket (or use an existing one) that contains the documents you want to process.
# 
# ### Required Environment Variables
# 
# Add these AWS configuration values to your `.env` file:
# ```
# AWS_ACCESS_KEY_ID=your-aws-access-key-id
# AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
# AWS_REGION=us-east-1
# S3_SOURCE_BUCKET=your-source-bucket-name
# ```
# 
# ### S3 URL Format Support
# 
# The pipeline accepts various S3 URL formats:
# - Raw bucket name: `my-documents-bucket`
# - Bucket with prefix: `my-documents-bucket/pdfs`
# - S3 protocol URL: `s3://my-documents-bucket/pdfs/`
# - HTTPS URL: `https://my-documents-bucket.s3.us-east-1.amazonaws.com/pdfs/`
# 
# All formats are automatically normalized to the s3:// format required by the Unstructured API.
# %%

# %% [markdown]
# ## Elasticsearch Configuration
# 
# This pipeline uses Elasticsearch for both source data and destination storage of processed results.
# 
# ### Elasticsearch Setup Requirements
# 
# You need an Elasticsearch cluster with proper authentication configured. The [Elasticsearch source connector documentation](https://docs.unstructured.io/api-reference/workflow/sources/elasticsearch) and [Elasticsearch destination connector documentation](https://docs.unstructured.io/api-reference/workflow/destinations/elasticsearch) provide general instructions for:
# 
# - Setting up your Elasticsearch cluster
# - Configuring authentication and API keys
# - Creating and managing Elasticsearch indices
# - Setting up proper permissions and access control
# 
# ### Required Indices for This Pipeline
# 
# This notebook expects specific index names:
# 
# **Source Index**: `sales-records-consolidated`
# - Must exist and contain data before running the pipeline
# - Should contain your source sales records data
# - You need to create and populate this index with your data
# 
# **Destination Index**: `customer-support`
# - Created automatically by the pipeline with standardized mapping
# - Will contain the processed results from both S3 and Elasticsearch sources
# - Any existing index with this name will be deleted and recreated
# 
# ### Required Environment Variables
# 
# Add these Elasticsearch configuration values to your `.env` file:
# ```
# ELASTICSEARCH_HOST=https://your-elasticsearch-host:9200
# ELASTICSEARCH_API_KEY=your-elasticsearch-api-key
# ELASTICSEARCH_INDEX=sales-records-consolidated
# ```
# 
# ### Pipeline Validation
# 
# The pipeline validates that the source index (`sales-records-consolidated`) exists and contains data before proceeding with workflow execution. If the index is missing or empty, the pipeline will exit with an error.
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
    ]
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *packages])
    except Exception:
        # If install fails, continue; imports below will surface actionable errors
        pass

# Install notebook dependencies (safe no-op if present)
ensure_notebook_deps()

import os
import sys
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
    SourceConnectorType,
    CreateDestinationConnector,
    WorkflowNode,
    WorkflowType,
    CreateWorkflow
)
from elasticsearch import Elasticsearch

# Install notebook dependencies (safe no-op if present)
ensure_notebook_deps()

# Load environment variables
load_dotenv()

# %% [markdown]
# ## Environment Configuration
# 
# The pipeline requires authentication and endpoint configuration. Choose **ONE** of these two methods:
# 
# ### Method 1: Using a .env File (RECOMMENDED)
# 
# Create a `.env` file in the project root with your actual values:
# ```
# AWS_ACCESS_KEY_ID=your-actual-access-key-id
# AWS_SECRET_ACCESS_KEY=your-actual-secret-access-key
# AWS_REGION=us-east-1
# S3_SOURCE_BUCKET=your-source-bucket-name
# UNSTRUCTURED_API_KEY=your-actual-unstructured-api-key
# ELASTICSEARCH_HOST=https://your-elasticsearch-host:9200
# ELASTICSEARCH_API_KEY=your-actual-elasticsearch-api-key
# ELASTICSEARCH_INDEX=sales-records-consolidated
# ```
# 
# ### Method 2: Direct Assignment in Notebook
# 
# If you prefer to paste credentials directly in the notebook:
# 
# 1. **Find the section** marked with `# Method 2: Direct assignment` in the code cell below
# 2. **Uncomment the lines** by removing the `#` at the beginning
# 3. **Replace** `PASTE_YOUR_VALUE_HERE` with your actual credentials
# 4. **Comment out** the corresponding `os.getenv()` lines above each section
# 
# **Look for these clear markers in the code:**
# - `# AWS_ACCESS_KEY_ID = "PASTE_YOUR_AWS_ACCESS_KEY_ID_HERE"`
# - `# UNSTRUCTURED_API_KEY = "PASTE_YOUR_UNSTRUCTURED_API_KEY_HERE"`
# - `# ELASTICSEARCH_HOST = "PASTE_YOUR_ELASTICSEARCH_HOST_HERE"`
# 
# ### Required Variables
# - **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
# - **S3**: `S3_SOURCE_BUCKET` (accepts bucket name, s3:// URL, or https:// URL)
# - **Unstructured API**: `UNSTRUCTURED_API_KEY`, `UNSTRUCTURED_API_URL`
# - **Elasticsearch**: `ELASTICSEARCH_HOST`, `ELASTICSEARCH_API_KEY`, `ELASTICSEARCH_INDEX`
# 
# ### Configuration Validation
# The script validates all required variables at startup and exits if any are missing 
# or contain placeholder values.
# 
# ### Dependency Management
# The `ensure_notebook_deps()` function automatically installs required Python packages:
# jupytext, python-dotenv, unstructured-client, elasticsearch, boto3, and PyYAML.
# %%
# Configuration
SKIPPED = "SKIPPED"

# =============================================================================
# CONFIGURATION OPTIONS - Choose ONE of the following methods:
# =============================================================================

# METHOD 1: Use .env file (RECOMMENDED)
# Create a .env file in the project root with your actual values
# Then keep all the os.getenv() lines below as-is

# METHOD 2: Paste your credentials directly below
# Comment out the os.getenv() lines and uncomment the direct assignment lines
# Replace "PASTE_YOUR_VALUE_HERE" with your actual credentials

# =============================================================================
# AWS CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-access-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")

# Method 2: Direct assignment (uncomment and paste your values)
# AWS_ACCESS_KEY_ID = "PASTE_YOUR_AWS_ACCESS_KEY_ID_HERE"
# AWS_SECRET_ACCESS_KEY = "PASTE_YOUR_AWS_SECRET_ACCESS_KEY_HERE"
# AWS_REGION = "us-east-1"
# S3_SOURCE_BUCKET = "PASTE_YOUR_SOURCE_BUCKET_NAME_HERE"

# =============================================================================
# UNSTRUCTURED API CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", "your-unstructured-api-key")
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Method 2: Direct assignment (uncomment and paste your values)
# UNSTRUCTURED_API_KEY = "PASTE_YOUR_UNSTRUCTURED_API_KEY_HERE"
# UNSTRUCTURED_API_URL = "https://platform.unstructuredapp.io/api/v1"

# =============================================================================
# ELASTICSEARCH CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")

# Method 2: Direct assignment (uncomment and paste your values)
# ELASTICSEARCH_HOST = "PASTE_YOUR_ELASTICSEARCH_HOST_HERE"  # e.g., "https://my-cluster.es.us-east-1.aws.com:9200"
# ELASTICSEARCH_API_KEY = "PASTE_YOUR_ELASTICSEARCH_API_KEY_HERE"
# ELASTICSEARCH_INDEX = "sales-records-consolidated"

# Validation
REQUIRED_VARS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    raise ValueError(f"Missing required configuration values: {missing_vars}")

print("✅ Configuration loaded successfully")

# Unstructured client will be initialized using context managers in each function

# %% [markdown]
# ## Data Source Preparation
# 
# Before running the workflows, this section automatically downloads and sets up the required source data from the GitHub repository.
# 
# ### Automatic Data Setup
# 
# The pipeline automatically handles data preparation by:
# 
# 1. **Downloading source data** from GitHub repository zip files
# 2. **Setting up Elasticsearch index** with synthetic sales records
# 3. **Creating and populating S3 bucket** with PDF documents
# 
# ### Data Sources
# 
# **Elasticsearch Sales Data:**
# - Downloads from: `https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/sales_data.zip`
# - Creates the `sales-records-consolidated` index
# - Loads 100 synthetic sales records with proper mapping
# 
# **S3 PDF Documents:**
# - Downloads from: `https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/s3_pdfs.zip`
# - Creates S3 bucket using the `S3_SOURCE_BUCKET` environment variable
# - Uploads 9 Bose headphone manuals and support documents
# 
# ### Smart Caching
# 
# The data preparation step is intelligent:
# - **Skips download** if data already exists and is populated
# - **Verifies data integrity** by checking document/file counts
# - **Reports status** of existing vs. newly created resources
# 
# ### Error Handling
# 
# If data preparation fails:
# - Clear error messages indicate the specific issue
# - Pipeline stops execution to prevent running with incomplete data
# - Common issues: network connectivity, authentication, or storage permissions
# 
# This automated setup ensures anyone can run the pipeline immediately after configuring their credentials, without manual data preparation steps.
# %%

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
        
        # Check if index already exists and has data
        index_name = "sales-records-consolidated"
        if es.indices.exists(index=index_name):
            count_response = es.count(index=index_name)
            count_data = count_response.body if hasattr(count_response, 'body') else count_response
            if count_data['count'] > 0:
                print(f"✅ Index '{index_name}' already exists with {count_data['count']} documents")
                return True
        
        # Download sales data zip file
        sales_data_url = "https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/sales_data.zip"
        
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
            
            # Delete existing index if present
            if es.indices.exists(index=index_name):
                print(f"🗑️ Deleting existing index '{index_name}'...")
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
# ## S3 Source Connector
# 
# Creates a source connector to read documents from an S3 bucket containing customer support materials.
# 
# ### Data Source: Amazon Headphone Manuals
# For this pipeline, the S3 bucket contains manuals and customer support materials downloaded directly from Amazon.com product pages for various headphones. These documents provide rich product information, troubleshooting guides, and user instructions that will be processed into searchable content.
# 
# ### URL Format Handling
# The connector accepts multiple S3 URL formats and normalizes them to s3:// format:
# - Raw bucket name: `example-data-bucket`
# - Bucket with prefix: `example-data-bucket/documents`
# - S3 protocol URL: `s3://example-data-bucket/documents/`
# - HTTPS URL: `https://example-data-bucket.s3.us-east-1.amazonaws.com/documents/`
# 
# ### Configuration Parameters
# - **remote_url**: Normalized s3:// URL for the source location
# - **recursive**: Set to `True` to process files in subdirectories
# - **key/secret**: AWS credentials for S3 access
# - **type**: Set to "s3" for S3 source connector
# 
# The function returns a source connector ID used to create the workflow.
# %%

def create_s3_source_connector():
    """Create an S3 source connector for PDF documents.
    """
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
        print(f":white_check_mark: Created S3 PDF source connector: {source_id} -> {s3_style}")
        return source_id
        
    except Exception as e:
        print(f":x: Error creating S3 source connector: {e}")
        return None

# %% [markdown]
# ## Elasticsearch Source Connector
# 
# Creates a source connector to read data from an existing Elasticsearch index containing sales records.
# 
# ### Data Source: Synthetic Consolidated Sales Data
# For this pipeline, the source index (`sales-records-consolidated`) contains synthetic sales data where multiple fields have been consolidated into a single long-form text field. This consolidation approach provides maximum context for vector search operations, allowing each sales record to be comprehensively searchable rather than having information fragmented across separate fields.
# 
# ### Data Source Configuration
# - **hosts**: List containing the Elasticsearch endpoint URL
# - **es_api_key**: API key for Elasticsearch authentication
# - **index_name**: Source index name (defaults to "sales-records-consolidated")
# - **type**: Set to "elasticsearch" for Elasticsearch source connector
# 
# ### Connector Naming
# Uses timestamp-based naming (`elasticsearch_sales_source_{timestamp}`) to ensure 
# unique connector names across multiple runs.
# 
# The function returns a source connector ID used to create the workflow.
# %%

def create_elasticsearch_source_connector():
    """Create an Elasticsearch source connector for sales data."""
    try:

        with UnstructuredClient(api_key_auth=os.getenv("UNSTRUCTURED_API_KEY")) as client:
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
        print(f":white_check_mark: Created Elasticsearch sales source connector: {source_id}")
        return source_id
    
    except Exception as e:
        print(f":x: Error creating Elasticsearch source connector: {e}")
        return None

# %% [markdown]
# ## Elasticsearch Destination Connector
# 
# Creates a destination connector where both workflows write their processed results.
# 
# ### Unified Destination Strategy
# Both the S3 PDF workflow and Elasticsearch sales workflow write to the same destination index:
# - Target index: `customer-support`
# - Same Elasticsearch cluster as the source data
# - Identical processing pipeline ensures consistent data format
# 
# ### Configuration Parameters
# - **hosts**: List containing the Elasticsearch endpoint URL
# - **es_api_key**: API key for Elasticsearch write access
# - **index_name**: Fixed as "customer-support" for the unified destination
# - **type**: Set to "elasticsearch" for Elasticsearch destination connector
# 
# ### Connector Naming
# Uses timestamp-based naming (`elasticsearch_customer_support_destination_{timestamp}`) 
# to ensure unique connector names.
# 
# The function returns a destination connector ID used by both workflows.
# %%

def create_elasticsearch_destination_connector():
    """Create an Elasticsearch destination connector for processed results."""
    try:
        with UnstructuredClient(api_key_auth=os.getenv("UNSTRUCTURED_API_KEY")) as client:
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

            print(response.destination_connector_information)

        destination_id = response.destination_connector_information.id
        print(f":white_check_mark: Created Elasticsearch destination connector: {destination_id}")
        return destination_id
        
    except Exception as e:
        print(f":x: Error creating Elasticsearch destination connector: {e}")
        return None

# %% [markdown]
# ## Processing Node Definitions
# 
# Defines four processing nodes that both workflows use to ensure consistent output format.
# 
# ### VLM Partitioner Node
# - **Type**: `partition` with `vlm` subtype
# - **Provider**: OpenAI
# - **Model**: GPT-4o
# - **Function**: Converts documents into structured JSON using vision-language model capabilities
# 
# ### Chunking Node
# - **Type**: `chunk` with `chunk_by_title` subtype
# - **Strategy**: Title-based chunking for semantic coherence
# - **Parameters**: 1,500 character trigger, 2,048 character maximum, 0 overlap
# - **Function**: Splits documents into retrieval-optimized chunks
# 
# ### Embedding Node
# - **Type**: `embed` with `openai` subtype
# - **Model**: text-embedding-3-small
# - **Output**: 1,536-dimensional vectors
# - **Function**: Converts text chunks to dense vector representations
# 
# ### NER Enrichment Node
# - **Type**: `prompter` with `openai_ner` subtype
# - **Function**: Extracts named entities and adds structured metadata
# - **Configuration**: Uses Unstructured's default NER prompt
# 
# All nodes are shared between workflows to ensure processing consistency.
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
    
    # Smart Chunker Node
    chunk_node = WorkflowNode(
        name="Smart_Chunker",
        subtype="chunk_by_title",
        type="chunk",
        settings={
            "new_after_n_chars": 1500,
            "max_characters": 2048,
            "overlap": 0
        }
    )
    
    # Vector Embedder Node
    embedder_node = WorkflowNode(
        name="Vector_Embedder",
        subtype="openai",
        type="embed",
        settings={
            "model_name": "text-embedding-3-small"
        }
    )
    
    # NER Enrichment Node
    ner_enrichment_node = WorkflowNode(
        name="NER_Enrichment",
        subtype="openai_ner",
        type="prompter",
        settings={
            # Use Unstructured's default NER prompt; override later if needed
        }
    )
    
    return vlm_partition_node, chunk_node, embedder_node, ner_enrichment_node

# %% [markdown]
# ## Parallel Workflow Creation
# 
# Creates two independent workflows that process different data sources and write to the same destination.
# 
# ### S3 PDF Workflow
# - **Source**: S3 source connector (PDF documents)
# - **Processing**: VLM partition → Chunk → Embed → NER
# - **Destination**: customer-support Elasticsearch index
# - **Naming**: `S3-PDFs-Parallel-Workflow_{timestamp}`
# 
# ### Elasticsearch Sales Workflow
# - **Source**: Elasticsearch source connector (sales records)
# - **Processing**: Same four processing nodes as PDF workflow
# - **Destination**: Same customer-support Elasticsearch index
# - **Naming**: `Elasticsearch-Sales-Parallel-Workflow_{timestamp}`
# 
# ### Workflow Configuration
# - **Type**: `CUSTOM` workflow type for custom node configuration
# - **Node Sequence**: All workflows use identical node ordering
# - **Parallel Execution**: Workflows run independently and can be executed simultaneously
# 
# The function returns workflow IDs for both created workflows.
# %%

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
            print(f":white_check_mark: Created S3 PDF workflow: {s3_workflow_id}")
        
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
        print(f":white_check_mark: Created Elasticsearch sales workflow: {es_workflow_id}")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f":x: Error creating parallel workflows: {e}")
        return None, None

# %% [markdown]
# ## Workflow Execution
# 
# Initiates workflow execution and returns job tracking information.
# 
# ### Execution Process
# 1. Sends workflow run request to Unstructured API using workflow ID
# 2. Receives job ID for the asynchronous processing task
# 3. Workflow executes on Unstructured's cloud infrastructure
# 4. Returns job ID for status monitoring
# 
# ### Job Management
# - Each workflow execution creates a separate job
# - Jobs run asynchronously and can be monitored independently
# - Job IDs are used for status polling and result verification
# 
# The main pipeline calls this function twice (once per workflow) to start parallel processing.
# %%

def run_workflow(workflow_id, workflow_name):
    """Run a workflow and return job information."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.workflows.run_workflow(
                request={"workflow_id": workflow_id}
            )
        
        job_id = response.job_information.id
        print(f":white_check_mark: Started {workflow_name} job: {job_id}")
        return job_id
        
    except Exception as e:
        print(f":x: Error running {workflow_name} workflow: {e}")
        return None

# %% [markdown]
# ## Job Status Monitoring
# 
# Provides synchronous job monitoring by polling status until completion.
# 
# ### Polling Mechanism
# - **Interval**: Configurable wait time (default: 30 seconds)
# - **States**: Handles SCHEDULED, IN_PROGRESS, COMPLETED, FAILED states
# - **Blocking**: Continuously polls until job reaches terminal state
# 
# ### Job Status Flow
# 1. **SCHEDULED**: Job queued, waiting for processing resources
# 2. **IN_PROGRESS**: Job actively processing data
# 3. **COMPLETED**: Job finished successfully
# 4. **FAILED**: Job encountered an error and stopped
# 
# ### Implementation Notes
# The function blocks execution until job completion, which is used in this pipeline 
# to ensure both jobs finish before proceeding to result verification.
# %%

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
                print(f"{job_name} job completed successfully!")
                return job
            elif status == "FAILED":
                print(f"{job_name} job failed!")
                return job
            else:
                print(f"❓ Unknown {job_name} job status: {status}")
                return job
                
        except Exception as e:
            print(f":x: Error polling {job_name} job status: {e}")
            time.sleep(wait_time)

# %% [markdown]
# ## Elasticsearch Index Management
# 
# Validates source data availability and prepares the destination index.
# 
# ### Source Index Validation
# Checks the `sales-records-consolidated` index:
# - Verifies index exists in Elasticsearch cluster
# - Confirms index contains documents (non-zero count)
# - Exits with error if index is missing or empty
# 
# ### Destination Index Preparation
# Manages the `customer-support` index:
# - Deletes existing index if present (clean slate approach)
# - Creates fresh index with standardized mapping
# - Configures field types: keyword, date, text with standard analyzer, object
# 
# ### Index Mapping Configuration
# ```json
# {
#   "properties": {
#     "id": {"type": "keyword"},
#     "timestamp": {"type": "date"},
#     "text": {"type": "text", "analyzer": "standard"},
#     "metadata": {"type": "object"}
#   }
# }
# ```
# 
# This preprocessing ensures data source availability and destination readiness before workflow execution.
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
        print(f"🔍 Checking {sales_index} index...")
        
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
# Displays comprehensive information about created resources and job status.
# 
# ### Resource Information
# - **Data Sources**: S3 bucket path and Elasticsearch source index
# - **Destination**: Elasticsearch destination index (customer-support)
# - **Connectors**: Source and destination connector IDs
# - **Workflows**: Workflow IDs for both parallel workflows
# - **Jobs**: Job IDs and execution status
# 
# ### Status Indicators
# - Shows "SKIPPED" for S3 components if S3 source connector creation failed
# - Displays actual resource IDs when creation succeeded
# - Provides endpoint information for verification and monitoring
# 
# This summary enables tracking of all pipeline components and their current state.
# %%

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

# %% [markdown]
# ## Result Verification
# 
# Analyzes processed results in the customer-support index to confirm successful data integration.
# 
# ### Document Analysis
# - **Count Verification**: Reports total number of processed documents
# - **Source Detection**: Identifies documents from different data sources using metadata
# - **Data Source Mapping**: Categorizes documents by origin (S3 files, Elasticsearch records)
# - **Random Sampling**: Uses Elasticsearch `function_score` with `random_score` to sample 50 documents for analysis
# 
# ### Source Identification Logic
# Uses metadata fields to determine document origins:
# - `data_source-url`: Direct source URL reference
# - `data_source-record_locator-index_name`: Elasticsearch source index
# - `filename`: File-based sources (S3 documents)
# - `filetype`: Document type indicators
# 
# ### Search Functionality Testing
# Performs test searches across the unified index:
# - Tests common search terms: "manual", "customer", "product", "support"
# - Verifies text search functionality across processed content
# - Reports match counts for each test query
# 
# ### Output Format
# - Pretty-prints sample documents from each identified data source
# - Shows metadata structure and content previews
# - Confirms index readiness for hybrid RAG applications
# 
# This verification step ensures both workflows successfully processed and integrated their respective data sources.
# %%

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

    except Exception as e:
        print(f"❌ Error verifying results: {e}")
        print("💡 This is normal if workflows are still processing or if there is a connection issue.")

# %% [markdown]
# ## Main Pipeline Orchestration
# 
# Coordinates the complete hybrid RAG pipeline execution across six sequential steps.
# 
# ### Step 0: Data Source Preparation
# - Downloads source data from GitHub repository zip files
# - Sets up Elasticsearch index with sales records
# - Creates and populates S3 bucket with PDF documents
# - Exits if data preparation fails
# 
# ### Step 1: Elasticsearch Preprocessing
# - Validates source data availability (sales-records-consolidated index)
# - Prepares destination index (customer-support)
# - Exits if source data is unavailable
# 
# ### Step 2: Source Connector Creation
# - Creates S3 source connector for PDF documents
# - Creates Elasticsearch source connector for sales records
# - Exits if either connector creation fails
# 
# ### Step 3: Destination Connector Creation
# - Creates unified Elasticsearch destination connector
# - Configures customer-support index as target
# - Exits if destination connector creation fails
# 
# ### Step 4: Workflow Creation
# - Creates parallel workflows with identical processing nodes
# - Configures both workflows to write to same destination
# - Exits if Elasticsearch workflow creation fails (S3 workflow is optional)
# 
# ### Step 5: Workflow Execution
# - Starts both workflows for parallel processing
# - Returns job IDs for monitoring
# - Exits if job initiation fails
# 
# ### Step 6: Summary Display
# - Shows all created resource IDs and job information
# - Provides status overview for monitoring and debugging
# 
# The function returns job IDs for both workflows, enabling subsequent monitoring and verification.
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
# ## Pipeline Execution Flow
# 
# The script executes the complete pipeline and monitors job completion.
# 
# ### Execution Sequence
# 1. **Pipeline Initialization**: Calls `main()` to execute all setup and workflow creation steps
# 2. **Job Monitoring**: Polls both job statuses until completion using `poll_job_status()`
# 3. **Result Verification**: Analyzes processed results in the destination index
# 
# ### Job Monitoring Strategy
# - Monitors Elasticsearch job first, then S3 job
# - Blocks execution until both jobs reach terminal state (COMPLETED or FAILED)
# - Provides real-time status updates during processing
# 
# ### Final Verification
# - Calls `verify_customer_support_results()` after job completion
# - Analyzes document count, source distribution, and search functionality
# - Confirms successful data integration from both sources
# 
# This execution flow ensures complete pipeline execution with verification of results.
# %%

# Run the pipeline
s3_job_id, es_job_id = main()

# Poll both jobs to make sure they have completed before proceeding
es_job_info = poll_job_status(es_job_id, "Elasticsearch Ingest")
s3_job_info = poll_job_status(s3_job_id, "S3 Ingest")

# Verify the results (run this after workflows have completed)
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results()
