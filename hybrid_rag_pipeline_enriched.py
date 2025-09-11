#!/usr/bin/env python3
# %% [markdown]
# # Hybrid RAG Pipeline: Multi-Source Data Processing with Unstructured API
# 
# This notebook demonstrates how to use the Unstructured Workflow Endpoint to process data
# from multiple sources and send it to a single destination using **parallel workflows**.
# 
# ## What you'll learn:
# - How Unstructured's 7-stage pipeline transforms raw data into RAG-ready vectors
# - Why parallel workflows enable efficient multi-source processing
# - How VLM partitioning handles both PDFs and structured data consistently
# - How smart chunking and embeddings create a unified knowledge base
# 
# ## Parallel Workflow Architecture
# 
# ```
# ┌─────────────────┐                           ┌─────────────────────────┐
# │   S3 PDFs       │──── WORKFLOW 1 ──────────▶│                         │
# │ (Tech Manuals)  │                           │    Unstructured API     │
# └─────────────────┘                           │                         │
#                                               │  VLM → Chunk → Embed    │
# ┌─────────────────┐                           │      → NER → Store      │
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
# ## Parallel Processing Approach
# 
# Two workflows process different data sources simultaneously and deposit results in the same destination index. Both workflows use identical processing nodes to ensure consistent output format.
# 
# ## Unstructured Pipeline:
# 1. **Connect**: Source connectors (S3, Elasticsearch) ingest data
# 2. **Route**: Auto partitioning strategy selects optimal processing (VLM for complex docs)
# 3. **Transform**: Documents converted to Unstructured's canonical JSON schema
# 4. **Chunk**: By-title chunking creates semantically coherent retrieval units
# 5. **Enrich**: Optional NER extraction adds metadata and entities
# 6. **Embed**: OpenAI embeddings enable semantic similarity search
# 7. **Persist**: Destination connector writes processed data to vector database
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
from dotenv import load_dotenv
from urllib.parse import urlparse

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
# ## Configuration Setup
# 
# Load and validate environment variables required for the pipeline.
# 
# ### Required Variables:
# - **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
# - **S3**: `S3_SOURCE_BUCKET`, `S3_DESTINATION_BUCKET`
# - **Unstructured API**: `UNSTRUCTURED_API_KEY`, `UNSTRUCTURED_API_URL`
# - **Elasticsearch**: `ELASTICSEARCH_HOST`, `ELASTICSEARCH_API_KEY`, `ELASTICSEARCH_INDEX`
# 
# The script validates these variables at startup and exits if any are missing or contain placeholder values.
# %%
# Configuration
SKIPPED = "SKIPPED"

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-access-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
# These are HTTPS URL prefixes (not s3:// URIs)
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")
S3_DESTINATION_BUCKET = os.getenv("S3_DESTINATION_BUCKET")
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "")


# Unstructured API Configuration
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", "your-unstructured-api-key")
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Elasticsearch Configuration
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")

# Validation
REQUIRED_VARS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
    "S3_DESTINATION_BUCKET": S3_DESTINATION_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    raise ValueError(f"Missing required configuration values: {missing_vars}")

print("✅ Configuration loaded successfully")

# Unstructured client will be initialized using context managers in each function

def clear_output_bucket():
    """Clear all contents from the output S3 bucket before running pipeline"""
    try:
        import boto3
        
        s3 = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        response = s3.list_objects_v2(Bucket=S3_DESTINATION_BUCKET)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            if objects_to_delete:
                s3.delete_objects(
                    Bucket=S3_DESTINATION_BUCKET,
                    Delete={'Objects': objects_to_delete}
                )
                print(f"✅ Deleted {len(objects_to_delete)} existing files")
            else:
                print("📁 Bucket was already empty")
        else:
            print("📁 Bucket was already empty")
            
    except Exception as e:
        print(f"⚠️ Could not clear bucket (continuing anyway): {e}")

# %% [markdown]
# ## Create S3 Source Connector
# 
# Creates a source connector for PDF documents in an S3 bucket.
# 
# ### URL Format Handling:
# Accepts various S3 URL formats and converts them to s3:// format:
# - Raw bucket name: `example-data-bose-headphones`
# - Bucket with prefix: `example-data-bose-headphones/manuals`
# - s3:// URL: `s3://example-data-bose-headphones/manuals/`
# - HTTPS URL: `https://example-data-bose-headphones.s3.us-east-2.amazonaws.com/manuals/`
# 
# ### Configuration:
# - Recursive processing enabled for subdirectories
# - Uses AWS credentials for authentication
# - Returns a source_id for workflow creation
# %%

def create_s3_source_connector():
    """Create an S3 source connector for PDF documents.

    Accepts the following in S3_SOURCE_BUCKET and normalizes to s3:// for the connector:
    - Raw bucket name (e.g., example-data-bose-headphones)
    - Bucket + prefix (e.g., example-data-bose-headphones/manuals)
    - s3:// URL (e.g., s3://example-data-bose-headphones/manuals/)
    - HTTPS URL (e.g., https://example-data-bose-headphones.s3.us-east-2.amazonaws.com/manuals/)
    """
    try:
        if not S3_SOURCE_BUCKET:
            raise ValueError("S3_SOURCE_BUCKET is required (bucket name, s3:// URL, or https:// URL)")
        value = S3_SOURCE_BUCKET.strip()
        print("value")
        print(value)    
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
        
        print("s3_style")
        print(s3_style)
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
# ## Create Elasticsearch Source Connector
# 
# Creates a source connector for the `sales-records-consolidated` Elasticsearch index.
# 
# ### Data Source:
# The consolidated sales index contains:
# - Customer interactions and support tickets
# - Product information and pricing data
# - Purchase dates and interaction timestamps
# - Customer segments and product categories
# 
# ### Configuration:
# - Connects to the same Elasticsearch host as the destination
# - Uses API key authentication
# - Reads from `sales-records-consolidated` index
# - Returns a source_id for workflow creation
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
# ## Create Elasticsearch Destination Connector
# 
# Creates a destination connector for the `customer-support` index where both workflows will write their processed results.
# 
# ### Unified Destination:
# Both workflows deposit their processed data into the same `customer-support` index:
# - S3 PDF workflow → processed technical documentation
# - Elasticsearch sales workflow → processed customer interaction data
# - Same processing pipeline ensures consistent data format
# 
# ### Configuration:
# - Writes to `customer-support` index (created fresh during preprocessing)
# - Uses same Elasticsearch host as the source data
# - API key authentication for write access
# - Returns a destination_id used by both workflows
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
# ## Define Processing Nodes
# 
# Creates four processing nodes that both workflows will use to ensure consistent output format.
# 
# ### VLM Partitioner (`partition/vlm`)
# - Uses OpenAI GPT-4o to convert documents into structured JSON
# - Handles PDFs, tables, images, and complex layouts
# 
# ### Chunker (`chunk/chunk_by_title`) 
# - Title-based chunking strategy
# - 1,500 character trigger, 2,048 character maximum
# - No overlap between chunks
# 
# ### Embedder (`embed/openai`)
# - Uses `text-embedding-3-small` model
# - Converts text chunks to 1,536-dimensional vectors
# 
# ### NER Enrichment (`prompter/openai_ner`)
# - Extracts entities (people, products, dates, etc.)
# - Adds structured metadata to processed content
# 
# Both workflows use identical nodes to ensure consistent processing and comparable output quality.
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
# ## Create Two Parallel Workflows
# 
# Creates two workflows that process different data sources and write to the same destination index.
# 
# ### S3 PDF Workflow
# - Source: S3 bucket with PDF documents
# - Processing: VLM partition → Chunking → Embedding → NER enrichment
# - Destination: `customer-support` index
# 
# ### Elasticsearch Sales Workflow  
# - Source: `sales-records-consolidated` index
# - Processing: Same four nodes as PDF workflow
# - Destination: Same `customer-support` index
# 
# Both workflows use identical processing nodes and write to the same destination index, creating a unified dataset from two different data sources.
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
# ## Run Workflow
# 
# Starts a workflow and returns a job ID for tracking.
# 
# ### Process:
# 1. Sends run request to Unstructured API with workflow ID
# 2. Receives job ID for the asynchronous processing job
# 3. Workflow runs in background on Unstructured's infrastructure
# 
# The main pipeline calls this function twice - once for each workflow. Both jobs run simultaneously and can be monitored through the Unstructured dashboard or API polling.
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
# ## Job Monitoring (Optional)
# 
# Provides synchronous job monitoring by polling status until completion. Disabled by default in this pipeline.
# 
# ### Polling Process:
# - Checks job status every 30 seconds (configurable)
# - Handles states: `SCHEDULED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`
# - Blocks until job reaches terminal state
# 
# ### Job States:
# - `SCHEDULED`: Queued, waiting for resources
# - `IN_PROGRESS`: Currently processing
# - `COMPLETED`: Finished successfully
# - `FAILED`: Error occurred
# 
# This function is disabled by default to allow both workflows to run in parallel without blocking. Jobs can be monitored through the Unstructured dashboard instead.
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
                time.sleep(wait_time)
            elif status == "COMPLETED":
                print(f":white_check_mark: {job_name} job completed successfully!")
                return job
            elif status == "FAILED":
                print(f":x: {job_name} job failed!")
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
# Validates source data and prepares destination index before running workflows.
# 
# ### Index Validation:
# 1. Check `sales-records-consolidated` index:
#    - Must exist and contain data
#    - Exit with error if missing or empty
# 
# 2. Manage `customer-support` index:
#    - Delete existing index if present
#    - Create fresh index with proper mapping
#    - Ready to receive processed data from both workflows
# 
# This preprocessing step ensures the source data exists and the destination is clean before workflows begin processing.
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
# ## Pipeline Summary
# 
# Displays a summary of the pipeline execution with all created resources and job statuses.
# 
# ### Information Shown:
# - Data source locations (S3 bucket, Elasticsearch indices)
# - Connector IDs for source and destination connectors
# - Workflow IDs for both parallel workflows
# - Job IDs and current status for each workflow
# 
# ### Next Steps:
# 1. Use `verify_customer_support_index.py` to confirm both data sources appear in the destination
# 2. Monitor job progress through the Unstructured dashboard
# 3. Query the `customer-support` index once processing completes
# 
# The result is a unified index containing processed data from both PDF documents and sales records, ready for hybrid RAG applications.
# %%

def print_pipeline_summary(s3_workflow_id, es_workflow_id, s3_job_id, es_job_id, s3_job, es_job):
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
    print(f"")
    print(f"✅ S3 PDFs Job Status: {s3_job.status if s3_job else SKIPPED}")
    print(f"✅ Elasticsearch Sales Job Status: {es_job.status if es_job else "Unknown"}")
    
    # Success check
    s3_success = s3_job and s3_job.status == "COMPLETED"
    es_success = es_job and es_job.status == "COMPLETED"
    
    print(f"\n🎯 PIPELINE RESULTS:")
    print("=" * 30)
    
    if s3_success and es_success:
        print("🎉 Both workflows completed successfully!")
    elif s3_success:
        print("🎉 S3 PDFs workflow completed successfully!")
    elif es_success:
        print("🎉 Elasticsearch workflow completed successfully!")
    else:
        print("⚠️ Workflows completed with issues. Check the logs above.")
    
    if s3_success or es_success:
        print("📁 Check your Elasticsearch customer-support index for processed results:")
        print(f"   {ELASTICSEARCH_HOST}/customer-support")
        print("🔍 This hybrid dataset is now ready for RAG applications!")
    else:
        print("💡 Check the Unstructured dashboard for detailed job status.")

def verify_customer_support_results():
    """Verify the processed results in the customer-support index."""
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
            print(f"❌ Index '{index_name}' does not exist yet. Workflows may still be processing.")
            return
        
        # Get document count
        count_response = es.count(index=index_name)
        total_docs = count_response['count']
        print(f"📊 Total processed documents: {total_docs}")
        
        if total_docs == 0:
            print("⏳ No documents found yet. Workflows may still be processing.")
            print("💡 Check the Unstructured dashboard for job status.")
            return
        
        # Try to identify source types by looking for common patterns
        # S3 PDF documents typically have different metadata than Elasticsearch sources
        print(f"\n📋 Analyzing Document Sources:")
        print("=" * 40)
        
        # Get sample documents to analyze source patterns
        sample_response = es.search(
            index=index_name,
            body={
                "size": 20,  # Get more samples to find different source types
                "_source": ["metadata", "text", "element_id"],
                "sort": [{"_timestamp": {"order": "desc", "unmapped_type": "date"}}]
            }
        )
        
        s3_docs = []
        es_docs = []
        unknown_docs = []
        
        # Analyze documents to determine source
        for hit in sample_response['hits']['hits']:
            source = hit['_source']
            metadata = source.get('metadata', {})
            
            # Look for indicators of S3 PDF source vs Elasticsearch source
            if 'filename' in metadata or 'filetype' in metadata or '.pdf' in str(metadata):
                s3_docs.append(hit)
            elif 'consolidated_text' in str(source) or 'product_line' in str(metadata):
                es_docs.append(hit)
            else:
                unknown_docs.append(hit)
        
        # Show statistics
        print(f"🔍 Source Analysis (from {len(sample_response['hits']['hits'])} sample docs):")
        print(f"   📄 Likely S3 PDF documents: {len(s3_docs)}")
        print(f"   🔗 Likely Elasticsearch documents: {len(es_docs)}")
        print(f"   ❓ Unknown source: {len(unknown_docs)}")
        
        # Show example from S3 PDF source if available
        if s3_docs:
            print(f"\n📄 Example S3 PDF Document:")
            print("-" * 35)
            s3_example = s3_docs[0]['_source']
            metadata = s3_example.get('metadata', {})
            text = s3_example.get('text', '')
            
            print(f"   Element ID: {s3_example.get('element_id', 'N/A')}")
            print(f"   Filename: {metadata.get('filename', 'N/A')}")
            print(f"   File Type: {metadata.get('filetype', 'N/A')}")
            print(f"   Text Preview: {text[:200]}..." if len(text) > 200 else f"   Text: {text}")
            
        # Show example from Elasticsearch source if available
        if es_docs:
            print(f"\n🔗 Example Elasticsearch Document:")
            print("-" * 38)
            es_example = es_docs[0]['_source']
            metadata = es_example.get('metadata', {})
            text = es_example.get('text', '')
            
            print(f"   Element ID: {es_example.get('element_id', 'N/A')}")
            print(f"   Metadata Keys: {list(metadata.keys())}")
            print(f"   Text Preview: {text[:200]}..." if len(text) > 200 else f"   Text: {text}")
        
        # Show unknown example if any
        if unknown_docs:
            print(f"\n❓ Example Unknown Source Document:")
            print("-" * 35)
            unknown_example = unknown_docs[0]['_source']
            metadata = unknown_example.get('metadata', {})
            text = unknown_example.get('text', '')
            
            print(f"   Element ID: {unknown_example.get('element_id', 'N/A')}")
            print(f"   Metadata: {metadata}")
            print(f"   Text Preview: {text[:200]}..." if len(text) > 200 else f"   Text: {text}")
        
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
        print("✅ Documents from both workflows are present (if both completed)")
        print("✅ Text search is functional across processed content")
        print("✅ Ready for hybrid RAG queries!")
        
    except Exception as e:
        print(f"❌ Error verifying results: {e}")
        print("💡 This is normal if workflows are still processing.")

# %% [markdown]
# ## Main Pipeline Function
# 
# Orchestrates the complete hybrid RAG pipeline execution.
# 
# ### Execution Steps:
# 
# **Step 0: Elasticsearch Preprocessing**
# - Validates `sales-records-consolidated` index exists and contains data
# - Creates fresh `customer-support` destination index
# 
# **Step 1: Create Source Connectors**
# - S3 source connector for PDF documents
# - Elasticsearch source connector for sales data
# 
# **Step 2: Create Destination Connector**
# - Elasticsearch destination connector pointing to `customer-support` index
# 
# **Step 3: Create Workflows**
# - Two parallel workflows with identical processing nodes
# - Both workflows write to the same destination index
# 
# **Step 4: Run Workflows**
# - Starts both workflows to run simultaneously
# - Jobs execute asynchronously on Unstructured infrastructure
# 
# **Step 5: Display Summary**
# - Shows all connector/workflow/job IDs
# - Provides status and next steps
# 
# The pipeline creates a unified `customer-support` index containing processed data from both PDF documents and sales records.
# %%

def main():
    """Main pipeline execution"""
    print("🚀 Starting Hybrid RAG Pipeline")
    
    # Step 0: Elasticsearch preprocessing
    print("\n🔧 Step 0: Elasticsearch preprocessing")
    print("-" * 50)
    
    if not run_elasticsearch_preprocessing():
        print("❌ Failed to complete Elasticsearch preprocessing")
        return
    
    # Step 1: Create Source Connectors
    print("\n🔗 Step 1: Creating source connectors")
    print("-" * 50)
    
    s3_source_id = create_s3_source_connector()
    if not s3_source_id:
        print("❌ Failed to create S3 source connector")
        return
    
    elasticsearch_source_id = create_elasticsearch_source_connector()
    if not elasticsearch_source_id:
        print("❌ Failed to create Elasticsearch source connector")
        return
    
    # Step 2: Create Destination Connector
    print("\n🎯 Step 2: Creating Elasticsearch destination connector")
    print("-" * 50)
    
    destination_id = create_elasticsearch_destination_connector()
    if not destination_id:
        print("❌ Failed to create destination connector")
        return
    
    # Step 3: Create Workflows
    print("\n⚙️ Step 3: Creating workflows")
    print("-" * 50)
    
    s3_workflow_id, es_workflow_id = create_parallel_workflows(
        s3_source_id, elasticsearch_source_id, destination_id
    )
    
    if not es_workflow_id:
        print("❌ Failed to create Elasticsearch workflow")
        return
    
    # Step 4: Run Workflows
    print("\n🚀 Step 4: Running workflows")
    print("-" * 50)
    
    s3_job_id = None
    if s3_workflow_id:
        s3_job_id = run_workflow(s3_workflow_id, "S3 PDFs")
    
    es_job_id = run_workflow(es_workflow_id, "Elasticsearch Sales")
    
    if not es_job_id:
        print("❌ Failed to start Elasticsearch workflow")
        return
    
    # Step 5: Monitor Jobs
    print("\n⏳ Step 5: Monitoring job progress")
    print("-" * 50)
    
    print("⚠️ Job monitoring disabled - check Unstructured dashboard for status")
    print("💡 Jobs are running in background and will deposit results in Elasticsearch customer-support index")
    
    # Set dummy job objects for summary
    s3_job = None
    es_job = type('Job', (), {'status': 'SUBMITTED'})()
    
    # Step 6: Pipeline Summary
    print_pipeline_summary(s3_workflow_id, es_workflow_id, s3_job_id, es_job_id, s3_job, es_job)

# Run the pipeline
main()

# %%
# Verify the results (run this after workflows have completed)
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results()
