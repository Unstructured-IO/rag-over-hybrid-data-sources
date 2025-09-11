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
# ┌─────────────────┐                    ┌──────────────────────────────────────────────────────┐
# │   S3 PDFs       │                    │              UNSTRUCTURED API PROCESSING             │
# │ (Tech Manuals,  │────────────────────┤                                                      │
# │  Safety Docs)   │                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
# └─────────────────┘                    │  │Connect  │  │ Route   │  │Transform│  │  Chunk  │ │
#                                        │  │   ↓     │  │   ↓     │  │   ↓     │  │    ↓    │ │
# ┌─────────────────┐    WORKFLOW 1      │  │ S3 Src  │→ │VLM Auto │→ │Elements │→ │By Title │ │
# │ Elasticsearch   │────────────────────┤  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
# │ (Sales Records) │                    │                                                      │
# └─────────────────┘                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
#                                        │  │Connect  │  │ Route   │  │Transform│              │
#                    WORKFLOW 2          │  │   ↓     │  │   ↓     │  │   ↓     │              │
#                                        │  │ ES Src  │→ │VLM Auto │→ │Elements │──────────────┤
#                                        │  └─────────┘  └─────────┘  └─────────┘              │
#                                        │                                                      │
#                                        │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
#                                        │  │ Enrich  │  │ Embed   │  │ Persist │              │
#                                        │  │   ↓     │  │   ↓     │  │   ↓     │              │
#                                        │  │OpenAI   │→ │OpenAI   │→ │   ES    │              │
#                                        │  │  NER    │  │text-emb │  │customer-│              │
#                                        │  │         │  │ -3-small│  │support  │              │
#                                        │  └─────────┘  └─────────┘  └─────────┘              │
#                                        └──────────────────────────────────────────────────────┘
#                                                                           │
#                                        ┌──────────────────────────────────▼───────────────────┐
#                                        │           UNIFIED KNOWLEDGE BASE                      │
#                                        │         Elasticsearch: customer-support              │
#                                        │                                                       │
#                                        │  • PDF content (manuals, troubleshooting)            │
#                                        │  • Sales data (customer interactions, products)      │
#                                        │  • Consistent chunking & embeddings                  │
#                                        │  • Ready for hybrid RAG queries                      │
#                                        └───────────────────────────────────────────────────────┘
# ```
# 
# ## Why Parallel Workflows?
# 
# **🚀 Efficiency**: Both data sources process simultaneously rather than sequentially
# 
# **🔄 Consistency**: Same processing nodes (VLM → Chunk → Embed → NER) ensure comparable outputs
# 
# **📊 Scalability**: Each workflow can be monitored, scheduled, and scaled independently
# 
# **🎯 Unified Destination**: Both workflows write to the same `customer-support` index for seamless hybrid retrieval
# 
# ## Unstructured's 7-Stage Pipeline:
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
# Configuration and environment variables used by the pipeline.
# 
# - AWS credentials and S3 bucket names (source PDFs, destination optional)
# - Unstructured API key and server URL
# - Elasticsearch host/API key and the working indices
# 
# The script validates critical variables at startup to prevent half-configured runs.
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
# Create an S3 source connector for PDF documents (Bose manuals, troubleshooting, safety docs).
# 
# Why: Product manuals and troubleshooting guides add authoritative reference material to the
# knowledge base and complement the structured sales data.
# 
# Key settings:
# - remote_url: s3://<bucket>/ with recursion enabled
# - AWS credentials and region
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
# Create an Elasticsearch source connector for the consolidated sales data.
# 
# Why: The consolidated index produced during preprocessing contains realistic conversational
# context (customers, products, prices, dates) and is ideal for retrieval-augmented analysis.
# 
# Key settings:
# - hosts: the managed Elasticsearch endpoint
# - es_api_key: API key for auth
# - index_name: the consolidated sales index
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
# Create an Elasticsearch destination connector for the `customer-support` index.
# 
# Why: We send outputs from both workflows here to power hybrid retrieval for support use cases.
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
# Shared workflow nodes used by both sources:
# 
# - VLM partition (GPT-4o): understands PDFs and structured docs, emitting structured elements
# - Smart chunking: title-based chunking (1500–2048 chars, 0 overlap) for retrieval-friendly units
# - Embedding: OpenAI `text-embedding-3-small` for semantic search
# - NER enrichment (prompter/openai_ner): optional entity extraction for analysis/metadata
# 
# Reusing the same stack ensures comparable outputs across sources.
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
# ## Creating Parallel Workflows
# 
# We create **two independent workflows** that process different data sources in parallel:
# 
# ### Workflow 1: S3 PDF Processing
# - **Source**: S3 bucket containing technical manuals, troubleshooting guides, safety documents
# - **Processing**: VLM partition → Smart chunking → OpenAI embeddings → NER enrichment
# - **Destination**: Elasticsearch `customer-support` index
# 
# ### Workflow 2: Elasticsearch Sales Data Processing  
# - **Source**: Elasticsearch `sales-records-consolidated` index with customer interactions
# - **Processing**: Same pipeline (VLM → Chunk → Embed → NER) for consistency
# - **Destination**: Same Elasticsearch `customer-support` index
# 
# ### Key Benefits:
# - **⚡ Parallel Execution**: Both workflows run simultaneously, reducing total processing time
# - **🔄 Consistent Processing**: Same node configuration ensures comparable output quality
# - **📍 Unified Destination**: Single index simplifies downstream RAG queries and validation
# - **🎯 Independent Monitoring**: Each workflow can be tracked, scheduled, and managed separately
# 
# The result is a unified knowledge base where PDF technical documentation and structured sales data 
# are processed with the same quality standards and stored together for hybrid retrieval.
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
# Start a workflow and capture the returned job ID for tracking.
# 
# Why: Jobs are asynchronous; you can poll or monitor in the Unstructured dashboard.
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
# Optional job polling. In this example, polling is disabled by default. Use the dashboard or
# enable the poller if you need to block until completion.
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
# ## Smart Elasticsearch Index Management
# 
# Our preprocessing implements intelligent index validation and management:
# 
# ### Index Validation Logic:
# 1. **Check `sales-records-consolidated`**: 
#    - ✅ Must exist and contain data (our source)
#    - ❌ If missing/empty → Error: "There is no data to use"
# 
# 2. **Manage `customer-support`**:
#    - 🗑️ If exists with data → Delete all records and recreate fresh
#    - 🆕 If doesn't exist → Create new with proper mapping
#    - 🎯 Result: Clean destination ready for processed data
# 
# ### Why This Approach?
# - **🛡️ Data Integrity**: Ensures source data exists before processing
# - **🔄 Clean Slate**: Fresh destination prevents mixing old/new processed data  
# - **⚡ Automated**: No manual index management required
# - **🎯 Fail-Fast**: Catches configuration issues early in the pipeline
# 
# This preprocessing step runs before creating workflows, ensuring a reliable foundation
# for our parallel processing architecture.
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
# Print a concise summary with connector and workflow IDs and job IDs. Use
# `verify_customer_support_index.py` to validate that both S3 and Elasticsearch sources appear in
# the destination index.
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

# %% [markdown]
# Orchestrate the full pipeline: preprocessing → connectors → workflows → runs → summary.
# 
# Tip: If you want blocking runs, enable the job polling function for both jobs.
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

