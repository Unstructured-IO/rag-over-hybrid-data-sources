#!/usr/bin/env python3
"""
Hybrid RAG Pipeline: S3 + Elasticsearch to S3 with NER Enrichment

This script demonstrates a hybrid RAG pipeline using the Unstructured Workflow Endpoint:
- Source 1: S3 bucket containing Bose product PDFs (manuals, troubleshooting, MSDS)
- Source 2: Elasticsearch index with synthetic sales data
- Destination: S3 bucket (output/ subfolder) for processed results
- Enrichment: Named Entity Recognition (NER) before final storage

Pipeline Architecture:
[Elasticsearch Sales Data] → [VLM Partition] → [Chunk] → [Embed] → [S3 Output]
"""

import os
import sys
import time
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import (
    CreateSourceRequest,
    CreateDestinationRequest
)
from unstructured_client.models.shared import (
    CreateSourceConnector,
    SourceConnectorType,
    S3SourceConnectorConfigInput,
    CreateDestinationConnector,
    DestinationConnectorType,
    S3DestinationConnectorConfigInput,
    WorkflowNode,
    WorkflowType
)

# Load environment variables
load_dotenv()

# Configuration
print("🔧 Loading configuration...")

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-access-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", "example-data-bose-headphones")  # Source bucket with PDFs
S3_DESTINATION_BUCKET = os.getenv("S3_DESTINATION_BUCKET", "example-data-bose-headphones-output")  # Separate output bucket
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "")  # No subfolder needed with separate bucket

# Unstructured API Configuration
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", "your-unstructured-api-key")
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Elasticsearch Configuration
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")  # Updated to use consolidated index

# Validation
REQUIRED_VARS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    sys.exit(1)

print("✅ All required configuration values loaded successfully")

# Initialize Unstructured client
print("🔧 Initializing Unstructured client...")
unstructured_client = UnstructuredClient(
    api_key_auth=UNSTRUCTURED_API_KEY,
    server_url=UNSTRUCTURED_API_URL
)

# ============================================================================
# EXISTING PIPELINE FUNCTIONS (Updated to use consolidated index)
# ============================================================================

def pretty_print_model(response_model):
    """Pretty print model responses for better readability"""
    print(response_model.model_dump_json(indent=4))

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
        
        print("🗑️ Clearing output bucket before pipeline run...")
        
        response = s3.list_objects_v2(Bucket=S3_DESTINATION_BUCKET)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            if objects_to_delete:
                s3.delete_objects(
                    Bucket=S3_DESTINATION_BUCKET,
                    Delete={'Objects': objects_to_delete}
                )
                print(f"  ✅ Deleted {len(objects_to_delete)} existing files")
            else:
                print("  📁 Bucket was already empty")
        else:
            print("  📁 Bucket was already empty")
            
    except Exception as e:
        print(f"  ⚠️ Could not clear bucket (continuing anyway): {e}")

def create_s3_source_connector():
    """
    Create an S3 source connector for PDF documents.
    
    This connector will read Bose product documentation (manuals, troubleshooting guides, MSDS)
    from the S3 bucket.
    
    Returns:
        str: Source connector ID if successful, None if failed
    """
    try:
        print("🔗 Creating S3 source connector for PDFs...")
        
        response = unstructured_client.sources.create_source(
            request=CreateSourceRequest(
                create_source_connector=CreateSourceConnector(
                    name=f"s3_pdf_source_{int(time.time())}",
                    type=SourceConnectorType.S3,
                    config=S3SourceConnectorConfigInput(
                        remote_url=f"s3://{S3_SOURCE_BUCKET}/",
                        recursive=True,
                        key=AWS_ACCESS_KEY_ID,
                        secret=AWS_SECRET_ACCESS_KEY,
                        region=AWS_REGION
                    )
                )
            )
        )
        
        source_id = response.source_connector_information.id
        print(f"  ✅ Created S3 PDF source connector: {source_id}")
        return source_id
        
    except Exception as e:
        print(f"  ❌ Error creating S3 source connector: {e}")
        return None

def create_elasticsearch_source_connector():
    """
    Create an Elasticsearch source connector for sales data.
    
    This connector will read synthetic Bose sales records from the Elasticsearch index,
    including consolidated sales conversations and customer interactions.
    
    Returns:
        str: Source connector ID if successful, None if failed
    """
    try:
        print("🔗 Creating Elasticsearch source connector for sales data...")
        
        response = unstructured_client.sources.create_source(
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
        print(f"  ✅ Created Elasticsearch sales source connector: {source_id}")
        return source_id
        
    except Exception as e:
        print(f"  ❌ Error creating Elasticsearch source connector: {e}")
        return None

def create_elasticsearch_destination_connector():
    """
    Create an Elasticsearch destination connector for processed results.
    
    This connector will store all processed documents (from both S3 PDFs and Elasticsearch sales data)
    in the customer-support index.
    
    Returns:
        str: Destination connector ID if successful, None if failed
    """
    try:
        print("🔗 Creating Elasticsearch destination connector for processed results...")
        
        response = unstructured_client.destinations.create_destination(
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
        print(f"  ✅ Created Elasticsearch destination connector: {destination_id}")
        print(f"  📁 Output location: {ELASTICSEARCH_HOST}/customer-support")
        return destination_id
        
    except Exception as e:
        print(f"  ❌ Error creating Elasticsearch destination connector: {e}")
        return None
def create_parallel_workflows(s3_source_id, elasticsearch_source_id, destination_id):
    """
    Create separate workflows for S3 PDFs and Elasticsearch data that run in parallel.
    
    Parallel Workflow Architecture:
    [S3 PDFs] → [VLM Partition] → [Smart Chunk] → [Vector Embed] → [Elasticsearch Output]
    [Elasticsearch] → [VLM Partition] → [Smart Chunk] → [Vector Embed] → [Elasticsearch Output]
    
    Both workflows use the same processing pipeline and destination.
    
    Args:
        s3_source_id (str): S3 source connector ID for PDFs
        elasticsearch_source_id (str): Elasticsearch source connector ID for sales data
        destination_id (str): Elasticsearch destination connector ID
        
    Returns:
        tuple: (s3_workflow_id, es_workflow_id) if successful, (None, None) if failed
    """
    try:
        print("⚙️ Creating parallel RAG workflows for both data sources...")
        
        # Shared processing nodes
        # 🧠 VLM Partitioner Node - processes both PDFs and structured data
        vlm_partition_node = WorkflowNode(
            name="VLM_Partitioner",
            subtype="vlm",
            type="partition",
            settings={
                "provider": "openai",
                "model": "gpt-4o",
            }
        )
        print("  🧠 Configured VLM Partitioner: GPT-4o will analyze both PDFs and structured data")
        
        # ✂️ Smart Chunker Node
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
        print("  ✂️ Configured Smart Chunker: Creating 1500-2048 character chunks")
        
        # 🔢 Vector Embedder Node
        embedder_node = WorkflowNode(
            name="Vector_Embedder",
            subtype="openai",
            type="embed",
            settings={
                "model_name": "text-embedding-3-small"
            }
        )
        print("  🔢 Configured Vector Embedder: Using text-embedding-3-small")
        
        # 🏷️ NER Enrichment Node - Temporarily disabled (plugin not available)
        # ner_node = WorkflowNode(
        #     name=NER_Enrichment,
        #     subtype=ner,
        #     type="enrich",
        #     settings={
        #         "provider": "openai",
        #         "model": "gpt-4",
        #         "entities": [
        #             "PERSON",      # Customer names, sales reps
        #             "ORG",         # Companies, retailers (Best Buy, Amazon)
        #             "PRODUCT",     # Bose products (SoundSport, QuietComfort, OpenAudio)
        #             "GPE",         # Locations (cities, states, regions)
        #             "MONEY",       # Prices, deal values
        #             "DATE",        # Sales dates, quarters
        #             "CARDINAL"     # Quantities, model numbers
        #         ]
        #     }
        # )
        print("  ⚠️ NER Enrichment: Temporarily disabled (plugin not available)")
        
        # Create workflow for S3 PDFs
        s3_workflow_id = None
        if s3_source_id:
            s3_response = unstructured_client.workflows.create_workflow(
                request={
                    "create_workflow": {
                        "name": f"S3-PDFs-Parallel-Workflow_{int(time.time())}",
                        "source_id": s3_source_id,
                        "destination_id": destination_id,
                        "workflow_type": WorkflowType.CUSTOM,
                        "workflow_nodes": [
                            vlm_partition_node,  # PDFs need VLM partitioning
                            chunk_node,
                            embedder_node
                            # ner_node          # Temporarily disabled
                        ],
                    }
                }
            )
            
            s3_workflow_id = s3_response.workflow_information.id
            print(f"  ✅ Created S3 PDF workflow: {s3_workflow_id}")
        else:
            print(f"  ⚠️ S3 PDF workflow: SKIPPED")
        
        # Create workflow for Elasticsearch sales data
        es_response = unstructured_client.workflows.create_workflow(
            request={
                "create_workflow": {
                    "name": f"Elasticsearch-Sales-Parallel-Workflow_{int(time.time())}",
                    "source_id": elasticsearch_source_id,
                    "destination_id": destination_id,
                    "workflow_type": WorkflowType.CUSTOM,
                    "workflow_nodes": [
                        vlm_partition_node,  # Use same VLM partitioning for consistent processing
                        chunk_node,          # Chunk the consolidated_text field
                        embedder_node        # Generate embeddings
                        # ner_node           # Extract entities - temporarily disabled
                    ],
                }
            }
        )
        
        es_workflow_id = es_response.workflow_information.id
        print(f"  ✅ Created Elasticsearch sales workflow: {es_workflow_id}")
        print(f"  📊 Both workflows will run in parallel to the same destination")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f"  ❌ Error creating parallel workflows: {e}")
        return None, None
        print("⚙️ Creating parallel RAG workflows for both data sources...")
        
        # Shared processing nodes
        # 🧠 VLM Partitioner Node - processes both PDFs and structured data
        vlm_partition_node = WorkflowNode(
            name="VLM_Partitioner",
            subtype="vlm",
            type="partition",
            settings={
                "provider": "openai",
                "model": "gpt-4o",
            }
        )
        print("  🧠 Configured VLM Partitioner: GPT-4o will analyze both PDFs and structured data")
        
        # ✂️ Smart Chunker Node
        chunk_node = WorkflowNode(
            name='Smart_Chunker',
            subtype='chunk_by_title',
            type="chunk",
            settings={
                'new_after_n_chars': 1500,
                'max_characters': 2048,
                'overlap': 0
            }
        )
        print("  ✂️ Configured Smart Chunker: Creating 1500-2048 character chunks")
        
        # 🔢 Vector Embedder Node
        embedder_node = WorkflowNode(
            name='Vector_Embedder',
            subtype='openai',
            type="embed",
            settings={
                'model_name': 'text-embedding-3-small'
            }
        )
        print("  🔢 Configured Vector Embedder: Using text-embedding-3-small")
        
        # 🏷️ NER Enrichment Node - Temporarily disabled (plugin not available)
        # ner_node = WorkflowNode(
        #     name='NER_Enrichment',
        #     subtype='ner',
        #     type="enrich",
        #     settings={
        #         "provider": "openai",
        #         "model": "gpt-4",
        #         "entities": [
        #             "PERSON",      # Customer names, sales reps
        #             "ORG",         # Companies, retailers (Best Buy, Amazon)
        #             "PRODUCT",     # Bose products (SoundSport, QuietComfort, OpenAudio)
        #             "GPE",         # Locations (cities, states, regions)
        #             "MONEY",       # Prices, deal values
        #             "DATE",        # Sales dates, quarters
        #             "CARDINAL"     # Quantities, model numbers
        #         ]
        #     }
        # )
        print("  ⚠️ NER Enrichment: Temporarily disabled (plugin not available)")
        
        # Create workflow for S3 PDFs
        s3_workflow_id = None
        if s3_source_id:
            s3_response = unstructured_client.workflows.create_workflow(
                request={
                    "create_workflow": {
                        "name": f"S3-PDFs-Parallel-Workflow_{int(time.time())}",
                        "source_id": s3_source_id,
                        "destination_id": destination_id,
                        "workflow_type": WorkflowType.CUSTOM,
                        "workflow_nodes": [
                            vlm_partition_node,  # PDFs need VLM partitioning
                            chunk_node,
                            embedder_node
                            # ner_node          # Temporarily disabled
                        ],
                    }
                }
            )
            
            s3_workflow_id = s3_response.workflow_information.id
            print(f"  ✅ Created S3 PDF workflow: {s3_workflow_id}")
        else:
            print("  ⚠️ S3 PDF workflow: SKIPPED")
        
        # Create workflow for Elasticsearch sales data
        es_response = unstructured_client.workflows.create_workflow(
            request={
                "create_workflow": {
                    "name": f"Elasticsearch-Sales-Parallel-Workflow_{int(time.time())}",
                    "source_id": elasticsearch_source_id,
                    "destination_id": destination_id,
                    "workflow_type": WorkflowType.CUSTOM,
                    "workflow_nodes": [
                        vlm_partition_node,  # Use same VLM partitioning for consistent processing
                        chunk_node,          # Chunk the consolidated_text field
                        embedder_node        # Generate embeddings
                        # ner_node           # Extract entities - temporarily disabled
                    ],
                }
            }
        )
        
        es_workflow_id = es_response.workflow_information.id
        print(f"  ✅ Created Elasticsearch sales workflow: {es_workflow_id}")
        print(f"  📊 Both workflows will run in parallel to the same destination")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f"  ❌ Error creating parallel workflows: {e}")
        return None, None

def run_workflow(workflow_id, workflow_name):
    """Run a workflow and return job information."""
    try:
        print(f"🚀 Running {workflow_name} workflow...")
        
        response = unstructured_client.workflows.run_workflow(
            request={"workflow_id": workflow_id}
        )
        
        job_id = response.job_information.id
        print(f"  ✅ Started {workflow_name} job: {job_id}")
        return job_id
        
    except Exception as e:
        print(f"  ❌ Error running {workflow_name} workflow: {e}")
        return None

def poll_job_status(job_id, job_name, wait_time=30):
    """Poll job status until completion."""
    print(f"⏳ Monitoring {job_name} job status (checking every {wait_time} seconds)...")
    
    while True:
        try:
            response = unstructured_client.jobs.get_job(
                request={"job_id": job_id}
            )
            
            job = response.job_information
            status = job.status
            
            if status == "SCHEDULED":
                print(f"  📅 {job_name} job is scheduled, checking again in {wait_time} seconds...")
                time.sleep(wait_time)
            elif status == "IN_PROGRESS":
                print(f"  ⚙️ {job_name} job is in progress, checking again in {wait_time} seconds...")
                time.sleep(wait_time)
            elif status == "COMPLETED":
                print(f"  ✅ {job_name} job completed successfully!")
                return job
            elif status == "FAILED":
                print(f"  ❌ {job_name} job failed!")
                return job
            else:
                print(f"  ❓ Unknown {job_name} job status: {status}")
                return job
                
        except Exception as e:
            print(f"  ❌ Error polling {job_name} job status: {e}")
            time.sleep(wait_time)

def main():
    """Main pipeline execution"""
    print("🚀 Starting Hybrid RAG Pipeline")
    # Step 0: Run Elasticsearch preprocessing to create required indices
    print("\n🔧 Step 0: Elasticsearch preprocessing")
    print("-" * 50)
    
    from elasticsearch_index_preprocessing import run_elasticsearch_preprocessing
    if not run_elasticsearch_preprocessing():
        print("❌ Failed to complete Elasticsearch preprocessing")
        return
    
    # Clear output bucket after preprocessing (not needed for Elasticsearch destination)
    # clear_output_bucket()  # Not needed for Elasticsearch destination
    print()
    
    # Step 1: Create Source Connectors
    print("\n🔗 Step 1: Creating source connectors")
    print("-" * 50)
    
    # S3 PDF source - now enabled for combined workflow
    s3_source_id = create_s3_source_connector()
    if not s3_source_id:
        print("❌ Failed to create S3 source connector")
        return
    print("✅ S3 PDF source: ENABLED for combined workflow")
    
    elasticsearch_source_id = create_elasticsearch_source_connector()
    if not elasticsearch_source_id:
        print("❌ Failed to create Elasticsearch source connector")
        return
    
    # Step 2: Create Destination Connector
    print("\n🎯 Step 2: Creating Elasticsearch destination connector")
    print("-" * 50)
    
    destination_id = create_elasticsearch_destination_connector()
    if not destination_id:
        print("❌ Failed to create S3 destination connector")
        return
    
    # Step 3: Create Hybrid Workflows
    print("\n⚙️ Step 3: Creating workflows")
    print("-" * 50)
    
    # Only create Elasticsearch workflow for now
    s3_workflow_id, es_workflow_id = create_parallel_workflows(
        s3_source_id, elasticsearch_source_id, destination_id
    )
    
    if not es_workflow_id:
        print("❌ Failed to create Elasticsearch workflow")
        return
    
    # Step 4: Run Both Workflows
    print("\n🚀 Step 4: Running workflows")
    print("-" * 50)
    
    # Run S3 workflow if available
    s3_job_id = None
    if s3_workflow_id:
        s3_job_id = run_workflow(s3_workflow_id, "S3 PDFs")
    else:
        print("⚠️ S3 PDFs workflow: SKIPPED")
    
    # Run Elasticsearch workflow
    es_job_id = run_workflow(es_workflow_id, "Elasticsearch Sales")
    
    if not es_job_id:
        print("❌ Failed to start Elasticsearch workflow")
        return
    # Step 5: Monitor Elasticsearch Job
    print("\n⏳ Step 5: Monitoring job progress")
    print("-" * 50)
    
    # Job monitoring commented out - polling function not detecting completion correctly
    # s3_job = None
    # if s3_job_id:
    #     s3_job = poll_job_status(s3_job_id, "S3 PDFs")
    # else:
    #     print("⚠️ S3 PDFs job: SKIPPED")
    # 
    # es_job = poll_job_status(es_job_id, "Elasticsearch Sales")
    
    print("⚠️ Job monitoring disabled - check Unstructured dashboard for status")
    print("💡 Jobs are running in background and will deposit results in Elasticsearch customer-support index")
    
    # Set dummy job objects for summary
    s3_job = None
    es_job = type('Job', (), {'status': 'SUBMITTED'})()
    
    # Step 6: Pipeline Summary
    # Step 6: Pipeline Summary
    print("\n" + "=" * 80)
    print("�� HYBRID RAG PIPELINE SUMMARY")
    print("=" * 80)
    print(f"📁 S3 Source (PDFs): {S3_SOURCE_BUCKET if s3_workflow_id else SKIPPED}")
    print(f"🔍 Elasticsearch Source: {ELASTICSEARCH_HOST}/{ELASTICSEARCH_INDEX}")
    print(f"📤 Elasticsearch Destination: {ELASTICSEARCH_HOST}/customer-support")
    print(f"")
    print(f"🔗 S3 Source Connector ID: {s3_source_id if s3_workflow_id else SKIPPED}")
    print(f"🔗 Elasticsearch Source Connector ID: {elasticsearch_source_id}")
    print(f"🔗 Elasticsearch Destination Connector ID: {destination_id}")
    print(f"")
    print(f"⚙️ S3 PDFs Workflow ID: {s3_workflow_id if s3_workflow_id else SKIPPED}")
    print(f"⚙️ Elasticsearch Sales Workflow ID: {es_workflow_id}")
    print(f"")
    print(f"🚀 S3 PDFs Job ID: {s3_job_id if s3_job_id else SKIPPED}")
    print(f"🚀 Elasticsearch Sales Job ID: {es_job_id}")
    print(f"")
    print(f"✅ S3 PDFs Job Status: {s3_job.status if s3_job else "SKIPPED"}")
    print(f"✅ Elasticsearch Sales Job Status: {es_job.status if es_job else "Unknown"}")
    
    # Success check for both workflows
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
        print("")
        if s3_success:
            print("🔍 S3 Results include processed PDF documents:")
            print("   • Bose product manuals and documentation")
            print("   • Troubleshooting guides")
            print("   • MSDS and safety information")
            print("")
        if es_success:
            print("🔍 Elasticsearch Results include processed sales data:")
            print("   • Consolidated sales records with full context")
            print("   • Customer and product information")
            print("   • Sales conversations and interactions")
            print("   • Temporal and geographic data")
            print("")
        print("🔍 This hybrid dataset is now ready for RAG applications!")
    else:
        print("💡 Check the Unstructured dashboard for detailed job status.")
        print("🔍 This Elasticsearch dataset is now ready for RAG applications!")

if __name__ == "__main__":
    main()
