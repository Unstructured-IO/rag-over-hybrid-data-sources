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
from pathlib import Path
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
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records")

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

def create_s3_destination_connector():
    """
    Create an S3 destination connector for processed results.
    
    This connector will store all processed documents (from both S3 PDFs and Elasticsearch sales data)
    in the output/ subfolder of the same S3 bucket.
    
    Returns:
        str: Destination connector ID if successful, None if failed
    """
    try:
        print("🔗 Creating S3 destination connector for processed results...")
        
        response = unstructured_client.destinations.create_destination(
            request=CreateDestinationRequest(
                create_destination_connector=CreateDestinationConnector(
                    name=f"s3_output_destination_{int(time.time())}",
                    type="s3",
                    config={
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "remote_url": f"s3://{S3_DESTINATION_BUCKET}/",
                        "endpoint_url": f"https://s3.{AWS_REGION}.amazonaws.com"
                    }
                )
            )
        )
        
        destination_id = response.destination_connector_information.id
        print(f"  ✅ Created S3 destination connector: {destination_id}")
        print(f"  📁 Output location: s3://{S3_DESTINATION_BUCKET}/")
        return destination_id
        
    except Exception as e:
        print(f"  ❌ Error creating S3 destination connector: {e}")
        return None

def create_hybrid_workflow(s3_source_id, elasticsearch_source_id, destination_id):
    """
    Create a hybrid RAG workflow that processes both S3 PDFs and Elasticsearch sales data.
    
    Workflow Architecture:
    [S3 PDFs] → [VLM Partition] → [Smart Chunk] → [Vector Embed] → [NER] → [S3 Output]
    [Elasticsearch Sales] → [Chunk] → [Vector Embed] → [NER] → [S3 Output]
    
    Processing Pipeline:
    - Elasticsearch: VLM Partition → Smart Chunk → Vector Embed (intelligent processing with embeddings)
    
    Args:
        s3_source_id (str): S3 source connector ID for PDFs
        elasticsearch_source_id (str): Elasticsearch source connector ID for sales data
        destination_id (str): S3 destination connector ID
        
    Returns:
        str: Workflow ID if successful, None if failed
    """
    try:
        print("⚙️ Creating hybrid RAG workflow with optimized processing per data type...")
        
        # Shared nodes for both workflows
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
        
        # PDF-specific node (only for S3 PDFs)
        # 🧠 VLM Partitioner Node - only needed for documents, not structured data
        vlm_partition_node = WorkflowNode(
            name="VLM_Partitioner",
            subtype="vlm",
            type="partition",
            settings={
                "provider": "openai",
                "model": "gpt-4o",
            }
        )
        print("  🧠 Configured VLM Partitioner: GPT-4o will analyze PDF document layout and structure")
        
        # Use the same VLM partitioner for both workflows
        # Elasticsearch data will also benefit from intelligent partitioning
        print("  📄 Using VLM Partitioner for both S3 PDFs and Elasticsearch data")
        
        # Create workflow for S3 PDFs (temporarily commented out)
        s3_workflow_id = None
        if s3_source_id:
            s3_response = unstructured_client.workflows.create_workflow(
                request={
                    "create_workflow": {
                        "name": f"S3-PDFs-to-S3-with-NER_{int(time.time())}",
                        "source_id": s3_source_id,
                        "destination_id": destination_id,
                        "workflow_type": WorkflowType.CUSTOM,
                                            "workflow_nodes": [
                        vlm_partition_node,  # PDFs need VLM partitioning
                        chunk_node,
                        embedder_node       # Generate embeddings
                        # ner_node          # Temporarily disabled
                    ],
                    }
                }
            )
            
            s3_workflow_id = s3_response.workflow_information.id
            print(f"  ✅ Created S3 PDF workflow (with partitioning): {s3_workflow_id}")
        else:
            print(f"  ⚠️ S3 PDF workflow: SKIPPED")
        
        # Create workflow for Elasticsearch sales data (no partitioning needed)
        es_response = unstructured_client.workflows.create_workflow(
            request={
                "create_workflow": {
                    "name": f"Elasticsearch-Sales-to-S3-with-NER_{int(time.time())}",
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
        print(f"  ✅ Created Elasticsearch sales workflow (structured data): {es_workflow_id}")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f"  ❌ Error creating hybrid workflow: {e}")
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
    print("🚀 Starting Elasticsearch RAG Pipeline → S3 with Embeddings")
    print("=" * 80)
    
    # Clear output bucket first
    clear_output_bucket()
    print()
    
    # Step 1: Create Source Connectors
    print("\n🔗 Step 1: Creating source connectors")
    print("-" * 50)
    
    # S3 PDF source - commented out for now
    # s3_source_id = create_s3_source_connector()
    # if not s3_source_id:
    #     print("❌ Failed to create S3 source connector")
    #     return
    s3_source_id = None
    print("⚠️ S3 PDF source: SKIPPED (focusing on Elasticsearch only)")
    
    elasticsearch_source_id = create_elasticsearch_source_connector()
    if not elasticsearch_source_id:
        print("❌ Failed to create Elasticsearch source connector")
        return
    
    # Step 2: Create Destination Connector
    print("\n🎯 Step 2: Creating destination connector")
    print("-" * 50)
    
    destination_id = create_s3_destination_connector()
    if not destination_id:
        print("❌ Failed to create S3 destination connector")
        return
    
    # Step 3: Create Hybrid Workflows
    print("\n⚙️ Step 3: Creating workflows")
    print("-" * 50)
    
    # Only create Elasticsearch workflow for now
    s3_workflow_id, es_workflow_id = create_hybrid_workflow(
        s3_source_id, elasticsearch_source_id, destination_id
    )
    
    if not es_workflow_id:
        print("❌ Failed to create Elasticsearch workflow")
        return
    
    # Step 4: Run Elasticsearch Workflow
    print("\n🚀 Step 4: Running workflows")
    print("-" * 50)
    
    # Skip S3 workflow for now
    s3_job_id = None
    if s3_workflow_id:
        s3_job_id = run_workflow(s3_workflow_id, "S3 PDFs")
    else:
        print("⚠️ S3 PDFs workflow: SKIPPED")
    
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
    print("💡 Jobs are running in background and will deposit results in S3")
    
    # Set dummy job objects for summary
    s3_job = None
    es_job = type('Job', (), {'status': 'SUBMITTED'})()
    
    # Step 6: Pipeline Summary
    print("\n" + "=" * 80)
    print("📊 ELASTICSEARCH RAG PIPELINE SUMMARY")
    print("=" * 80)
    print(f"📁 S3 Source (PDFs): SKIPPED")
    print(f"🔍 Elasticsearch Source: {ELASTICSEARCH_HOST}/{ELASTICSEARCH_INDEX}")
    print(f"📤 S3 Destination: s3://{S3_DESTINATION_BUCKET}/")
    print(f"")
    print(f"🔗 S3 Source Connector ID: SKIPPED")
    print(f"🔗 Elasticsearch Source Connector ID: {elasticsearch_source_id}")
    print(f"🔗 S3 Destination Connector ID: {destination_id}")
    print(f"")
    print(f"⚙️ S3 PDFs Workflow ID: SKIPPED")
    print(f"⚙️ Elasticsearch Sales Workflow ID: {es_workflow_id}")
    print(f"")
    print(f"🚀 S3 PDFs Job ID: SKIPPED")
    print(f"🚀 Elasticsearch Sales Job ID: {es_job_id}")
    print(f"")
    print(f"✅ S3 PDFs Job Status: SKIPPED")
    print(f"✅ Elasticsearch Sales Job Status: {es_job.status if es_job else 'Unknown'}")
    
    # Success check (only Elasticsearch)
    s3_success = False  # Skipped
    es_success = es_job and es_job.status == "COMPLETED"
    
    print(f"\n🎯 PIPELINE RESULTS:")
    print("=" * 30)
    
    if es_success:
        print("🎉 Elasticsearch workflow completed successfully!")
        print("📁 Check your S3 output folder for processed results:")
        print(f"   s3://{S3_DESTINATION_BUCKET}/")
        print("")
        print("🔍 Results include processed Elasticsearch sales data:")
        print("   • Consolidated sales records with full context")
        print("   • Customer and product information")
        print("   • Sales conversations and interactions")
        print("   • Temporal and geographic data")
        print("")
        print("🔍 This Elasticsearch dataset is now ready for RAG applications!")
    else:
        print("⚠️ Elasticsearch workflow completed with issues. Check the logs above.")
        print("💡 Note: S3 PDFs workflow was skipped (commented out)")

if __name__ == "__main__":
    main() 