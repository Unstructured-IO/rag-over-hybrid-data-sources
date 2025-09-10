#!/usr/bin/env python3
"""
Hybrid RAG Pipeline: S3 + Elasticsearch to S3 with NER Enrichment

This script demonstrates a hybrid RAG pipeline using the Unstructured Workflow Endpoint:
- Source 1: S3 bucket containing Bose product PDFs (manuals, troubleshooting, MSDS)
- Source 2: Elasticsearch index with synthetic sales data
- Destination: S3 bucket (output/ subfolder) for processed results
- Enrichment: Named Entity Recognition (NER) before final storage

Pipeline Architecture:
[Elasticsearch Sales Data] → [VLM Partition] → [Chunk] → [Embed] → [NER Enrichment] → [S3 Output]
"""

import os
import sys
import time
from dotenv import load_dotenv

from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import (
    CreateSourceRequest,
    CreateDestinationRequest
)
from unstructured_client.models.shared import (
    CreateSourceConnector,
    SourceConnectorType,
    CreateDestinationConnector,
    WorkflowNode,
    WorkflowType
)

# Load environment variables
load_dotenv()

# Configuration
SKIPPED = "SKIPPED"

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-access-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", "example-data-bose-headphones")
S3_DESTINATION_BUCKET = os.getenv("S3_DESTINATION_BUCKET", "example-data-bose-headphones-output")
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
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    sys.exit(1)

print("✅ Configuration loaded successfully")

# Initialize Unstructured client
unstructured_client = UnstructuredClient(
    api_key_auth=UNSTRUCTURED_API_KEY,
    server_url=UNSTRUCTURED_API_URL
)

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

def create_s3_source_connector():
    """Create an S3 source connector for PDF documents."""
    try:
        response = unstructured_client.sources.create_source(
            request=CreateSourceRequest(
                create_source_connector=CreateSourceConnector(
                    name=f"s3_pdf_source_{int(time.time())}",
                    type=SourceConnectorType.S3,
                    config={
                        "remote_url": f"s3://{S3_SOURCE_BUCKET}/",
                        "recursive": True,
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "region": AWS_REGION
                    }
                )
            )
        )
        
        source_id = response.source_connector_information.id
        print(f"✅ Created S3 PDF source connector: {source_id}")
        return source_id
        
    except Exception as e:
        print(f"❌ Error creating S3 source connector: {e}")
        return None

def create_elasticsearch_source_connector():
    """Create an Elasticsearch source connector for sales data."""
    try:
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
        print(f"✅ Created Elasticsearch sales source connector: {source_id}")
        return source_id
        
    except Exception as e:
        print(f"❌ Error creating Elasticsearch source connector: {e}")
        return None

def create_elasticsearch_destination_connector():
    """Create an Elasticsearch destination connector for processed results."""
    try:
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
        print(f"✅ Created Elasticsearch destination connector: {destination_id}")
        return destination_id
        
    except Exception as e:
        print(f"❌ Error creating Elasticsearch destination connector: {e}")
        return None

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

def create_parallel_workflows(s3_source_id, elasticsearch_source_id, destination_id):
    """Create separate workflows for S3 PDFs and Elasticsearch data that run in parallel."""
    try:
        vlm_partition_node, chunk_node, embedder_node, ner_enrichment_node = create_workflow_nodes()
        
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
                            vlm_partition_node,
                            chunk_node,
                            embedder_node,
                            ner_enrichment_node
                        ],
                    }
                }
            )
            
            s3_workflow_id = s3_response.workflow_information.id
            print(f"✅ Created S3 PDF workflow: {s3_workflow_id}")
        
        # Create workflow for Elasticsearch sales data
        es_response = unstructured_client.workflows.create_workflow(
            request={
                "create_workflow": {
                    "name": f"Elasticsearch-Sales-Parallel-Workflow_{int(time.time())}",
                    "source_id": elasticsearch_source_id,
                    "destination_id": destination_id,
                    "workflow_type": WorkflowType.CUSTOM,
                    "workflow_nodes": [
                        vlm_partition_node,
                        chunk_node,
                        embedder_node,
                        ner_enrichment_node
                    ],
                }
            }
        )
        
        es_workflow_id = es_response.workflow_information.id
        print(f"✅ Created Elasticsearch sales workflow: {es_workflow_id}")
        
        return s3_workflow_id, es_workflow_id
        
    except Exception as e:
        print(f"❌ Error creating parallel workflows: {e}")
        return None, None

def run_workflow(workflow_id, workflow_name):
    """Run a workflow and return job information."""
    try:
        response = unstructured_client.workflows.run_workflow(
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
            response = unstructured_client.jobs.get_job(
                request={"job_id": job_id}
            )
            
            job = response.job_information
            status = job.status
            
            if status in ["SCHEDULED", "IN_PROGRESS"]:
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

def run_elasticsearch_preprocessing():
    """Run Elasticsearch preprocessing to create required indices."""
    print("🔧 Running Elasticsearch preprocessing...")
    
    try:
        from elasticsearch_index_preprocessing import run_elasticsearch_preprocessing
        return run_elasticsearch_preprocessing()
    except ImportError:
        print("⚠️ Elasticsearch preprocessing module not found, skipping...")
        return True
    except Exception as e:
        print(f"❌ Error running Elasticsearch preprocessing: {e}")
        return False

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

if __name__ == "__main__":
    main()
