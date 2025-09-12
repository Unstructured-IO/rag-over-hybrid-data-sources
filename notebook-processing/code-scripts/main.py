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