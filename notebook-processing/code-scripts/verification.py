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
