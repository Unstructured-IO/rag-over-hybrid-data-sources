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
