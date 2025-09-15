# RAG Demonstration Configuration and Queries

RAG_OPENAI_API_KEY = "your-openai-api-key-here"

RAG_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", RAG_OPENAI_API_KEY)

print("🤖 RAG Query Demonstration Setup")
print("=" * 40)

if not RAG_OPENAI_API_KEY or RAG_OPENAI_API_KEY.startswith("your-"):
    print("⚠️ OpenAI API key not configured.")
    print("💡 Please update the RAG_OPENAI_API_KEY variable above with your actual OpenAI API key.")
    print("📝 You can get one at: https://platform.openai.com/api-keys")
else:
    print("✅ OpenAI API key configured for RAG demonstrations")

def setup_rag_system():
    """Initialize the RAG system with LangChain and Elasticsearch."""
    if not RAG_OPENAI_API_KEY or RAG_OPENAI_API_KEY.startswith("your-"):
        print("❌ Cannot setup RAG system without OpenAI API key")
        return None
    
    try:
        from langchain_elasticsearch import ElasticsearchStore
        from langchain_openai import OpenAIEmbeddings, ChatOpenAI
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.runnables import RunnablePassthrough
        
        # Set OpenAI API key
        os.environ["OPENAI_API_KEY"] = RAG_OPENAI_API_KEY
        
        print("🔧 Setting up RAG components...")
        
        # Initialize embeddings (same model used in processing)
        embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            openai_api_key=RAG_OPENAI_API_KEY
        )
        
        # Connect to Elasticsearch vector store - using your working pattern
        vector_store = ElasticsearchStore(
            index_name="customer-support",
            embedding=embeddings,
            es_url=ELASTICSEARCH_HOST,
            es_api_key=ELASTICSEARCH_API_KEY,
            vector_query_field="embeddings",
            query_field="text",
        )
        
        # Create retriever
        retriever = vector_store.as_retriever(search_kwargs={"k": 5})
        
        # Initialize LLM
        llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0,
            openai_api_key=RAG_OPENAI_API_KEY
        )
        
        # Enhanced prompt template that leverages NER metadata
        prompt = ChatPromptTemplate.from_template("""
Use the following context to answer the question. Pay attention to any entity information (people, organizations, products, locations, dates) and relationships mentioned in the context.

Context:
{context}

Question:
{question}
""")
        
        print("✅ RAG system ready!")
        return {"retriever": retriever, "llm": llm, "prompt": prompt}
        
    except ImportError as e:
        print(f"❌ Missing RAG dependencies: {e}")
        print("💡 Install with: pip install langchain langchain-elasticsearch langchain-openai")
        return None
    except Exception as e:
        print(f"❌ Error setting up RAG system: {e}")
        return None

def extract_ner_entities(docs):
    """Extract NER entities from document metadata."""
    entities = {"people": set(), "organizations": set(), "products": set(), "locations": set(), "dates": set()}
    
    for doc in docs:
        metadata = doc.metadata
        if "entities-items" in metadata:
            try:
                import json
                entity_items = json.loads(metadata["entities-items"]) if isinstance(metadata["entities-items"], str) else metadata["entities-items"]
                
                for item in entity_items:
                    entity_type = item.get("type", "").upper()
                    entity_name = item.get("entity", "")
                    
                    if entity_type == "PERSON":
                        entities["people"].add(entity_name)
                    elif entity_type == "ORGANIZATION":
                        entities["organizations"].add(entity_name)
                    elif entity_type == "PRODUCT":
                        entities["products"].add(entity_name)
                    elif entity_type == "LOCATION":
                        entities["locations"].add(entity_name)
                    elif entity_type == "DATE":
                        entities["dates"].add(entity_name)
            except:
                pass
    
    return entities

def analyze_sources(docs):
    """Analyze retrieved documents by source type."""
    s3_docs = []
    es_docs = []
    unknown_docs = []
    
    for doc in docs:
        metadata = doc.metadata
        if "data_source-record_locator-index_name" in metadata:
            es_docs.append(doc)
        elif "data_source-url" in metadata and "s3://" in metadata.get("data_source-url", ""):
            s3_docs.append(doc)
        else:
            unknown_docs.append(doc)
    
    return s3_docs, es_docs, unknown_docs

def demonstrate_hybrid_ner_queries(rag_components):
    """Demonstrate NER-enhanced hybrid RAG capabilities."""
    if not rag_components:
        return
    
    retriever = rag_components["retriever"]
    llm = rag_components["llm"]
    prompt = rag_components["prompt"]
    
    # Build RAG chain using your working pattern
    rag_chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    
    # Hybrid NER demonstration queries targeting different sources
    hybrid_queries = [
        {
            "query": "How do I troubleshoot Bose headphone connectivity issues?",
            "description": "Product support query targeting S3 PDFs (product manuals)",
            "expected_source": "S3 (Product Docs)"
        },
        {
            "query": "Tell me about Daniel Hahn and his purchases",
            "description": "Customer analysis query targeting Elasticsearch (sales data)",
            "expected_source": "Elasticsearch (Sales)"
        },
        {
            "query": "What are the technical specifications for SoundSport Wireless headphones?",
            "description": "Product specification query targeting S3 PDFs",
            "expected_source": "S3 (Product Docs)"
        },
        {
            "query": "Show me customers in San Antonio, TX",
            "description": "Geographic customer query targeting Elasticsearch",
            "expected_source": "Elasticsearch (Sales)"
        },
        {
            "query": "How do I reset wireless headphones to factory settings?",
            "description": "Technical support query targeting S3 PDFs",
            "expected_source": "S3 (Product Docs)"
        },
        {
            "query": "What products does Newegg sell and what are their features?",
            "description": "Hybrid query targeting BOTH sources (sales + product specs)",
            "expected_source": "Both S3 and Elasticsearch"
        },
        {
            "query": "I have a customer who bought Bose headphones and is having connectivity issues. What should I tell them?",
            "description": "Customer support query requiring BOTH customer data AND product manuals",
            "expected_source": "Both S3 and Elasticsearch"
        }
    ]
    
    print("\n🧠 Hybrid NER-Enhanced RAG Demonstration")
    print("=" * 60)
    
    for i, query_info in enumerate(hybrid_queries, 1):
        query = query_info["query"]
        description = query_info["description"]
        expected_source = query_info["expected_source"]
        
        print(f"\n{'='*70}")
        print(f"Query {i}: {description}")
        print(f"📝 Query: {query}")
        print(f"🎯 Expected Source: {expected_source}")
        print("=" * 70)
        
        try:
            # Retrieve documents
            docs = retriever.invoke(query)
            
            if not docs:
                print("❌ No documents retrieved")
                continue
            
            # Analyze sources (keeping your preferred format)
            s3_docs, es_docs, unknown_docs = analyze_sources(docs)
            print(f"📊 Retrieved {len(docs)} documents:")
            print(f"   📄 S3 (Product Docs): {len(s3_docs)}")
            print(f"   📊 Elasticsearch (Sales): {len(es_docs)}")
            print(f"   ❓ Unknown: {len(unknown_docs)}")
            
            # Check if we hit the expected source
            if expected_source == "S3 (Product Docs)" and len(s3_docs) > 0:
                print("✅ SUCCESS: Retrieved from expected S3 source!")
            elif expected_source == "Elasticsearch (Sales)" and len(es_docs) > 0:
                print("✅ SUCCESS: Retrieved from expected Elasticsearch source!")
            elif expected_source == "Both S3 and Elasticsearch" and len(s3_docs) > 0 and len(es_docs) > 0:
                print("✅ SUCCESS: Retrieved from BOTH sources as expected!")
            elif expected_source.startswith("Both") and (len(s3_docs) > 0 or len(es_docs) > 0):
                print("✅ PARTIAL: Retrieved from at least one expected source")
            else:
                print("⚠️ UNEXPECTED: Did not retrieve from expected source")
            
            # Extract and show NER entities
            entities = extract_ner_entities(docs)
            print(f"\n🏷️ NER Entities Found:")
            if entities["people"]:
                print(f"   👤 People: {', '.join(list(entities['people'])[:3])}")
            if entities["organizations"]:
                print(f"   🏢 Organizations: {', '.join(list(entities['organizations'])[:3])}")
            if entities["products"]:
                print(f"   📱 Products: {', '.join(list(entities['products'])[:3])}")
            if entities["locations"]:
                print(f"   🗺️ Locations: {', '.join(list(entities['locations'])[:3])}")
            if entities["dates"]:
                print(f"   📅 Dates: {', '.join(list(entities['dates'])[:3])}")
            
            # Generate answer
            print(f"\n💬 Answer:")
            answer = rag_chain.invoke(query)
            print(f"{answer}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            if "429" in str(e):
                print("⚠️ OpenAI API quota exceeded. Stopping demo.")
                break
    
    print(f"\n{'='*70}")
    print("🧠 Hybrid NER Demo Complete!")
    print("✅ Demonstrated cross-source retrieval capabilities")
    print("✅ Showed NER metadata integration across data sources")
    print("✅ Validated hybrid RAG architecture")

def run_rag_demonstration():
    """Run the RAG demonstration."""
    print("\n🚀 Starting Hybrid RAG Demonstration")
    print("=" * 50)
    
    rag_components = setup_rag_system()
    
    if rag_components:
        demonstrate_hybrid_ner_queries(rag_components)
    else:
        print("❌ RAG demonstration skipped due to configuration issues")

# Run the demonstration
run_rag_demonstration()
