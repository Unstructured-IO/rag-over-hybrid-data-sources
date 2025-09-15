# Data preparation functions

def download_file(url: str, local_path: str) -> bool:
    """Download a file from URL to local path."""
    try:
        print(f"📥 Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
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
        es = Elasticsearch(
            ELASTICSEARCH_HOST,
            api_key=ELASTICSEARCH_API_KEY,
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )
        
        index_name = "sales-records-consolidated"
        
        sales_data_url = "https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/sales_records_consolidated.zip"
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            if not download_file(sales_data_url, tmp_file.name):
                return False
            
            with zipfile.ZipFile(tmp_file.name, 'r') as zipf:
                # Load mapping
                with zipf.open('mapping.json') as f:
                    mapping_data = json.loads(f.read().decode('utf-8'))
                
                # Load documents
                with zipf.open('documents.json') as f:
                    documents = json.loads(f.read().decode('utf-8'))
            
            # Always delete existing index if present and reload from zip
            if es.indices.exists(index=index_name):
                print(f"🗑️ Deleting existing index '{index_name}' to reload fresh data...")
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