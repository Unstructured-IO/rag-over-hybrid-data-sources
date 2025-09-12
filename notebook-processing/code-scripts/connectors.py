def create_s3_source_connector():
    """Create an S3 source connector for PDF documents."""
    try:
        if not S3_SOURCE_BUCKET:
            raise ValueError("S3_SOURCE_BUCKET is required (bucket name, s3:// URL, or https:// URL)")
        value = S3_SOURCE_BUCKET.strip()

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
        print(f"✅ Created S3 PDF source connector: {source_id} -> {s3_style}")
        return source_id
        
    except Exception as e:
        print(f"❌ Error creating S3 source connector: {e}")
        return None

def create_elasticsearch_source_connector():
    """Create an Elasticsearch source connector for sales data."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
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
        print(f"✅ Created Elasticsearch sales source connector: {source_id}")
        return source_id
    
    except Exception as e:
        print(f"❌ Error creating Elasticsearch source connector: {e}")
        return None

def create_elasticsearch_destination_connector():
    """Create an Elasticsearch destination connector for processed results."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
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

        destination_id = response.destination_connector_information.id
        print(f"✅ Created Elasticsearch destination connector: {destination_id}")
        return destination_id
        
    except Exception as e:
        print(f"❌ Error creating Elasticsearch destination connector: {e}")
        return None
