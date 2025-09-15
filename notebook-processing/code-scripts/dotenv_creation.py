def create_dotenv_file():
    """Create a .env file with placeholder values for the user to fill in."""
    env_content = """# Unstructured API Configuration
UNSTRUCTURED_API_KEY=your-unstructured-api-key
UNSTRUCTURED_API_URL=https://platform.unstructuredapp.io/api/v1

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
S3_SOURCE_BUCKET=your-s3-bucket-name

# Elasticsearch Configuration
ELASTICSEARCH_HOST=https://your-cluster.es.io:9200
ELASTICSEARCH_API_KEY=your-elasticsearch-api-key
ELASTICSEARCH_INDEX=sales-records-consolidated

# OpenAI Configuration (for RAG demo)
OPENAI_API_KEY=your-openai-api-key
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ Created .env file with placeholder values")
    print("📝 Please edit the .env file and replace the placeholder values with your actual credentials")
    print("🔒 The .env file will be loaded automatically by the pipeline")

# Create the .env file
create_dotenv_file()
