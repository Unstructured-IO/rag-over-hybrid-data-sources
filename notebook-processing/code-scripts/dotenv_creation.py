def create_dotenv_file():
    """Create a .env file with placeholder values for the user to fill in."""
    env_content = """# Hybrid RAG Pipeline Environment Configuration
# Fill in your actual values below
# Configuration - Set these explicitly

# ===================================================================
# AWS CONFIGURATION
# ===================================================================
AWS_ACCESS_KEY_ID="your-aws-access-key-id"
AWS_SECRET_ACCESS_KEY="your-aws-secret-access-key"
AWS_REGION="us-east-1"

# ===================================================================
# UNSTRUCTURED API CONFIGURATION  
# ===================================================================
UNSTRUCTURED_API_KEY="your-unstructured-api-key"
UNSTRUCTURED_API_URL="https://platform.unstructuredapp.io/api/v1"

# ===================================================================
# ELASTICSEARCH CONFIGURATION
# ===================================================================
ELASTICSEARCH_HOST="https://your-cluster.es.io:443"
ELASTICSEARCH_API_KEY="your-elasticsearch-api-key"

# ===================================================================
# PIPELINE DATA SOURCES
# ===================================================================
S3_SOURCE_BUCKET="your-s3-source-bucket"
S3_DESTINATION_BUCKET="your-s3-destination-bucket"
S3_OUTPUT_PREFIX=""
ELASTICSEARCH_INDEX="sales-records-consolidated"

# ===================================================================
# OPENAI API CONFIGURATION 
# ===================================================================
OPENAI_API_KEY="your-openai-api-key"
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ Created .env file with placeholder values")
    print("📝 Please edit the .env file and replace the placeholder values with your actual credentials")
    print("🔒 The .env file will be loaded automatically by the pipeline")

# Create the .env file
create_dotenv_file()
