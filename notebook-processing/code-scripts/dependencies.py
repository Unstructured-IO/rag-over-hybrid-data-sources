import sys, subprocess

def ensure_notebook_deps() -> None:
    packages = [
        "jupytext",
        "python-dotenv", 
        "unstructured-client",
        "elasticsearch",
        "boto3",
        "PyYAML",
        "langchain",
        "langchain-elasticsearch",
        "langchain-openai"
    ]
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *packages])
    except Exception:
        # If install fails, continue; imports below will surface actionable errors
        pass

# Install notebook dependencies (safe no-op if present)
ensure_notebook_deps()

import os
import time
import json
import zipfile
import tempfile
import requests
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import (
    CreateSourceRequest,
    CreateDestinationRequest,
    CreateWorkflowRequest
)
from unstructured_client.models.shared import (
    CreateSourceConnector,
    CreateDestinationConnector,
    WorkflowNode,
    WorkflowType,
    CreateWorkflow
)

# =============================================================================
# GOOGLE COLAB ENVIRONMENT SETUP
# =============================================================================
# Paste your actual credentials here (replace the placeholder values):

UNSTRUCTURED_API_KEY = "your-unstructured-api-key"
AWS_ACCESS_KEY_ID = "your-aws-access-key"
AWS_SECRET_ACCESS_KEY = "your-aws-secret-key"
AWS_REGION = "us-east-1"
S3_SOURCE_BUCKET = "your-s3-bucket-name"
ELASTICSEARCH_HOST = "https://your-cluster.es.io:9200"
ELASTICSEARCH_API_KEY = "your-elasticsearch-api-key"
ELASTICSEARCH_INDEX = "sales-records-consolidated"

# Optional: OpenAI API key for RAG functionality
OPENAI_API_KEY = "your-openai-api-key"

# =============================================================================
# ENVIRONMENT FILE FALLBACK
# =============================================================================
# Load from .env file (will override the values above if file exists)
load_dotenv()

# Override with environment variables if available
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", UNSTRUCTURED_API_KEY)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)  
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
AWS_REGION = os.getenv("AWS_REGION", AWS_REGION)
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", S3_SOURCE_BUCKET)
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", ELASTICSEARCH_HOST)
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", ELASTICSEARCH_API_KEY)
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", ELASTICSEARCH_INDEX)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", OPENAI_API_KEY)

# Configuration constants
SKIPPED = "SKIPPED"
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Validation
REQUIRED_VARS = {
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update the configuration section above with your actual API keys.")
    raise ValueError(f"Missing required configuration values: {missing_vars}")

print("✅ Configuration loaded successfully") 