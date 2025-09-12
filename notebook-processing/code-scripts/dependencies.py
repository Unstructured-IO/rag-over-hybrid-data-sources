import sys, subprocess

def ensure_notebook_deps() -> None:
    packages = [
        "jupytext",
        "python-dotenv",
        "unstructured-client",
        "elasticsearch",
        "boto3",
        "PyYAML",
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

# Load environment variables
load_dotenv()

# Configuration
SKIPPED = "SKIPPED"

# =============================================================================
# CONFIGURATION OPTIONS - Choose ONE of the following methods:
# =============================================================================

# METHOD 1: Use .env file (RECOMMENDED)
# Create a .env file in the project root with your actual values
# Then keep all the os.getenv() lines below as-is

# METHOD 2: Paste your credentials directly below
# Comment out the os.getenv() lines and uncomment the direct assignment lines
# Replace "PASTE_YOUR_VALUE_HERE" with your actual credentials

# =============================================================================
# AWS CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-access-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")

# Method 2: Direct assignment (uncomment and paste your values)
# AWS_ACCESS_KEY_ID = "PASTE_YOUR_AWS_ACCESS_KEY_ID_HERE"
# AWS_SECRET_ACCESS_KEY = "PASTE_YOUR_AWS_SECRET_ACCESS_KEY_HERE"
# AWS_REGION = "us-east-1"
# S3_SOURCE_BUCKET = "PASTE_YOUR_SOURCE_BUCKET_NAME_HERE"

# =============================================================================
# UNSTRUCTURED API CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY", "your-unstructured-api-key")
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Method 2: Direct assignment (uncomment and paste your values)
# UNSTRUCTURED_API_KEY = "PASTE_YOUR_UNSTRUCTURED_API_KEY_HERE"
# UNSTRUCTURED_API_URL = "https://platform.unstructuredapp.io/api/v1"

# =============================================================================
# ELASTICSEARCH CONFIGURATION
# =============================================================================
# Method 1: Environment variables (default)
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "your-elasticsearch-host")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "your-elasticsearch-api-key")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")

# Method 2: Direct assignment (uncomment and paste your values)
# ELASTICSEARCH_HOST = "PASTE_YOUR_ELASTICSEARCH_HOST_HERE"  # e.g., "https://my-cluster.es.us-east-1.aws.com:9200"
# ELASTICSEARCH_API_KEY = "PASTE_YOUR_ELASTICSEARCH_API_KEY_HERE"
# ELASTICSEARCH_INDEX = "sales-records-consolidated"

# Validation
REQUIRED_VARS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value or value.startswith("your-")]
if missing_vars:
    print(f"❌ Missing required configuration values: {', '.join(missing_vars)}")
    print("Please update your .env file with the required values.")
    raise ValueError(f"Missing required configuration values: {missing_vars}")

print("✅ Configuration loaded successfully") 