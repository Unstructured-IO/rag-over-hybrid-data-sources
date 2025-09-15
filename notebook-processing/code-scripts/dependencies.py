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
# ENVIRONMENT CONFIGURATION
# =============================================================================
# Load from .env file if it exists
load_dotenv()

# Configuration constants
SKIPPED = "SKIPPED"
UNSTRUCTURED_API_URL = os.getenv("UNSTRUCTURED_API_URL", "https://platform.unstructuredapp.io/api/v1")

# Get environment variables
UNSTRUCTURED_API_KEY = os.getenv("UNSTRUCTURED_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")
S3_DESTINATION_BUCKET = os.getenv("S3_DESTINATION_BUCKET")
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sales-records-consolidated")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Validation
REQUIRED_VARS = {
    "UNSTRUCTURED_API_KEY": UNSTRUCTURED_API_KEY,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
    "ELASTICSEARCH_API_KEY": ELASTICSEARCH_API_KEY,
    "S3_SOURCE_BUCKET": S3_SOURCE_BUCKET,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value]
if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("Please set these environment variables or create a .env file with your credentials.")
    raise ValueError(f"Missing required environment variables: {missing_vars}")

print("✅ Configuration loaded successfully")
