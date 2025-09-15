#!/usr/bin/env python3
"""
S3 PDFs Data Loader

This script provides functionality to:
1. Zip PDF files from a local directory and save as a zip file
2. Load PDFs from a zip file and upload them to an S3 bucket

Usage:
    # Zip PDFs from local directory
    python load_s3_pdfs.py zip --input source_data/s3_pdfs --output source_data/s3_pdfs.zip
    
    # Load PDFs from zip file to S3 bucket
    python load_s3_pdfs.py load --input source_data/s3_pdfs.zip
"""

import os
import sys
import zipfile
import argparse
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Load environment variables
load_dotenv()

def get_s3_client():
    """Initialize and return S3 client."""
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in .env file")
    
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

def zip_pdfs(input_dir: str, output_path: str = None):
    """Zip PDF files from a directory."""
    print(f"🔄 Zipping PDFs from '{input_dir}'...")
    
    # Generate output path based on input directory name if not provided
    if not output_path:
        input_path = Path(input_dir)
        # Use the directory name to create zip file name
        dir_name = input_path.name
        output_path = f"source_data/{dir_name}.zip"
    
    try:
        input_path = Path(input_dir)
        if not input_path.exists():
            raise ValueError(f"❌ Input directory '{input_path}' does not exist")
        
        # Find all PDF files
        pdf_files = list(input_path.rglob("*.pdf"))
        if not pdf_files:
            raise ValueError(f"❌ No PDF files found in '{input_path}'")
        
        print(f"📊 Found {len(pdf_files)} PDF files")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create zip file
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for pdf_file in pdf_files:
                # Get relative path to maintain directory structure
                relative_path = pdf_file.relative_to(input_path)
                zipf.write(pdf_file, relative_path)
                print(f"  📁 Added: {relative_path}")
        
        print(f"✅ Successfully zipped {len(pdf_files)} PDFs to {output_path}")
        
    except Exception as e:
        print(f"❌ Error zipping PDFs: {e}")
        sys.exit(1)

def load_pdfs_to_s3(input_path: str, bucket_name: str = None):
    """Load PDFs from zip file and upload to S3 bucket."""
    if not bucket_name:
        bucket_name = os.getenv("S3_SOURCE_BUCKET")
    
    if not bucket_name:
        raise ValueError("S3_SOURCE_BUCKET must be set in .env file or provided as argument")
    
    print(f"🔄 Loading PDFs to S3 bucket '{bucket_name}'...")
    
    try:
        input_path = Path(input_path)
        if not input_path.exists():
            raise ValueError(f"❌ Input file '{input_path}' does not exist")
        
        s3 = get_s3_client()
        
        # Check if bucket exists, create if it doesn't
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"📦 Using existing bucket '{bucket_name}'")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"🔧 Creating bucket '{bucket_name}'...")
                try:
                    # Get region for bucket creation
                    aws_region = os.getenv("AWS_REGION", "us-east-1")
                    if aws_region == "us-east-1":
                        s3.create_bucket(Bucket=bucket_name)
                    else:
                        s3.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': aws_region}
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
        with zipfile.ZipFile(input_path, 'r') as zipf:
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
        else:
            print(f"❌ Bucket '{bucket_name}' is empty after upload")
            sys.exit(1)
        
    except NoCredentialsError:
        print("❌ AWS credentials not found. Please check your .env file.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading PDFs to S3: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="S3 PDFs Data Loader")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Zip command
    zip_parser = subparsers.add_parser('zip', help='Zip PDF files from directory')
    zip_parser.add_argument('--input', '-i', required=True,
                           help='Input directory containing PDFs (e.g., source_data/s3_pdfs)')
    zip_parser.add_argument('--output', '-o',
                           help='Output zip file path (default: source_data/{dirname}.zip)')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load PDFs from zip file to S3 bucket')
    load_parser.add_argument('--input', '-i', required=True,
                            help='Input zip file path (e.g., source_data/s3_pdfs.zip)')
    load_parser.add_argument('--bucket', '-b', 
                            help='S3 bucket name (defaults to S3_SOURCE_BUCKET from .env)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'zip':
        zip_pdfs(args.input, args.output)
    elif args.command == 'load':
        load_pdfs_to_s3(args.input, args.bucket)

if __name__ == "__main__":
    main() 