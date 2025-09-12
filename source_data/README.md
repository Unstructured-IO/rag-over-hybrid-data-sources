# Source Data Management

This directory contains scripts and data files for managing the source data used in the hybrid RAG pipeline.

## Directory Structure

```
source_data/
├── README.md                    # This file
├── s3_pdfs/                    # Directory containing PDF source files
├── sales_data.zip              # sales-records-consolidated index (100 documents, 18KB)
├── sales_records.zip           # sales-records index (100 documents, 16KB)
└── s3_pdfs.zip                 # S3 PDF files (9 Bose headphone manuals, 6.5MB)
```

## Data Files

### Elasticsearch Sales Data

**`sales_data.zip`** - **Used by hybrid RAG pipeline**
- Contains the `sales-records-consolidated` index
- 100 synthetic sales records with consolidated fields
- Used by the automated data preparation in `hybrid_rag_pipeline.py`
- Downloaded from: `https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/sales_data.zip`

**`sales_records.zip`** - **Reference data**
- Contains the `sales-records` index  
- 100 synthetic sales records with separate fields
- Available for comparison or alternative workflows

### S3 PDF Data

**`s3_pdfs.zip`**
- Contains 9 Bose headphone manuals and customer support documents
- Used by the automated data preparation in `hybrid_rag_pipeline.py`
- Downloaded from: `https://github.com/Unstructured-IO/rag-over-hybrid-data-sources/raw/feature/hybrid-rag-pipeline/source_data/s3_pdfs.zip`

## Scripts

### Elasticsearch Sales Index Loader (`../load_es_sales_index.py`)

Manages Elasticsearch index data for both sales indices.

#### Download index data to zip file:
```bash
# Download sales-records-consolidated (used by pipeline)
python ../load_es_sales_index.py download --output source_data/sales_data.zip --index sales-records-consolidated

# Download sales-records (reference data)
python ../load_es_sales_index.py download --output source_data/sales_records.zip --index sales-records
```

#### Load data from zip file to index:
```bash
# Load consolidated data (pipeline default)
python ../load_es_sales_index.py load --input source_data/sales_data.zip

# Load non-consolidated data
python ../load_es_sales_index.py load --input source_data/sales_records.zip --index sales-records
```

### S3 PDFs Loader (`../load_s3_pdfs.py`)

Manages PDF files for the S3 source connector.

#### Zip PDF files from local directory:
```bash
python ../load_s3_pdfs.py zip --input source_data/s3_pdfs --output source_data/s3_pdfs.zip
```

#### Load PDFs from zip file to S3 bucket:
```bash
python ../load_s3_pdfs.py load --input source_data/s3_pdfs.zip
```

## Pipeline Usage

The `hybrid_rag_pipeline.py` automatically downloads and sets up data from:

1. **Elasticsearch Source**: `sales_data.zip` → `sales-records-consolidated` index
2. **S3 Source**: `s3_pdfs.zip` → S3 bucket (from `S3_SOURCE_BUCKET` env var)

The pipeline is configured to use the **consolidated** sales data (`sales_data.zip`) because:
- Multiple fields are consolidated into single long-form text fields
- Provides maximum context for vector search operations
- Optimized for RAG applications where comprehensive searchability is preferred

## Environment Variables Required

Both scripts require the following environment variables to be set in your `.env` file:

### For Elasticsearch operations:
- `ELASTICSEARCH_HOST` - Your Elasticsearch cluster URL
- `ELASTICSEARCH_API_KEY` - API key for authentication

### For S3 operations:
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region (defaults to us-east-1)
- `S3_SOURCE_BUCKET` - S3 bucket name (used as default for load operations)

## File Details

### Sales Data Comparison

| File | Index | Documents | Size | Field Structure | Usage |
|------|-------|-----------|------|-----------------|-------|
| `sales_data.zip` | `sales-records-consolidated` | 100 | 18KB | Consolidated fields | **Pipeline default** |
| `sales_records.zip` | `sales-records` | 100 | 16KB | Separate fields | Reference/comparison |

### PDF Files Included

The `s3_pdfs.zip` contains these Bose headphone manuals:
- `bose-OpenAudio-manual.pdf`
- `bose-OpenAudio-troubleshooting.pdf`
- `bose-OpenAudio-msds.pdf`
- `bose-OpenAudio-instructions.pdf`
- `bose-SoundSport-userguide.pdf`
- `bose-SoundSport-safety.pdf`
- `bose-SoundSport-manual.pdf`
- `bose-QUIETCOMFORT-manual.pdf`
- `bose-QUIETCOMFORT-troubleshooting-guide.pdf`

## Error Handling

Both scripts include comprehensive error handling:
- Missing environment variables
- Network connectivity issues
- Authentication failures
- File/index not found scenarios
- Partial upload/download failures

All errors are reported with clear messages and appropriate exit codes. 