# Notebook Processing Tools

This directory contains tools for processing Jupyter notebooks and setting up data sources for hybrid RAG pipelines.

## remove_images.py

A Python script that uses regular expressions to remove embedded base64-encoded images from Python files that were converted from Jupyter notebooks using `jupytext`.

### Features

- Removes base64 data URL images (e.g., `![Screenshot 1](data:image/png;base64,...)`)
- Cleans up extra empty lines left behind after image removal
- Can either overwrite the original file or create a new cleaned file
- Provides detailed feedback on the number of images found and removed

### Usage

```bash
# Remove images from a file (overwrites original)
python remove_images.py <input_file.py>

# Remove images and save to a new file
python remove_images.py <input_file.py> <output_file.py>
```

### Examples

```bash
# Clean the converted notebook file in-place
python remove_images.py ../donor-notebooks/S3_to_Qdrant_Workflow_using_Unstructured_API.py

# Create a cleaned copy
python remove_images.py notebook.py cleaned_notebook.py
```

### Requirements

- Python 3.6+
- No external dependencies (uses only standard library modules: `re`, `sys`, `os`, `pathlib`)

### How it works

The script uses a regular expression pattern to identify and remove markdown-style image references with base64 data URLs:

```python
image_pattern = r'!\[.*?\]\(data:image/[^;]+;base64,[A-Za-z0-9+/=]+\)'
```

This pattern matches:
- `![...]` - Markdown image syntax
- `(data:image/...)` - Data URL with image MIME type
- `;base64,` - Base64 encoding indicator
- `[A-Za-z0-9+/=]+` - Base64 encoded data

## Workflow Example

1. Convert Jupyter notebook to Python file using `jupytext`:
   ```bash
   jupytext --to py notebook.ipynb
   ```

2. Remove embedded images from the converted file:
   ```bash
   python remove_images.py notebook.py
   ```

The result is a clean Python file without embedded base64 images, making it more readable and reducing file size significantly.

## elasticsearch_setup.py

A comprehensive Python script that creates and populates an Elasticsearch index with NER-rich synthetic sales data for Bose products. This data serves as one of the source connectors in a hybrid RAG pipeline.

### Features

- **Elastic Cloud Integration**: Connects directly to your Elasticsearch Cloud deployment
- **NER-Optimized Data**: Generates synthetic sales records rich in named entities (people, organizations, locations, prices, dates)
- **Semantic Text Support**: Uses `semantic_text` field type for enhanced search capabilities
- **Bose Product Focus**: Covers SoundSport, OpenAudio, and QuietComfort product lines
- **Realistic Sales Scenarios**: Creates contextual sales interactions with detailed customer information

### Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   ```bash
   # Copy the template and add your credentials
   cp env_template.txt .env
   # Edit .env and add your ELASTIC_API_KEY
   ```

3. **Run the Setup**:
   ```bash
   python elasticsearch_setup.py
   ```

### Generated Data Structure

Each sales record contains rich named entities perfect for NER extraction:

- **PERSON**: Customer names, sales representatives
- **ORG**: Retailers (Best Buy, Target, Amazon, etc.)
- **LOCATION**: Cities and regions across the US
- **MONEY**: Product prices and revenue potential
- **DATE**: Timestamps, quarters, months
- **PRODUCT**: Bose product lines and specific models

### Example Generated Record

```json
{
  "customer_name": "Jennifer Martinez",
  "sales_representative": "Michael Chen", 
  "product_model": "SoundSport Free",
  "price": 149,
  "retailer": "Best Buy",
  "location_city": "New York, NY",
  "interaction_text": "Customer Jennifer Martinez from New York, NY called to inquire about purchasing the SoundSport Free. Sales rep Michael Chen provided detailed product information and quoted $149. Customer is comparing with similar products at Best Buy.",
  "text": "Customer Jennifer Martinez from New York, NY called to inquire about purchasing the SoundSport Free. Sales rep Michael Chen provided detailed product information and quoted $149. Customer is comparing with similar products at Best Buy."
}
```

### Integration with Unstructured Workflow

This Elasticsearch index can be used as a source connector in the Unstructured Workflow Endpoint alongside S3 technical documentation to create a hybrid RAG system:

1. **S3 Source**: Technical manuals, troubleshooting guides, MSDS PDFs
2. **Elasticsearch Source**: Synthetic sales data (this script)
3. **NER Enrichment**: Extract named entities from both sources
4. **Qdrant Destination**: Combined processed data for RAG queries

### Requirements

See `requirements.txt` for Python dependencies:
- elasticsearch>=8.0.0
- python-dotenv>=0.19.0
- faker>=15.0.0

## verify_elasticsearch_data.py

A comprehensive verification script that inspects and validates the synthetic sales data in your Elasticsearch index. Use this script to confirm that data was successfully uploaded and is ready for NER processing.

### Features

- **Connection Testing**: Verifies Elasticsearch cluster connectivity
- **Index Validation**: Confirms the index exists and contains data
- **Data Statistics**: Provides comprehensive metrics on document count, index size, and distribution
- **Sample Document Display**: Shows actual records with key fields
- **NER Readiness Check**: Validates that data contains rich named entities
- **Search Query Testing**: Tests various search patterns to ensure data accessibility

### Usage

```bash
python verify_elasticsearch_data.py
```

### What It Checks

1. **Basic Connectivity**: Tests connection to your Elasticsearch cluster
2. **Index Existence**: Confirms the `sales-records` index exists
3. **Document Count**: Reports total number of indexed documents
4. **Data Distribution**: Analyzes breakdown by:
   - Product lines (SoundSport, OpenAudio, QuietComfort)
   - Product models
   - Retailers (Best Buy, Target, Amazon, etc.)
   - Geographic regions
   - Interaction types
   - Price and revenue statistics
   - Temporal distribution (by year)
5. **NER Entity Validation**: Confirms presence of:
   - Person names (customers, sales reps)
   - Organizations (retailers)
   - Locations (cities, regions)
   - Monetary values (prices)
   - Dates (timestamps)
   - Rich text content
6. **Search Functionality**: Tests sample queries to verify data is searchable

### Sample Output

```
🚀 Starting Elasticsearch Data Verification
============================================================
🔧 Testing Elasticsearch connection...
✅ Connected to Elasticsearch cluster: instance-0000000000
   Version: 8.11.0
   Cluster: 2371b9a1d2ad40c590fd1e22652a8236

✅ Index 'sales-records' exists
📊 Getting statistics for index 'sales-records'...
   📄 Total documents: 500
   💾 Index size: 245,760 bytes (0.23 MB)
   🔧 Primary shards: 12

📋 Retrieving 5 sample documents...
📄 Document 1:
   🆔 ID: abc123-def456
   👤 Customer: Jennifer Martinez
   🏷️ Product: SoundSport Free
   💰 Price: $149
   🏪 Retailer: Best Buy
   📍 Location: New York, NY
   📅 Date: 2023-11-15T14:30:00
   📝 Text: Customer Jennifer Martinez from New York, NY called to inquire about purchasing the SoundSport...
``` 