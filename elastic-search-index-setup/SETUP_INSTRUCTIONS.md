# Elasticsearch Index Setup Instructions

This guide walks you through setting up an Elasticsearch index with synthetic Bose sales data for use in a hybrid RAG pipeline.

## 🎯 Overview

We'll create:
- An Elasticsearch index called `sales-records`
- 500 synthetic sales records with rich named entities (NER-optimized)
- An API key with appropriate permissions for index management

## 📋 Prerequisites

- Elasticsearch Cloud deployment
- Python virtual environment with required packages
- Access to create API keys in your Elasticsearch deployment

## 🔑 Step 1: Create API Key with Full Index Permissions

### 1.1 Navigate to API Key Creation
1. Go to your Elasticsearch Cloud deployment
2. Click **"Create API key"** button
3. Fill in the setup details:
   - **Name**: `bose-sales-data-full-access` (or your preferred name)
   - **Type**: Personal
   - **Apply expiration date**: ✅ Enabled (recommended for security)
   - **Days**: 15-30 days (adjust as needed)

### 1.2 Configure Security Privileges
1. Enable **"Control security privileges"** ✅
2. In the JSON text box, paste the following role descriptor:

```json
{
  "sales-records-full-access": {
    "indices": [
      {
        "names": ["sales-records"],
        "privileges": [
          "create_index",
          "delete_index", 
          "manage",
          "write",
          "read",
          "view_index_metadata",
          "monitor"
        ]
      }
    ]
  }
}
```

### 1.3 Create the API Key
1. Click **"Create API key"**
2. **IMPORTANT**: Copy the generated API key immediately (it won't be shown again)

## 🔧 Step 2: Configure Environment

### 2.1 Update .env File
1. Navigate to the project root directory
2. Edit the `.env` file:
   ```bash
   nano .env
   ```
3. Update the API key:
   ```env
   ELASTIC_API_KEY=your-new-api-key-here
   ```
4. Save and close the file

### 2.2 Install Dependencies
```bash
# Activate virtual environment
source venv/bin/activate

# Install required packages
pip install -r elastic-search-index-setup/requirements.txt
```

## 🚀 Step 3: Run Setup Scripts

Navigate to the setup directory:
```bash
cd elastic-search-index-setup
```

### 3.1 Test Connection (Optional)
First, verify your API key works:
```bash
python simple_check.py
```

**Expected Output**: Should show connection success or indicate if index doesn't exist yet.

### 3.2 Create Index and Upload Synthetic Data
Run the main setup script:
```bash
python elasticsearch_setup.py
```

**What this script does**:
- ✅ Tests connection to Elasticsearch
- ✅ Creates `sales-records` index with NER-optimized mapping
- ✅ Generates 500 synthetic Bose sales records (2016-present)
- ✅ Bulk indexes all records to Elasticsearch
- ✅ Provides setup completion summary

**Expected Output**:
```
🚀 Starting Elasticsearch Setup for Bose Sales Data
============================================================
⚙️ Loading configuration...
   Index name: sales-records
   Records to generate: 500
🔧 Testing Elasticsearch connection...
✅ Successfully connected to Elasticsearch
   Index 'sales-records' exists: False
🔧 Creating index: sales-records
✅ Created new index: sales-records
🔄 Generating 500 synthetic sales records...
  Generated 100/500 records...
  [... progress updates ...]
✅ Generated 500 records successfully
📤 Bulk indexing 500 records...
✅ Successfully indexed 500 records
🎉 SETUP COMPLETE!
```

### 3.3 Verify Data Upload
Confirm the data was successfully indexed:
```bash
python simple_check.py
```

**Expected Output**:
```
🔍 Checking sales-records index...
📊 Total documents: 500

✅ SUCCESS! Data has been indexed.

📋 Sample documents:
📄 Document 1:
   👤 Customer: Jennifer Martinez
   🎧 Product: SoundSport Free
   💰 Price: $149
   🏪 Retailer: Best Buy
   📝 Text: Customer Jennifer Martinez from New York, NY called to inquire...

🎉 Your synthetic Bose sales data is ready!
🔗 Ready to use as Elasticsearch source connector in Unstructured Workflow
```

## 📊 Step 4: Verify in Elasticsearch Console

1. Go to your Elasticsearch deployment
2. Navigate to **Index Management**
3. Look for the `sales-records` index
4. You should see:
   - **Health**: Green
   - **Status**: Open
   - **Documents**: 500
   - **Storage size**: ~500KB-1MB

## 🎯 What You Now Have

### Rich NER-Optimized Data
Your `sales-records` index contains 500 synthetic records with:

- **PERSON entities**: Customer names, sales representatives
- **ORGANIZATION entities**: Retailers (Best Buy, Target, Amazon, etc.)
- **LOCATION entities**: Cities and regions across the US
- **MONETARY entities**: Product prices and revenue potential
- **DATE entities**: Timestamps from 2016 to present
- **PRODUCT entities**: Bose product lines and specific models

### Sample Record Structure
```json
{
  "customer_name": "Jennifer Martinez",
  "sales_representative": "Michael Chen",
  "product_model": "SoundSport Free",
  "price": 149,
  "retailer": "Best Buy",
  "location_city": "New York, NY",
  "region": "Northeast",
  "interaction_text": "Customer Jennifer Martinez from New York, NY called to inquire about purchasing the SoundSport Free...",
  "timestamp": "2023-11-15T14:30:00",
  "quarter": "Q4",
  "year": 2023
}
```

## 🔗 Next Steps: Hybrid RAG Pipeline

Your synthetic data is now ready to be used as:

1. **Elasticsearch Source Connector** in Unstructured Workflow Endpoint
2. **Input for NER Enrichment** - Extract named entities from sales interactions
3. **Hybrid Data Source** - Combine with S3 technical documentation (manuals, troubleshooting guides, MSDS)
4. **RAG System Foundation** - Process both sources through NER → Qdrant destination

## 🛠️ Troubleshooting

### Connection Issues
- Verify API key is correctly set in `.env` file
- Ensure API key hasn't expired
- Check that security privileges JSON was pasted correctly

### Permission Errors
- Confirm all required privileges are included in the API key role descriptor
- Verify the index name matches exactly (`sales-records`)

### Data Issues
- Run `simple_check.py` to verify document count
- Check Elasticsearch console for index health status
- Re-run `elasticsearch_setup.py` if needed (it will recreate the index)

## 📁 File Reference

- `elasticsearch_setup.py` - Main setup script (creates index + data)
- `simple_check.py` - Quick verification script
- `verify_elasticsearch_data.py` - Comprehensive data analysis
- `requirements.txt` - Python dependencies
- `.env` - Environment configuration (API key)

---

✅ **Setup Complete!** Your synthetic Bose sales data is ready for NER processing and hybrid RAG implementation. 