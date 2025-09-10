# Elasticsearch Data Structure Examples

This directory contains examples of two different approaches for structuring synthetic sales data in Elasticsearch for RAG (Retrieval-Augmented Generation) use cases.

## 📊 **Data Structure Comparison**

### 1. Non-Consolidated Structure (`nonconsolidated_examples.json`)

**Use Case**: Traditional database-like structure with separate fields for each data element.

**Characteristics**:
- Each piece of information is stored in its own field
- Easy to query specific fields (e.g., all records from "Best Buy")
- Good for analytics and aggregations
- Follows normalized database principles

**⚠️ Problem with Unstructured Processing**:
When Unstructured processes this data and creates separate Text elements for each field, **context is lost**. For example:
- One Text element might contain: "Jennifer Martinez"
- Another Text element might contain: "SoundSport Free"
- Another Text element might contain: "$149"

The resulting embeddings won't understand that Jennifer Martinez was interested in the SoundSport Free at $149.

### 2. Consolidated Structure (`consolidated_examples.json`)

**Use Case**: RAG-optimized structure where all context is preserved in a single text field.

**Characteristics**:
- All relevant information combined into a single `consolidated_text` field
- Maintains complete context relationships
- Optimized for embedding generation and RAG queries
- Minimal metadata fields for filtering/aggregation

**✅ Benefits for RAG**:
When Unstructured processes the `consolidated_text` field, each Text element contains **complete context**:
- Customer information, product details, sales information, and conversation summary all in one coherent text block
- Vector embeddings capture full relationships between entities
- RAG queries have access to complete context without information loss

## 🔧 **Generation Scripts**

### Non-Consolidated Data
- **Script**: `create_nonconsolidated_index.py`
- **Index**: `sales-records` (separate fields structure)
- **Fields**: 20+ individual fields (customer_name, product_model, price, etc.)

### Consolidated Data  
- **Script**: `create_consolidated_index.py`
- **Index**: `sales-records` (consolidated structure)
- **Key Field**: `consolidated_text` (contains all context)

## 📋 **Example Comparison**

### Non-Consolidated Record
```json
{
  "customer_name": "Jennifer Martinez",
  "product_model": "SoundSport Free", 
  "price": 149,
  "retailer": "Best Buy",
  "interaction_text": "Customer called to inquire about purchasing..."
}
```
**Problem**: When processed by Unstructured, context between fields is lost.

### Consolidated Record
```json
{
  "consolidated_text": "SALES RECORD - November 15, 2023\n\nCustomer Information:\n- Name: Jennifer Martinez\n- Location: New York, NY\n...\n\nProduct Details:\n- Model: SoundSport Free\n- Price: $149\n...\n\nConversation Summary:\nCustomer Jennifer Martinez contacted Michael Chen about the SoundSport Free..."
}
```
**Solution**: All context preserved in single field for complete RAG understanding.

## 🎯 **Recommendation**

**For RAG Use Cases**: Use the **consolidated structure** to ensure context preservation and optimal embedding quality.

**For Analytics**: Use the **non-consolidated structure** for traditional database queries and business intelligence.

**For Hybrid Approaches**: You can maintain both structures - use consolidated for RAG processing and non-consolidated for analytics dashboards.

## 🚀 **Usage**

1. **Choose your approach** based on use case
2. **Run the appropriate script**:
   - `python create_nonconsolidated_index.py` (separate fields)
   - `python create_consolidated_index.py` (RAG-optimized)
3. **Configure Unstructured Workflow** to process the appropriate field(s)
4. **Verify results** with the corresponding verification scripts

## 📁 **Files in This Directory**

- `nonconsolidated_examples.json` - 3 example records with separate fields
- `consolidated_examples.json` - 3 example records with consolidated text
- `README.md` - This documentation file

Both examples contain the same underlying sales data, just structured differently for different use cases. 