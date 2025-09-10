#!/usr/bin/env python3
"""
Create Consolidated Elasticsearch Index for RAG
Combines multiple fields into a single text field to preserve context during Unstructured processing.

This approach ensures that when Unstructured processes the data:
1. All context (product, customer, location, etc.) is preserved in each Text element
2. The resulting embeddings contain complete contextual information
3. RAG queries can access full context without losing field relationships
"""

import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

# Load environment variables from base directory (one level up)
load_dotenv(dotenv_path="../.env")

class ConsolidatedBoseSalesDataGenerator:
    """Generate consolidated sales data optimized for RAG processing"""
    
    def __init__(self):
        self.fake = Faker(['en_US'])
        Faker.seed(42)  # For reproducible data
        
        # Bose product data
        self.products = {
            'SoundSport': {
                'models': ['SoundSport Free', 'SoundSport Wireless', 'SoundSport Pulse'],
                'price_range': (129, 199),
                'category': 'Sports Earbuds',
                'features': ['sweat-resistant', 'secure fit', 'wireless', 'noise isolation']
            },
            'OpenAudio': {
                'models': ['OpenAudio Sport', 'OpenAudio Ultra', 'OpenAudio Pro'],
                'price_range': (149, 249),
                'category': 'Open-Ear Audio',
                'features': ['open-ear design', 'situational awareness', 'comfortable fit', 'premium audio']
            },
            'QuietComfort': {
                'models': ['QuietComfort 45', 'QuietComfort Ultra', 'QuietComfort Earbuds'],
                'price_range': (199, 429),
                'category': 'Noise Cancelling',
                'features': ['world-class noise cancellation', 'premium comfort', 'long battery life', 'crystal clear calls']
            }
        }
        
        # Rich entity data for NER
        self.retailers = [
            "Best Buy", "Target", "Amazon", "Walmart", "Costco", "B&H Photo",
            "Guitar Center", "Sam's Club", "Newegg", "Adorama", "Crutchfield"
        ]
        
        self.sales_reps = [
            "Jennifer Martinez", "Michael Chen", "Sarah Johnson", "David Rodriguez",
            "Emily Wilson", "Robert Taylor", "Lisa Anderson", "James Thompson",
            "Maria Garcia", "Christopher Lee", "Amanda Davis", "Daniel Brown"
        ]
        
        self.regions = [
            "Northeast", "Southeast", "Midwest", "Southwest", "West Coast",
            "Pacific Northwest", "Mountain West", "Great Lakes", "Mid-Atlantic", "Gulf Coast"
        ]
        
        self.cities = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
            "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "Austin, TX",
            "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Charlotte, NC", "Seattle, WA",
            "Denver, CO", "Washington, DC", "Boston, MA", "Nashville, TN", "Detroit, MI"
        ]
        
        self.interaction_types = [
            "purchase_inquiry", "product_comparison", "pricing_discussion", 
            "sales_consultation", "order_processing", "upsell_opportunity",
            "customer_preferences", "warranty_inquiry", "bulk_order_request",
            "promotional_campaign", "seasonal_sale", "loyalty_program_enrollment"
        ]

    def generate_consolidated_record(self) -> dict:
        """Generate a single sales record with consolidated text field"""
        
        # Generate individual field data
        product_line = random.choice(list(self.products.keys()))
        product_info = self.products[product_line]
        model = random.choice(product_info['models'])
        price = random.randint(*product_info['price_range'])
        features = random.sample(product_info['features'], k=random.randint(1, 3))
        
        customer_name = self.fake.name()
        sales_rep = random.choice(self.sales_reps)
        retailer = random.choice(self.retailers)
        city = random.choice(self.cities)
        region = random.choice(self.regions)
        interaction_type = random.choice(self.interaction_types)
        
        # Generate timestamp from 2016 onwards
        start_date = datetime(2016, 1, 1)
        random_date = self.fake.date_time_between(start_date=start_date, end_date='now')
        
        customer_segment = random.choice(["Consumer", "Business", "Education", "Government"])
        sales_channel = random.choice(["Direct", "Retail Partner", "Online", "Phone"])
        lead_source = random.choice(["Website", "Advertisement", "Referral", "Trade Show", "Cold Call"])
        deal_stage = random.choice(["Prospect", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"])
        
        # Create contextual interaction based on type
        interaction_details = self._generate_interaction_text(
            interaction_type, customer_name, sales_rep, model, price, retailer, city, features
        )
        
        # **KEY CHANGE: Create consolidated text field with ALL context**
        consolidated_text = f"""
SALES RECORD - {random_date.strftime('%B %d, %Y')}

Customer Information:
- Name: {customer_name}
- Location: {city}
- Segment: {customer_segment}
- Lead Source: {lead_source}

Product Details:
- Product Line: {product_line}
- Model: {model}
- Category: {product_info['category']}
- Price: ${price}
- Key Features: {', '.join(features)}

Sales Information:
- Sales Representative: {sales_rep}
- Retailer: {retailer}
- Region: {region}
- Channel: {sales_channel}
- Deal Stage: {deal_stage}
- Interaction Type: {interaction_type.replace('_', ' ').title()}

Conversation Summary:
{interaction_details}

Temporal Context:
- Date: {random_date.strftime('%B %d, %Y')}
- Quarter: Q{random_date.month//3 + 1} {random_date.year}
- Day of Week: {random_date.strftime('%A')}

Revenue Information:
- Unit Price: ${price}
- Potential Deal Value: ${price * random.randint(1, 5)}
- Priority: {random.choice(['High', 'Medium', 'Low'])}
        """.strip()
        
        # Return document with consolidated text + minimal metadata for Elasticsearch
        return {
            "id": str(uuid.uuid4()),
            "timestamp": random_date.isoformat(),
            "document_type": "sales_record",
            "product_line": product_line,  # Keep for filtering/aggregation
            "region": region,  # Keep for filtering/aggregation
            "consolidated_text": consolidated_text,  # **This is what Unstructured will process**
            "record_date": random_date.strftime('%Y-%m-%d'),
            "quarter": f"Q{random_date.month//3 + 1}",
            "year": random_date.year
        }
    
    def _generate_interaction_text(self, interaction_type, customer_name, sales_rep, model, price, retailer, city, features):
        """Generate detailed interaction text based on type"""
        
        interaction_templates = {
            "purchase_inquiry": f"Customer {customer_name} from {city} contacted {sales_rep} to inquire about purchasing the {model}. The customer was particularly interested in the {', '.join(features[:2])} features. {sales_rep} provided detailed product specifications and quoted ${price}. Customer mentioned they had seen similar products at {retailer} but was impressed with the Bose quality and features.",
            
            "product_comparison": f"{sales_rep} conducted a comprehensive product comparison session with {customer_name}. The customer was deciding between the {model} and competitor products. Key selling points discussed included {', '.join(features)} which differentiate Bose from competitors. The ${price} price point was justified through superior audio quality and build reliability. Customer appreciated the detailed comparison and is considering the purchase.",
            
            "sales_consultation": f"In-depth consultation with {customer_name} in the {city} area. {sales_rep} assessed customer needs and recommended the {model} based on their lifestyle and audio preferences. Highlighted features included {', '.join(features)}. Discussed ${price} pricing structure and available financing options. Customer showed strong interest and requested follow-up information.",
            
            "order_processing": f"Order successfully processed for {customer_name}: {random.randint(1, 3)} units of {model} at ${price} each. {sales_rep} confirmed shipping details to {city} and explained warranty coverage. Customer opted for expedited shipping and was provided with tracking information. Partnership with {retailer} ensured competitive pricing and reliable delivery.",
            
            "promotional_campaign": f"Q{random.randint(1,4)} promotional outreach to {customer_name}. {sales_rep} presented special pricing on {model} - limited time offer at ${price - random.randint(10, 30)} (regularly ${price}). Emphasized exclusive features: {', '.join(features)}. Customer expressed interest and plans to visit {retailer} location this weekend to experience the product firsthand.",
            
            "warranty_inquiry": f"{customer_name} contacted {sales_rep} regarding warranty coverage for their {model} purchased from {retailer}. {sales_rep} reviewed the comprehensive warranty terms and explained the repair/replacement process. Customer was satisfied with the coverage and expressed loyalty to the Bose brand. Discussed potential upgrade paths and new features in latest models.",
            
            "upsell_opportunity": f"{sales_rep} identified upsell opportunity with existing customer {customer_name}. Customer currently owns an older Bose model and was introduced to the {model} with enhanced features: {', '.join(features)}. The ${price} upgrade investment was positioned as worthwhile for the improved experience. Customer is considering the upgrade and requested a demo unit.",
            
            "bulk_order_request": f"Corporate customer {customer_name} from {city} requested bulk pricing for {model} units. {sales_rep} prepared enterprise quotation for {random.randint(10, 50)} units at discounted rate. Discussed features relevant to business use: {', '.join(features)}. Partnership with {retailer} enables volume discounts and dedicated support. Proposal under review by customer's procurement team."
        }
        
        return interaction_templates.get(
            interaction_type, 
            f"{sales_rep} assisted {customer_name} with {interaction_type.replace('_', ' ')} regarding {model}. Discussed ${price} pricing and key features: {', '.join(features)}. Customer interaction was positive and follow-up scheduled."
        )

def main():
    """Create consolidated Elasticsearch index optimized for RAG"""
    print("🚀 Creating Consolidated Elasticsearch Index for RAG")
    print("=" * 60)
    
    # Initialize
    api_key = os.getenv('ELASTIC_API_KEY')
    es = Elasticsearch(
        "https://2371b9a1d2ad40c590fd1e22652a8236.us-central1.gcp.cloud.es.io:443",
        api_key=api_key,
        request_timeout=60
    )
    
    index_name = "sales-records"  # Use same index name as API key permissions
    num_records = 100
    
    try:
        # Delete existing index if it exists
        if es.indices.exists(index=index_name):
            print(f"🗑️ Deleting existing index: {index_name}")
            es.indices.delete(index=index_name)
        
        # Create index with simple mapping optimized for consolidated text
        print(f"🔧 Creating consolidated index: {index_name}")
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "document_type": {"type": "keyword"},
                    "product_line": {"type": "keyword"},
                    "region": {"type": "keyword"},
                    "consolidated_text": {
                        "type": "text",
                        "analyzer": "standard"  # This is the field Unstructured will process
                    },
                    "record_date": {"type": "date"},
                    "quarter": {"type": "keyword"},
                    "year": {"type": "integer"}
                }
            }
        }
        
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Created index with consolidated text mapping")
        
        # Generate and index data
        generator = ConsolidatedBoseSalesDataGenerator()
        print(f"🔄 Generating {num_records} consolidated sales records...")
        
        records = []
        for i in range(num_records):
            if i % 25 == 0 and i > 0:
                print(f"  Generated {i}/{num_records} records...")
            records.append(generator.generate_consolidated_record())
        
        print(f"✅ Generated {len(records)} consolidated records")
        
        # Bulk index
        print("📤 Bulk indexing consolidated records...")
        actions = []
        for record in records:
            actions.append({
                "_index": index_name,
                "_id": record["id"],
                "_source": record
            })
        
        from elasticsearch.helpers import bulk
        success_count, failed_items = bulk(es, actions, chunk_size=50)
        
        print(f"✅ Successfully indexed {success_count} consolidated records")
        
        # Verify
        count_response = es.count(index=index_name)
        total_docs = count_response['count']
        print(f"📊 Total documents in consolidated index: {total_docs}")
        
        # Show sample
        sample_response = es.search(
            index=index_name,
            body={"size": 1, "_source": ["consolidated_text", "product_line", "region"]},
        )
        
        if sample_response['hits']['hits']:
            sample = sample_response['hits']['hits'][0]['_source']
            print(f"\n📋 Sample Consolidated Record:")
            print(f"   Product Line: {sample['product_line']}")
            print(f"   Region: {sample['region']}")
            print(f"   Consolidated Text Preview:")
            text_preview = sample['consolidated_text'][:300] + "..."
            print(f"   {text_preview}")
        
        print(f"\n🎉 CONSOLIDATED INDEX READY!")
        print(f"✅ Index: {index_name}")
        print(f"✅ Records: {total_docs}")
        print(f"✅ Each record contains ALL context in 'consolidated_text' field")
        print(f"✅ Ready for Unstructured Workflow processing with full context preservation")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 