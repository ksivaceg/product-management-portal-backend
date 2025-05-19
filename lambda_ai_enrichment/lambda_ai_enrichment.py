import json
import os
import logging
import pymongo # For DocumentDB
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone
import re # For parsing attribute name from prompt in mock

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD')
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_PRODUCTS_COLLECTION_NAME = os.environ.get('DOCDB_COLLECTION_NAME', 'products')
DOCDB_ATTRIBUTES_COLLECTION_NAME = os.environ.get('DOCDB_ATTRIBUTES_COLLECTION_NAME', 'attribute_definitions')

mongo_client_db = None 

# Attributes to target for enrichment
TARGET_ENRICHMENT_ATTRIBUTE_TYPES = ['short_text', 'long_text', 'rich_text']
# Max length for short_text suggestions (mocked)
SHORT_TEXT_MAX_LENGTH_MOCK = int(os.environ.get('SHORT_TEXT_MAX_LENGTH_MOCK', 100)) 


def get_db_client():
    global mongo_client_db
    if mongo_client_db:
        try:
            mongo_client_db.admin.command('ping')
            logger.info("Reusing existing DocumentDB client connection.")
            return mongo_client_db
        except ConnectionFailure:
            logger.warning("Existing DocumentDB connection failed ping. Re-initializing.")
            mongo_client_db = None
    if not DOCDB_ENDPOINT or not DOCDB_USERNAME or not DOCDB_PASSWORD:
        logger.error("DocumentDB connection details are not fully configured.")
        raise ValueError("DocumentDB connection details not configured.")
    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping')
        logger.info("Successfully connected to DocumentDB.")
        mongo_client_db = client
        return mongo_client_db
    except Exception as e:
        logger.error(f"Error initializing DocumentDB client: {e}")
        raise

def get_attribute_definitions(db_client):
    attributes_map = {}
    try:
        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_ATTRIBUTES_COLLECTION_NAME]
        for attr_def in collection.find({}):
            if '_id' in attr_def and 'name' in attr_def:
                attributes_map[attr_def['name']] = attr_def
        logger.info(f"Fetched {len(attributes_map)} attribute definitions.")
    except Exception as e:
        logger.error(f"Failed to fetch attribute definitions: {e}")
    return attributes_map

def generate_mock_suggestion(prompt_text, attribute_name_for_mock="Unknown Attribute", attribute_type_for_mock="unknown"):
    """
    Generates a mock AI suggestion based on the attribute name and type.
    """
    logger.info(f"Generating MOCK suggestion for attribute: {attribute_name_for_mock} (type: {attribute_type_for_mock})")
    
    mock_value = f"[MOCK] This is a suggested {attribute_type_for_mock} for {attribute_name_for_mock}."

    name_lower = attribute_name_for_mock.lower()
    if "description" in name_lower:
        mock_value = f"[MOCK] An excellent, engaging, and detailed mock description for '{attribute_name_for_mock}', perfect for attracting customers and boosting sales. It highlights unique selling points."
    elif "color" in name_lower:
        mock_value = "[MOCK] Mystic Teal"
    elif "material" in name_lower:
        mock_value = "[MOCK] Eco-Friendly Bamboo Composite"
    elif "key features" in name_lower:
         mock_value = "[MOCK] Feature A: High Durability; Feature B: User-Friendly Interface; Feature C: Extended Warranty."
    
    # Simulate short_text truncation for the mock value
    if attribute_type_for_mock == 'short_text' and len(mock_value) > SHORT_TEXT_MAX_LENGTH_MOCK: 
         mock_value = mock_value[:SHORT_TEXT_MAX_LENGTH_MOCK].rsplit(' ', 1)[0] + "..." if ' ' in mock_value[:SHORT_TEXT_MAX_LENGTH_MOCK] else mock_value[:SHORT_TEXT_MAX_LENGTH_MOCK]

    logger.info(f"MOCK response generated: {mock_value}")
    return mock_value.strip()


def lambda_handler(event, context):
    logger.info(f"Received event for AI product enrichment preview (MOCK): {json.dumps(event)}")

    db_client = None
    try:
        db_client = get_db_client()
    except Exception as db_conn_err:
        logger.error(f"CRITICAL: Could not connect to DocumentDB. Error: {db_conn_err}")
        return {'statusCode': 503, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': f'Failed to connect to database: {str(db_conn_err)}'})}

    try:
        body = json.loads(event.get('body', '{}'))
        product_ids_to_enrich = body.get('productIds') 

        if not product_ids_to_enrich or not isinstance(product_ids_to_enrich, list):
            return {'statusCode': 400, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': "Request body must contain a 'productIds' array."})}
        if not product_ids_to_enrich:
            return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'message': 'No product IDs provided for enrichment.', 'enrichedProductsPreview': []})}

        attribute_definitions = get_attribute_definitions(db_client)
        if not attribute_definitions:
            return {'statusCode': 500, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'Failed to load attribute definitions for enrichment.'})}

        products_collection = db_client[DOCDB_DATABASE_NAME][DOCDB_PRODUCTS_COLLECTION_NAME]
        
        enriched_products_preview = []
        processed_product_ids = set()

        for product_id_str in product_ids_to_enrich:
            if product_id_str in processed_product_ids: continue
            processed_product_ids.add(product_id_str)

            product = products_collection.find_one({'_id': product_id_str})
            if not product:
                logger.warning(f"Product with ID '{product_id_str}' not found. Skipping enrichment.")
                continue

            logger.info(f"Preparing enrichment preview for product ID: {product_id_str}, Name: {product.get('name', product.get('ProductName', 'N/A'))}")
            
            product_preview = json.loads(json.dumps(product, default=str)) 
            ai_suggestions_made = {} 

            # Base prompt info is not strictly needed for mock, but kept for structure
            base_prompt_info = f"Product Name: {product.get('name', product.get('ProductName', 'N/A'))}\n"
            base_prompt_info += f"Brand: {product.get('brand', product.get('Brand', 'N/A'))}\n"
            
            for attr_name, attr_def in attribute_definitions.items():
                current_value = product.get(attr_name)
                should_enrich = (
                    attr_def.get('type') in TARGET_ENRICHMENT_ATTRIBUTE_TYPES and
                    (current_value is None or (isinstance(current_value, str) and not current_value.strip()))
                )
                
                if should_enrich:
                    attr_description_for_prompt = attr_def.get('description', f"the {attr_name} of the product")
                    attr_type_for_prompt = attr_def.get('type')
                    
                    # Construct a dummy prompt string, as the mock function might use it for context
                    dummy_prompt = (
                        f"Generate value for attribute '{attr_name}' (type: {attr_type_for_prompt}) "
                        f"described as '{attr_description_for_prompt}' for product: {base_prompt_info}"
                    )

                    logger.info(f"Product ID {product_id_str}: Generating MOCK suggestion for attribute '{attr_name}'")
                    ai_generated_value = generate_mock_suggestion(dummy_prompt, attr_name, attr_type_for_prompt) 

                    if ai_generated_value:
                        product_preview[attr_name] = ai_generated_value 
                        ai_suggestions_made[attr_name] = ai_generated_value
                        logger.info(f"Product ID {product_id_str}: MOCK AI suggested for '{attr_name}': '{ai_generated_value[:70]}...'")
            
            if ai_suggestions_made: 
                if '_id' in product_preview and not isinstance(product_preview['_id'], str): 
                    product_preview['_id'] = str(product_preview['_id'])
                for dt_key in ['createdAt', 'updatedAt']:
                    if dt_key in product_preview and isinstance(product_preview[dt_key], datetime):
                        product_preview[dt_key] = product_preview[dt_key].isoformat()
                
                enriched_products_preview.append({
                    '_id': product_id_str, 
                    'originalProductName': product.get('name', product.get('ProductName', 'N/A')), 
                    'enrichedProductData': product_preview, 
                    'aiSuggestions': ai_suggestions_made 
                })
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': f'Enrichment preview (MOCKED AI) generated for {len(enriched_products_preview)} out of {len(product_ids_to_enrich)} requested products.',
                'enrichedProductsPreview': enriched_products_preview
            })
        }

    except OperationFailure as e:
        logger.error(f"DocumentDB operation failed during enrichment: {e.details}")
        return {'statusCode': 500, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})}
    except Exception as e:
        logger.error(f"Unexpected error during enrichment: {e}", exc_info=True)
        return {'statusCode': 500, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})}

