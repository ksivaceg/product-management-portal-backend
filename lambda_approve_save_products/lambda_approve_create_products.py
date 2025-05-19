import json
import os
import logging
import pymongo # MongoDB Driver for DocumentDB
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone
# For DocumentDB, you'll need the CA certificate if not using SRV connection string and connecting from outside VPC (less common for Lambda)
# or if your pymongo version requires it for TLS.
# Typically, for Lambda in the same VPC, direct connection is fine.

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration - Get from Environment Variables
# For production, use AWS Secrets Manager for username/password
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT') # e.g., 'your-cluster-endpoint.docdb.amazonaws.com:27017'
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD') # Consider AWS Secrets Manager
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_COLLECTION_NAME = os.environ.get('DOCDB_COLLECTION_NAME', 'products')

# Path to the Amazon DocumentDB CA certificate (if needed, typically for connections from outside VPC or specific driver configs)
# For Lambda in the same VPC, this might not be strictly necessary if not enforcing TLS with CA validation in a specific way.
# However, it's good practice for ensuring secure connections.
# Download from: https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
# You'll need to package this .pem file with your Lambda deployment package.
# For Lambda Layers or simple Lambda, place it in the root or a known path.
# PEM_PATH = os.environ.get('DOCDB_PEM_PATH', 'global-bundle.pem') # Ensure this file is in your deployment package

# Global variable for the MongoDB client to allow connection reuse
mongo_client = None

def get_db_client():
    global mongo_client
    if mongo_client:
        # Test connection if already initialized (optional, can add overhead)
        try:
            mongo_client.admin.command('ping')
            logger.info("Reusing existing DocumentDB client connection.")
            return mongo_client
        except ConnectionFailure:
            logger.warning("Existing DocumentDB connection failed ping. Re-initializing.")
            mongo_client = None # Force re-initialization

    if not DOCDB_ENDPOINT:
        logger.error("DOCDB_ENDPOINT environment variable not set.")
        raise ValueError("DocumentDB endpoint not configured.")

    # Construct the connection string.
    # Ensure 'global-bundle.pem' is in your Lambda deployment package at the root.
    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info(f"Attempting to connect to DocumentDB using connection string.")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping') # Verify connection
        logger.info("Successfully connected to DocumentDB.")
        mongo_client = client
        return mongo_client
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to DocumentDB: {e}")
        raise
    except Exception as e: # Catch other potential errors like missing pem file or configuration issues
        logger.error(f"An error occurred during DocumentDB client initialization: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        client = get_db_client()
        db = client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_COLLECTION_NAME]
    except Exception as e: # Catch errors from get_db_client
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database: {str(e)}'})
        }

    try:
        body = json.loads(event.get('body', '{}'))
        products_to_save = body.get('products') # Expects a list of product dictionaries
        s3_key_source = body.get('s3Key', 'unknown_source_csv') # Get s3Key for logging import source

        if not products_to_save or not isinstance(products_to_save, list):
            logger.error("'products' array not found or not a list in request body.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Request body must contain a 'products' array."})
            }

        if not products_to_save: # Empty list
            logger.info("Received an empty list of products to save.")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'No products provided to save.', 'productsSaved': 0})
            }

        processed_products = []
        current_timestamp = datetime.now(timezone.utc)

        for product_data in products_to_save:
            if not isinstance(product_data, dict):
                logger.warning(f"Skipping non-dictionary item in products list: {product_data}")
                continue
            
            # Create a new dictionary to avoid modifying the input directly if it's reused
            new_product_doc = product_data.copy()

            # Add/Update metadata
            # Ensure _id is unique. Using Barcode or ProductName is an example.
            # Consider a more robust UUID if these aren't guaranteed unique or always present.
            new_product_doc['_id'] = new_product_doc.get('Barcode') or new_product_doc.get('ProductName') or str(uuid.uuid4())
            new_product_doc['createdAt'] = current_timestamp # Will be set only on insert due to $setOnInsert
            new_product_doc['updatedAt'] = current_timestamp
            new_product_doc['importSource'] = s3_key_source

            # Consolidate measure type attributes
            measure_attributes = {
                "ItemWeight": ("ItemWeightValue", "ItemWeightUnit"),
                "Width": ("WidthValue", "WidthUnit"),
                "Height": ("HeightValue", "HeightUnit"),
                # Add other measure attributes here following the pattern ("AttributeName", "ValueKey", "UnitKey")
            }

            for attr_name, (val_key, unit_key) in measure_attributes.items():
                if val_key in new_product_doc and unit_key in new_product_doc:
                    new_product_doc[attr_name] = {
                        'value': new_product_doc.pop(val_key),
                        'unit': new_product_doc.pop(unit_key)
                    }
            
            # Basic Type Conversion (Example)
            # A more robust solution would use AttributeDefinition types from your 'attributes' collection
            for key, value in new_product_doc.items():
                if isinstance(value, str):
                    # Try to convert to int
                    if value.isdigit():
                        new_product_doc[key] = int(value)
                    else:
                        # Try to convert to float
                        try:
                            new_product_doc[key] = float(value)
                        except ValueError:
                            pass # Keep as string if not a simple number
            
            processed_products.append(new_product_doc)
        
        if not processed_products:
            logger.info("No valid products to save after processing.")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'No valid products to save.', 'productsSaved': 0})
            }

        logger.info(f"Attempting to save {len(processed_products)} products to DocumentDB.")
        
        bulk_operations = []
        for prod_doc in processed_products:
            filter_query = {'_id': prod_doc['_id']}
            
            # Separate fields for $set and $setOnInsert
            # $setOnInsert will only apply if the document is being newly created (upsert=True)
            set_on_insert_fields = {'createdAt': prod_doc['createdAt']}
            
            # Fields for $set will apply on both insert and update
            # Remove createdAt from prod_doc before passing to $set if you only want it on insert
            update_fields = {k: v for k, v in prod_doc.items() if k != 'createdAt'}

            update_query = {
                '$set': update_fields,
                '$setOnInsert': set_on_insert_fields
            }
            bulk_operations.append(pymongo.UpdateOne(filter_query, update_query, upsert=True))

        if not bulk_operations:
            logger.info("No operations to perform after processing products for bulk write.")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'No products with valid identifiers to save.', 'productsSaved': 0})
            }

        result = collection.bulk_write(bulk_operations)
        
        saved_count = result.upserted_count + result.modified_count
        logger.info(f"Successfully saved/updated {saved_count} products. Upserted: {result.upserted_count}, Modified: {result.modified_count}, Matched: {result.matched_count}")

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'message': f'Successfully saved/updated {saved_count} products.', 'productsSaved': saved_count})
        }

    except OperationFailure as e:
        logger.error(f"DocumentDB operation failed: {e.details}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True) # Log full traceback for unexpected errors
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
