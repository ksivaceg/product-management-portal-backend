import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone
# from bson import ObjectId # Only if your _id can be an ObjectId

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration - Get from Environment Variables
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD') # Consider AWS Secrets Manager
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_PRODUCTS_COLLECTION_NAME = os.environ.get('DOCDB_COLLECTION_NAME', 'products') 

mongo_client_db = None 

def get_db_client():
    """
    Initializes and returns a DocumentDB client.
    Reuses existing connection if available.
    """
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
        logger.error("DocumentDB connection details (endpoint, username, password) are not fully configured.")
        raise ValueError("DocumentDB connection details not configured.")

    # Ensure 'global-bundle.pem' is in your Lambda deployment package at the root.
    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info(f"Attempting to connect to DocumentDB: {DOCDB_ENDPOINT}")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping') # Verify connection
        logger.info("Successfully connected to DocumentDB.")
        mongo_client_db = client
        return mongo_client_db
    except Exception as e:
        logger.error(f"Error initializing DocumentDB client: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event to save enriched products: {json.dumps(event)}")

    db_client = None
    try:
        db_client = get_db_client()
    except Exception as db_conn_err:
        logger.error(f"CRITICAL: Could not connect to DocumentDB. Error: {db_conn_err}")
        return {
            'statusCode': 503, # Service Unavailable
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database: {str(db_conn_err)}'})
        }

    try:
        body = json.loads(event.get('body', '{}'))
        products_to_save = body.get('products') 

        if not products_to_save or not isinstance(products_to_save, list):
            logger.error("'products' array not found or not a list in request body.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Request body must contain a 'products' array."})
            }

        if not products_to_save: 
            logger.info("Received an empty list of products to save.")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'No products provided to save.', 'productsUpdated': 0})
            }

        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_PRODUCTS_COLLECTION_NAME]
        
        bulk_operations = []
        current_timestamp_iso = datetime.now(timezone.utc).isoformat() 
        updated_product_ids = []

        for product_data in products_to_save:
            if not isinstance(product_data, dict) or '_id' not in product_data:
                logger.warning(f"Skipping invalid product data item (missing _id or not a dict): {product_data}")
                continue

            product_id = product_data['_id']
            
            # Prepare the document for the $set operation.
            # We explicitly remove 'createdAt' to avoid conflict with $setOnInsert.
            # 'updatedAt' will always be set to the current time for any update/upsert.
            update_doc_for_set = product_data.copy()
            update_doc_for_set['updatedAt'] = current_timestamp_iso
            
            # Store the original createdAt from payload if it exists, otherwise use current time for $setOnInsert
            # This ensures that if the UI sends back a createdAt (e.g., from an existing document),
            # that value is used if the document is being inserted for the first time.
            # If the document already exists, $setOnInsert for createdAt will be ignored.
            original_or_new_created_at = product_data.get('createdAt', current_timestamp_iso)
            
            # Remove createdAt from the fields to be set by $set, as it's handled by $setOnInsert
            if 'createdAt' in update_doc_for_set:
                del update_doc_for_set['createdAt']
            
            set_on_insert_fields = {'createdAt': original_or_new_created_at}
            
            bulk_operations.append(
                pymongo.UpdateOne(
                    {'_id': product_id},
                    {
                        '$set': update_doc_for_set, 
                        '$setOnInsert': set_on_insert_fields 
                    },
                    upsert=True 
                )
            )
            updated_product_ids.append(product_id)

        if not bulk_operations:
            logger.info("No valid product operations to perform.")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'No valid products to update.', 'productsUpdated': 0})
            }

        logger.info(f"Attempting to bulk update/upsert {len(bulk_operations)} products in DocumentDB.")
        result = collection.bulk_write(bulk_operations)
        
        actual_updated_count = result.modified_count + result.upserted_count 
        logger.info(f"Bulk write result: Matched={result.matched_count}, Modified={result.modified_count}, Upserted={result.upserted_count}")

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': f'Successfully updated {actual_updated_count} products.',
                'productsUpdated': actual_updated_count,
                'updatedProductIds': updated_product_ids 
            })
        }

    except OperationFailure as e:
        logger.error(f"DocumentDB operation failed: {e.details}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
