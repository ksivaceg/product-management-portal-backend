import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from urllib.parse import unquote_plus # Import for URL decoding

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration - Get from Environment Variables
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD')
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

    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info(f"Attempting to connect to DocumentDB: {DOCDB_ENDPOINT}")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping') 
        logger.info("Successfully connected to DocumentDB.")
        mongo_client_db = client
        return mongo_client_db
    except Exception as e:
        logger.error(f"Error initializing DocumentDB client: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event to delete product: {json.dumps(event)}")

    db_client = None
    product_id_to_delete_raw = None # For logging the raw path param
    product_id_to_delete_decoded = None # For the query

    try:
        db_client = get_db_client()
    except Exception as db_conn_err:
        logger.error(f"CRITICAL: Could not connect to DocumentDB to delete product. Error: {db_conn_err}")
        return {
            'statusCode': 503, 
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database: {str(db_conn_err)}'})
        }

    try:
        path_parameters = event.get('pathParameters', {})
        product_id_to_delete_raw = path_parameters.get('productId') 

        if not product_id_to_delete_raw:
            logger.error("Missing 'productId' in path parameters.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing 'productId' in path."})
            }

        # --- ADDED URL DECODING STEP ---
        product_id_to_delete_decoded = unquote_plus(product_id_to_delete_raw)
        logger.info(f"Raw productId from path: '{product_id_to_delete_raw}', Decoded productId for query: '{product_id_to_delete_decoded}'")
        # --- END ADDED URL DECODING STEP ---
        
        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_PRODUCTS_COLLECTION_NAME]
        
        query_filter = {'_id': product_id_to_delete_decoded} # Use the decoded ID for the query
        
        result = collection.delete_one(query_filter)

        if result.deleted_count == 1:
            logger.info(f"Successfully deleted product with ID: '{product_id_to_delete_decoded}'")
            return {
                'statusCode': 200, 
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': f"Product with ID '{product_id_to_delete_decoded}' deleted successfully."})
            }
        else:
            logger.warning(f"Product with ID: '{product_id_to_delete_decoded}' not found for deletion (using decoded ID). Raw path param was '{product_id_to_delete_raw}'.")
            return {
                'statusCode': 404, 
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Product with ID '{product_id_to_delete_decoded}' not found."})
            }

    except OperationFailure as e:
        logger.error(f"Product ID '{product_id_to_delete_decoded or product_id_to_delete_raw}': DocumentDB operation failed: {e.details}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})
        }
    except Exception as e:
        logger.error(f"Product ID '{product_id_to_delete_decoded or product_id_to_delete_raw}': Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
