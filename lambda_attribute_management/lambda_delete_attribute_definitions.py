import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
# bson.ObjectId is not strictly needed if _id is a string derived from name
# from bson import ObjectId # Only if _id can be an actual ObjectId

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration - Get from Environment Variables
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD')
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_ATTRIBUTES_COLLECTION_NAME = os.environ.get('DOCDB_ATTRIBUTES_COLLECTION_NAME', 'attribute_definitions')

mongo_client = None

def get_db_client():
    global mongo_client
    if mongo_client:
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

    # Ensure 'global-bundle.pem' is in your Lambda deployment package at the root.
    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info("Attempting to connect to DocumentDB for attributes management.")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping') # Verify connection
        logger.info("Successfully connected to DocumentDB.")
        mongo_client = client
        return mongo_client
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to DocumentDB: {e}")
        raise
    except Exception as e: # Catch other potential errors
        logger.error(f"An error occurred during DocumentDB client initialization: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event to delete attribute definition: {json.dumps(event)}")

    try:
        client = get_db_client()
        db = client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_ATTRIBUTES_COLLECTION_NAME]
    except Exception as e: # Catch errors from get_db_client
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database or setup collection: {str(e)}'})
        }

    try:
        # attribute_id will come from the path parameter
        path_parameters = event.get('pathParameters', {})
        attribute_id_to_delete = path_parameters.get('attributeId') # e.g., "material_type"

        if not attribute_id_to_delete:
            logger.error("Missing 'attributeId' in path parameters.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing 'attributeId' in path."})
            }

        logger.info(f"Attempting to delete attribute definition with ID: '{attribute_id_to_delete}'")
        
        # Perform the delete operation
        # The _id for attribute definitions is currently derived from its name (e.g., "material_type")
        result = collection.delete_one({'_id': attribute_id_to_delete})

        if result.deleted_count == 1:
            logger.info(f"Successfully deleted attribute definition with ID: '{attribute_id_to_delete}'")
            return {
                'statusCode': 200, # Or 204 No Content if you prefer not to send a body
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': f"Attribute definition '{attribute_id_to_delete}' deleted successfully."})
            }
        else:
            logger.warning(f"Attribute definition with ID: '{attribute_id_to_delete}' not found for deletion.")
            return {
                'statusCode': 404, # Not Found
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Attribute definition with ID '{attribute_id_to_delete}' not found."})
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
