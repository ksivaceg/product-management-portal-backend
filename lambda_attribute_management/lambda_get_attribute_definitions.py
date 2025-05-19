import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime # Keep for consistency if used elsewhere, though not strictly for this GET
from bson import ObjectId # If you were to use MongoDB ObjectIds for _id

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
            mongo_client = None

    if not DOCDB_ENDPOINT:
        logger.error("DOCDB_ENDPOINT environment variable not set.")
        raise ValueError("DocumentDB endpoint not configured.")

    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info("Attempting to connect to DocumentDB for retrieving attribute definitions.")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping')
        logger.info("Successfully connected to DocumentDB.")
        mongo_client = client
        return mongo_client
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to DocumentDB: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred during DocumentDB client initialization: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event to get attribute definitions: {json.dumps(event)}")

    try:
        client = get_db_client()
        db = client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_ATTRIBUTES_COLLECTION_NAME]
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database or setup collection: {str(e)}'})
        }

    try:
        logger.info(f"Fetching all attribute definitions from collection: {DOCDB_ATTRIBUTES_COLLECTION_NAME}")
        
        # Fetch all documents from the attribute_definitions collection
        # You might want to add sorting here, e.g., by name or createdAt
        attribute_definitions_cursor = collection.find({}).sort("name", pymongo.ASCENDING)
        
        attribute_definitions_list = list(attribute_definitions_cursor)

        # Convert datetime objects to ISO format strings for JSON serialization
        for attr_def in attribute_definitions_list:
            if 'createdAt' in attr_def and isinstance(attr_def['createdAt'], datetime):
                attr_def['createdAt'] = attr_def['createdAt'].isoformat()
            if 'updatedAt' in attr_def and isinstance(attr_def['updatedAt'], datetime):
                attr_def['updatedAt'] = attr_def['updatedAt'].isoformat()
            # If _id is an ObjectId (not the case if derived from name as string), convert it:
            # if '_id' in attr_def and isinstance(attr_def['_id'], ObjectId):
            #    attr_def['_id'] = str(attr_def['_id'])

        logger.info(f"Successfully retrieved {len(attribute_definitions_list)} attribute definitions.")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' # IMPORTANT: Restrict in production
            },
            'body': json.dumps({
                'message': 'Attribute definitions retrieved successfully.',
                'data': attribute_definitions_list
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
