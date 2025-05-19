import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure, DuplicateKeyError
from datetime import datetime, timezone
import uuid # Only if not deriving _id from name

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
    
    logger.info("Attempting to connect to DocumentDB for attributes management.")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping')
        logger.info("Successfully connected to DocumentDB.")
        mongo_client = client
        return mongo_client
    except Exception as e:
        logger.error(f"An error occurred during DocumentDB client initialization: {e}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received event for creating attribute definition: {json.dumps(event)}")

    try:
        client = get_db_client()
        db = client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_ATTRIBUTES_COLLECTION_NAME]
        # Consider creating index on 'name' once, outside the handler, if names should be unique
        # and _id is not derived from name.
        # Example: if not collection.index_information().get('name_1'):
        #              collection.create_index("name", unique=True, background=True)
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database or setup collection: {str(e)}'})
        }

    try:
        body = json.loads(event.get('body', '{}'))
        
        attribute_name = body.get('name')
        attribute_type = body.get('type')
        attribute_description = body.get('description', '') # New description field, defaults to empty string

        if not attribute_name or not attribute_type:
            logger.error("Missing 'name' or 'type' for attribute definition.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing required fields: 'name' and 'type'."})
            }

        allowed_types = ['short_text', 'long_text', 'rich_text', 'number', 
                         'single_select', 'multiple_select', 'measure']
        if attribute_type not in allowed_types:
            logger.error(f"Invalid attribute type: {attribute_type}. Allowed types are: {allowed_types}")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Invalid attribute type '{attribute_type}'. Allowed types: {', '.join(allowed_types)}."})
            }

        # Sanitize name to create a unique, filesystem-like _id.
        # Example: "Product Name" -> "product_name"
        # This makes attribute names effectively unique.
        attribute_id = attribute_name.strip().lower().replace(" ", "_").replace("-", "_")
        # Remove any non-alphanumeric characters except underscore
        attribute_id = "".join(c if c.isalnum() or c == '_' else "" for c in attribute_id)


        attribute_definition = {
            '_id': attribute_id, # Use sanitized name as _id for uniqueness
            'name': attribute_name.strip(),
            'type': attribute_type,
            'description': attribute_description.strip(), # Store the description
            'createdAt': datetime.now(timezone.utc),
            'updatedAt': datetime.now(timezone.utc)
        }

        if attribute_type in ['single_select', 'multiple_select']:
            options = body.get('options')
            if not options or not isinstance(options, list) or not all(isinstance(opt, str) for opt in options):
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': f"For type '{attribute_type}', 'options' must be an array of strings."})
                }
            attribute_definition['options'] = [opt.strip() for opt in options if opt.strip()]
        
        if attribute_type == 'measure':
            unit = body.get('unit')
            if not unit or not isinstance(unit, str) or not unit.strip():
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': "For type 'measure', 'unit' string is required."})
                }
            attribute_definition['unit'] = unit.strip()
        
        attribute_definition['isFilterable'] = body.get('isFilterable', True) 
        attribute_definition['isSortable'] = body.get('isSortable', True)   
        attribute_definition['isRequired'] = body.get('isRequired', False) 

        logger.info(f"Attempting to insert attribute definition: {attribute_definition}")
        
        try:
            result = collection.insert_one(attribute_definition)
            logger.info(f"Successfully inserted attribute definition with ID: {result.inserted_id}")
            
            # Prepare response by converting datetime objects
            attribute_definition_response = attribute_definition.copy()
            attribute_definition_response['createdAt'] = attribute_definition_response['createdAt'].isoformat()
            attribute_definition_response['updatedAt'] = attribute_definition_response['updatedAt'].isoformat()

            return {
                'statusCode': 201, 
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({
                    'message': 'Attribute definition created successfully.',
                    'attribute': attribute_definition_response
                })
            }
        except DuplicateKeyError:
            logger.error(f"Attribute with name '{attribute_name}' (ID: '{attribute_definition['_id']}') already exists.")
            return {
                'statusCode': 409, 
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"An attribute with the name '{attribute_name}' (or its sanitized version for ID) already exists."})
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
