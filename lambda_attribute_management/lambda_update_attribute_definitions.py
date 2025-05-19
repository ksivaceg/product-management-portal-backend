import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure, WriteConcernError
from datetime import datetime, timezone
# bson.ObjectId is not strictly needed if _id is a string derived from name,
# but good to have if you might use ObjectIds elsewhere.
from bson import ObjectId # Not currently used as _id is string, but kept for potential future use
from urllib.parse import unquote_plus # For decoding path parameters if they contain URL-encoded chars

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
    logger.info(f"Received event to update attribute definition: {json.dumps(event)}")

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

    attribute_id_raw = None
    attribute_id_decoded = None
    try:
        path_parameters = event.get('pathParameters', {})
        attribute_id_raw = path_parameters.get('attributeId') 

        if not attribute_id_raw:
            logger.error("Missing 'attributeId' in path parameters.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing 'attributeId' in path."})
            }
        
        attribute_id_decoded = unquote_plus(attribute_id_raw) # Decode path parameter
        logger.info(f"Raw attributeId from path: '{attribute_id_raw}', Decoded attributeId for query: '{attribute_id_decoded}'")


        body = json.loads(event.get('body', '{}'))
        
        update_payload = {}
        # Added 'description' to the list of fields that can be updated
        allowed_to_update = ['options', 'unit', 'isFilterable', 'isSortable', 'isRequired', 'description']
        
        has_updates = False
        for key in allowed_to_update:
            if key in body:
                # For description, strip whitespace. For booleans, ensure they are bool.
                if key == 'description':
                    update_payload[key] = str(body[key]).strip() if body[key] is not None else ''
                elif key in ['isFilterable', 'isSortable', 'isRequired']:
                    if not isinstance(body[key], bool):
                        logger.error(f"Invalid value for '{key}'. Must be a boolean (true/false).")
                        return {
                            'statusCode': 400,
                            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                            'body': json.dumps({'error': f"Field '{key}' must be a boolean."})
                        }
                    update_payload[key] = body[key]
                else: # For 'options' and 'unit'
                    update_payload[key] = body[key]
                has_updates = True
        
        if not has_updates:
            logger.info("No updatable fields provided in the request body.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "No updatable fields provided."})
            }

        update_payload['updatedAt'] = datetime.now(timezone.utc)

        existing_attribute = collection.find_one({'_id': attribute_id_decoded})
        if not existing_attribute:
            logger.error(f"Attribute with ID '{attribute_id_decoded}' not found.")
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Attribute with ID '{attribute_id_decoded}' not found."})
            }
        
        current_attribute_type = existing_attribute.get('type')

        if 'options' in update_payload:
            if current_attribute_type not in ['single_select', 'multiple_select']:
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': f"'options' field is not applicable for attribute type '{current_attribute_type}'."})
                }
            options = update_payload['options']
            if not isinstance(options, list) or not all(isinstance(opt, str) for opt in options):
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': "'options' must be an array of strings."})
                }
            update_payload['options'] = [opt.strip() for opt in options if opt.strip()]

        if 'unit' in update_payload:
            if current_attribute_type != 'measure':
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': f"'unit' field is not applicable for attribute type '{current_attribute_type}'."})
                }
            unit = update_payload['unit']
            if not isinstance(unit, str) or not unit.strip():
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': "'unit' must be a non-empty string."})
                }
            update_payload['unit'] = unit.strip()
        
        logger.info(f"Attempting to update attribute ID '{attribute_id_decoded}' with payload: {update_payload}")
        
        result = collection.update_one(
            {'_id': attribute_id_decoded},
            {'$set': update_payload}
        )

        if result.matched_count == 0: # Should have been caught by find_one earlier
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Attribute with ID '{attribute_id_decoded}' not found."})
            }
        
        logger.info(f"Successfully processed update for attribute ID '{attribute_id_decoded}'. Matched: {result.matched_count}, Modified: {result.modified_count}")
        
        updated_attribute_doc = collection.find_one({'_id': attribute_id_decoded})
        if updated_attribute_doc: 
            # Convert datetime objects for JSON response
            if 'createdAt' in updated_attribute_doc and isinstance(updated_attribute_doc['createdAt'], datetime):
                updated_attribute_doc['createdAt'] = updated_attribute_doc['createdAt'].isoformat()
            if 'updatedAt' in updated_attribute_doc and isinstance(updated_attribute_doc['updatedAt'], datetime):
                updated_attribute_doc['updatedAt'] = updated_attribute_doc['updatedAt'].isoformat()
        else: 
            logger.error(f"Failed to retrieve attribute '{attribute_id_decoded}' after update operation.")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Failed to retrieve attribute after update.'})
            }

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': 'Attribute definition updated successfully.',
                'attribute': updated_attribute_doc
            })
        }

    except OperationFailure as e:
        logger.error(f"Attribute ID '{attribute_id_decoded or attribute_id_raw}': DocumentDB operation failed: {e.details}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})
        }
    except Exception as e:
        logger.error(f"Attribute ID '{attribute_id_decoded or attribute_id_raw}': Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
