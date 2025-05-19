import json
import os
import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId # If you use MongoDB ObjectIds as _id
import math
from datetime import datetime # <--- Added missing import

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DocumentDB Configuration - Get from Environment Variables
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD') # Consider AWS Secrets Manager
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_COLLECTION_NAME = os.environ.get('DOCDB_COLLECTION_NAME', 'products')
# PEM_PATH = os.environ.get('DOCDB_PEM_PATH', 'global-bundle.pem') # Ensure this file is in your deployment package

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
    
    logger.info(f"Attempting to connect to DocumentDB using connection string.")
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

def parse_query_param_value(value_str):
    """
    Tries to convert a string query parameter value to int or float if possible.
    """
    if value_str is None:
        return None
    try:
        return int(value_str)
    except ValueError:
        try:
            return float(value_str)
        except ValueError:
            return value_str # Keep as string if not a number

def build_filter_query(query_params):
    """
    Builds a MongoDB filter query from API Gateway query parameters.
    Example query_params: {'Brand': 'Acme', 'Color': 'Red', 'ItemWeight.value[gte]': '100'}
    """
    filter_query = {}
    if not query_params:
        return filter_query

    for key, value in query_params.items():
        # Skip pagination/sorting params or other reserved params
        if key in ['page', 'limit', 'sortBy', 'sortOrder', '_', 'nextToken']: # '_' is often added by API Gateway
            continue

        # Handle range queries like field[gte]=value, field[lte]=value, etc.
        # Example: 'ItemWeight.value[gte]' -> field='ItemWeight.value', op='$gte'
        if '[' in key and key.endswith(']'):
            field_name, operator_part = key.split('[', 1)
            operator = operator_part[:-1] # Remove trailing ']'
            
            mongo_operator = f"${operator}" # e.g., $gte, $lte, $gt, $lt, $ne
            
            # Ensure operator is one of the supported ones to prevent injection
            supported_range_ops = ['$gte', '$gt', '$lte', '$lt', '$ne']
            if mongo_operator not in supported_range_ops:
                logger.warning(f"Unsupported operator: {operator} for field {field_name}. Skipping.")
                continue
            
            parsed_value = parse_query_param_value(value)
            if field_name not in filter_query:
                filter_query[field_name] = {}
            filter_query[field_name][mongo_operator] = parsed_value
        
        # Handle exact matches or $in for comma-separated values
        elif ',' in value: # Handle 'field=value1,value2' as an $in query
            values_list = [parse_query_param_value(v.strip()) for v in value.split(',')]
            filter_query[key] = {'$in': values_list}
        else: # Exact match
            # For exact matches, try to convert to number if possible, otherwise use as string
            # If the field in DB is a number, string match won't work.
            parsed_value = parse_query_param_value(value)
            filter_query[key] = parsed_value
            
    logger.info(f"Constructed filter query: {filter_query}")
    return filter_query


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        client = get_db_client()
        db = client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_COLLECTION_NAME]
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database: {str(e)}'})
        }

    try:
        # API Gateway passes query string parameters in 'queryStringParameters'
        # when using Lambda proxy integration.
        query_params = event.get('queryStringParameters')
        if query_params is None:
            query_params = {} # Ensure query_params is a dict even if no params are passed

        logger.info(f"Received query parameters: {query_params}")

        # Pagination parameters
        try:
            page = int(query_params.get('page', 1))
            limit = int(query_params.get('limit', 10)) # Default limit to 10 items
            if page < 1: page = 1
            if limit < 1: limit = 1
            if limit > 100: limit = 100 # Max limit to prevent abuse
        except ValueError:
            logger.warning("Invalid page or limit parameters. Using defaults.")
            page = 1
            limit = 10
        
        skip = (page - 1) * limit

        # Sorting parameters
        sort_by = query_params.get('sortBy')
        sort_order_str = query_params.get('sortOrder', 'asc').lower()
        sort_order = pymongo.ASCENDING if sort_order_str == 'asc' else pymongo.DESCENDING
        
        sort_criteria = []
        if sort_by:
            sort_criteria.append((sort_by, sort_order))
        # else: # Default sort if needed, e.g., by _id or createdAt
        #    sort_criteria.append(('_id', pymongo.ASCENDING))


        # Filtering parameters
        filter_query = build_filter_query(query_params)

        # Fetch products
        products_cursor = collection.find(filter_query).skip(skip).limit(limit)
        if sort_criteria:
            products_cursor = products_cursor.sort(sort_criteria)
        
        products_list = list(products_cursor)

        # Convert ObjectId to string for JSON serialization if your _ids are ObjectIds
        # Convert datetime objects to ISO format strings
        for product in products_list:
            if '_id' in product and isinstance(product['_id'], ObjectId):
                product['_id'] = str(product['_id'])
            if 'createdAt' in product and isinstance(product['createdAt'], datetime): # Now datetime is defined
                 product['createdAt'] = product['createdAt'].isoformat()
            if 'updatedAt' in product and isinstance(product['updatedAt'], datetime): # Now datetime is defined
                 product['updatedAt'] = product['updatedAt'].isoformat()


        # Get total count of documents matching the filter (for pagination)
        total_products_matching_filter = collection.count_documents(filter_query)
        total_pages = math.ceil(total_products_matching_filter / limit) if limit > 0 else 0 # Handle limit=0 case

        logger.info(f"Found {total_products_matching_filter} products matching filter. Returning page {page} of {total_pages} with {len(products_list)} items.")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' # IMPORTANT: Restrict in production
            },
            'body': json.dumps({
                'message': 'Products retrieved successfully',
                'data': products_list,
                'pagination': {
                    'currentPage': page,
                    'totalPages': total_pages,
                    'totalItems': total_products_matching_filter,
                    'itemsPerPage': limit,
                    'hasNextPage': page < total_pages,
                    'hasPrevPage': page > 1
                }
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

