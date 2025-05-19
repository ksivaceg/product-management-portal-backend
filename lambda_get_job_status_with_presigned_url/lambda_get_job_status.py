import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime # For BSON datetime deserialization
from bson import ObjectId # If job _ids were ObjectIds

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client for generating pre-signed URLs
s3_client = boto3.client('s3')

# DocumentDB Configuration
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD')
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_JOBS_COLLECTION_NAME = os.environ.get('DOCDB_JOBS_COLLECTION_NAME', 'processing_jobs')

# S3 bucket where results are stored (needed for pre-signed URL generation context)
# This Lambda needs to know which bucket the resultS3Key refers to.
# It's assumed to be the same as PROCESSED_RESULTS_BUCKET from the worker Lambda.
PROCESSED_RESULTS_BUCKET = os.environ.get('PROCESSED_RESULTS_BUCKET')
URL_EXPIRATION_SECONDS_GET_RESULT = int(os.environ.get('URL_EXPIRATION_SECONDS_GET_RESULT', 300)) # 5 minutes

mongo_client_db = None 

def get_db_client():
    global mongo_client_db
    if mongo_client_db:
        try:
            mongo_client_db.admin.command('ping')
            logger.info("Reusing existing DocumentDB client connection for job status.")
            return mongo_client_db
        except ConnectionFailure:
            logger.warning("Existing DocumentDB job status connection failed ping. Re-initializing.")
            mongo_client_db = None

    if not DOCDB_ENDPOINT or not DOCDB_USERNAME or not DOCDB_PASSWORD:
        logger.error("DocumentDB connection details for job status are not fully configured.")
        raise ValueError("DocumentDB job status connection details not configured.")

    connection_string = f"mongodb://{DOCDB_USERNAME}:{DOCDB_PASSWORD}@{DOCDB_ENDPOINT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    logger.info(f"Attempting to connect to DocumentDB for job status: {DOCDB_ENDPOINT}")
    try:
        client = pymongo.MongoClient(connection_string)
        client.admin.command('ping')
        logger.info("Successfully connected to DocumentDB for job status.")
        mongo_client_db = client
        return mongo_client_db
    except Exception as e:
        logger.error(f"Error initializing DocumentDB client for job status: {e}")
        raise

def generate_presigned_get_url(bucket_name, object_key, expiration_seconds):
    if not bucket_name or not object_key:
        logger.warning("Cannot generate pre-signed GET URL: bucket_name or object_key is missing.")
        return None
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration_seconds,
            HttpMethod='GET'
        )
        logger.info(f"Generated pre-signed GET URL for s3://{bucket_name}/{object_key}")
        return url
    except ClientError as e:
        logger.error(f"Failed to generate pre-signed GET URL for s3://{bucket_name}/{object_key}: {e}")
        return None

def lambda_handler(event, context):
    logger.info(f"Received event to get job status: {json.dumps(event)}")

    db_client = None
    try:
        db_client = get_db_client()
    except Exception as db_conn_err:
        logger.error(f"CRITICAL: Could not connect to DocumentDB to get job status. Error: {db_conn_err}")
        return {
            'statusCode': 503, 
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Failed to connect to database: {str(db_conn_err)}'})
        }

    try:
        path_parameters = event.get('pathParameters', {})
        job_id = path_parameters.get('jobId')

        if not job_id:
            logger.error("Missing 'jobId' in path parameters.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing 'jobId' in path."})
            }

        logger.info(f"Attempting to fetch job status for Job ID: '{job_id}'")
        
        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_JOBS_COLLECTION_NAME]
        
        job_document = collection.find_one({'_id': job_id})

        if job_document:
            # Serialize datetime objects
            for key in ['submittedAt', 'processingStartedAt', 'updatedAt']:
                if key in job_document and isinstance(job_document[key], datetime):
                    job_document[key] = job_document[key].isoformat()
            
            # If job is completed and has a resultS3Key, generate a pre-signed GET URL for it
            if job_document.get('status') in ['COMPLETED', 'COMPLETED_WITH_ISSUES'] and job_document.get('resultS3Key'):
                if PROCESSED_RESULTS_BUCKET:
                    presigned_url = generate_presigned_get_url(
                        PROCESSED_RESULTS_BUCKET, 
                        job_document['resultS3Key'], 
                        URL_EXPIRATION_SECONDS_GET_RESULT
                    )
                    if presigned_url:
                        job_document['resultDownloadUrl'] = presigned_url
                    else:
                        logger.warning(f"Job {job_id}: Could not generate download URL for resultS3Key: {job_document['resultS3Key']}")
                else:
                    logger.warning(f"Job {job_id}: PROCESSED_RESULTS_BUCKET env var not set. Cannot generate download URL.")


            logger.info(f"Job {job_id}: Found status - {job_document.get('status')}")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps(job_document) 
            }
        else:
            logger.warning(f"Job {job_id}: Not found in '{DOCDB_JOBS_COLLECTION_NAME}' collection.")
            return {
                'statusCode': 404, 
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Job with ID '{job_id}' not found."})
            }

    except OperationFailure as e:
        logger.error(f"Job {job_id}: DocumentDB operation failed: {e.details}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Database operation error: {str(e.details)}'})
        }
    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
