import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Generates a pre-signed S3 URL for uploading a file.

    Expects a JSON payload in the event body with:
    - "fileName": The name of the file to be uploaded.
    - "contentType": (Optional) The MIME type of the file. If not provided, S3 might try to guess.
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # --- CORRECTED SECTION: Retrieving environment variables ---
        # Standard practice: Environment variable name is UPLOAD_BUCKET_NAME
        # Its VALUE in Lambda config would be 'my-product-portal-uploads' (or your actual bucket name)
        upload_bucket_name_env_var = 'UPLOAD_BUCKET_NAME' # The NAME of the environment variable
        upload_bucket_name = os.environ.get(upload_bucket_name_env_var)
        # --- END CORRECTED SECTION ---

        s3_key_prefix = os.environ.get('S3_KEY_PREFIX', 'user-uploads/') # Default to 'user-uploads/'
        url_expiration_seconds = int(os.environ.get('URL_EXPIRATION_SECONDS', 3600)) # Default to 1 hour

        if not upload_bucket_name:
            # --- CORRECTED SECTION: Error message reflects the env var name ---
            logger.error(f"{upload_bucket_name_env_var} environment variable not set.")
            # --- END CORRECTED SECTION ---
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*' # Adjust for your frontend's origin in production
                },
                'body': json.dumps({'error': 'Server configuration error: Missing bucket name configuration.'})
            }

        # Parse the request body
        try:
            body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError:
            logger.error("Invalid JSON in request body.")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'Invalid request body: Must be valid JSON.'})
            }

        file_name = body.get('fileName')
        content_type = body.get('contentType')

        if not file_name:
            logger.error("Missing 'fileName' in request body.")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': "Missing required parameter: 'fileName'"})
            }

        unique_file_id = str(uuid.uuid4())
        sanitized_file_name_part = "".join(c if c.isalnum() or c in ['.', '-', '_'] else '_' for c in file_name)
        
        object_key = f"{s3_key_prefix.rstrip('/')}/{unique_file_id}/{sanitized_file_name_part}"
        
        # --- CORRECTED SECTION: Using the correct variable for bucket name ---
        s3_params = {
            'Bucket': upload_bucket_name, # Use the variable that holds the bucket name
            'Key': object_key,
        }
        # --- END CORRECTED SECTION ---
        if content_type:
            s3_params['ContentType'] = content_type
            
        # --- CORRECTED SECTION: Using the correct variable in logger ---
        logger.info(f"Generating pre-signed URL for Bucket: {upload_bucket_name}, Key: {object_key}, ContentType: {content_type}")
        # --- END CORRECTED SECTION ---

        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params=s3_params,
            ExpiresIn=url_expiration_seconds,
            HttpMethod='PUT'
        )

        logger.info(f"Successfully generated pre-signed URL: {presigned_url}")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'uploadUrl': presigned_url,
                's3Key': object_key
            })
        }

    except ClientError as e:
        logger.error(f"Boto3 ClientError: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'Failed to generate pre-signed URL: {str(e)}'})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'An unexpected error occurred: {str(e)}'})
        }
