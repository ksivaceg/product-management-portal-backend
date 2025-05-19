import json
import os
import logging
import boto3
import uuid
from datetime import datetime, timezone 

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SQS client
sqs_client = boto3.client('sqs')

# SQS Queue URL for the processing tasks - Get from Environment Variable
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
UPLOAD_BUCKET_NAME_ENV = os.environ.get('UPLOAD_BUCKET_NAME')


def lambda_handler(event, context):
    logger.info(f"Received event to initiate file processing via SQS: {json.dumps(event)}")

    if not SQS_QUEUE_URL:
        logger.error("SQS_QUEUE_URL environment variable not set.")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': 'Server configuration error: SQS Queue not defined.'})
        }

    try:
        body = json.loads(event.get('body', '{}'))
        s3_bucket_from_payload = body.get('s3Bucket')
        s3_key = body.get('s3Key')
        
        s3_bucket_to_process = UPLOAD_BUCKET_NAME_ENV if UPLOAD_BUCKET_NAME_ENV else s3_bucket_from_payload
        
        if not s3_bucket_to_process:
             logger.error("Missing 's3Bucket' in request body and UPLOAD_BUCKET_NAME env var not set.")
             return {'statusCode': 400, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': "Missing S3 bucket information."})}

        if not s3_key:
            logger.error("Missing 's3Key' in request body.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': "Missing required parameter: 's3Key'"})
            }

        job_id = str(uuid.uuid4())
        original_file_name = os.path.basename(s3_key) 
        submitted_at_iso = datetime.now(timezone.utc).isoformat()

        logger.info(f"Generated Job ID: {job_id} for S3 key: {s3_key}, Original Filename: {original_file_name}")

        message_payload = {
            'jobId': job_id,
            's3Bucket': s3_bucket_to_process,
            's3Key': s3_key,
            'originalFileName': original_file_name,
            'maxPreviewRows': body.get('maxPreviewRows', 50), 
            'submittedAt': submitted_at_iso 
        }

        logger.info(f"Sending message to SQS Queue: {SQS_QUEUE_URL} with payload: {json.dumps(message_payload)}")
        
        # --- MODIFIED SECTION for Standard SQS Queues ---
        # Removed MessageGroupId and MessageDeduplicationId as they are for FIFO queues
        send_message_params = {
            'QueueUrl': SQS_QUEUE_URL,
            'MessageBody': json.dumps(message_payload)
        }
        
        # If your SQS_QUEUE_URL ends with .fifo, then it's a FIFO queue
        # and you MUST provide MessageGroupId. MessageDeduplicationId is optional for FIFO
        # but good for content-based deduplication.
        if SQS_QUEUE_URL.endswith(".fifo"):
            send_message_params['MessageGroupId'] = job_id # Or another appropriate group ID
            # send_message_params['MessageDeduplicationId'] = job_id # Optional for FIFO

        response = sqs_client.send_message(**send_message_params)
        # --- END MODIFIED SECTION ---
        
        logger.info(f"Message sent to SQS. Message ID: {response.get('MessageId')}. Job ID: {job_id}. Returning 202 to client.")

        return {
            'statusCode': 202, 
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' 
            },
            'body': json.dumps({
                'message': 'File processing request accepted and queued successfully. Checking status...',
                'jobId': job_id 
            })
        }

    except Exception as e:
        logger.error(f"Unexpected error in SQS initiation Lambda: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'An unexpected server error occurred: {str(e)}'})
        }
