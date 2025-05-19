import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
import csv
import io 
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure, DuplicateKeyError
from datetime import datetime, timezone 
from bson import ObjectId # In case you use ObjectIds for _id in attributes or jobs later

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client
s3_client = boto3.client('s3')

# DocumentDB Configuration
DOCDB_ENDPOINT = os.environ.get('DOCDB_ENDPOINT')
DOCDB_USERNAME = os.environ.get('DOCDB_USERNAME')
DOCDB_PASSWORD = os.environ.get('DOCDB_PASSWORD')
DOCDB_DATABASE_NAME = os.environ.get('DOCDB_DATABASE_NAME', 'product_portal')
DOCDB_ATTRIBUTES_COLLECTION_NAME = os.environ.get('DOCDB_ATTRIBUTES_COLLECTION_NAME', 'attribute_definitions')
DOCDB_JOBS_COLLECTION_NAME = os.environ.get('DOCDB_JOBS_COLLECTION_NAME', 'processing_jobs')

# S3 bucket for storing processed results
PROCESSED_RESULTS_BUCKET = os.environ.get('PROCESSED_RESULTS_BUCKET')
PROCESSED_RESULTS_PREFIX = os.environ.get('PROCESSED_RESULTS_PREFIX', 'processed-files/')

mongo_client_db = None 

SHORT_TEXT_MAX_LENGTH = int(os.environ.get('SHORT_TEXT_MAX_LENGTH', 50))

def get_db_client():
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

def log_initial_job_status(db_client, job_id, s3_bucket, s3_key, original_file_name, submitted_at_iso):
    try:
        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_JOBS_COLLECTION_NAME]
        current_time_iso = datetime.now(timezone.utc).isoformat()
        job_document_fields = {
            's3Bucket': s3_bucket, 's3Key': s3_key, 'originalFileName': original_file_name,
            'status': 'PROCESSING', 'processingStartedAt': current_time_iso, 'updatedAt': current_time_iso,
        }
        result = collection.update_one(
            {'_id': job_id},
            {'$set': job_document_fields, '$setOnInsert': {'_id': job_id, 'submittedAt': submitted_at_iso}},
            upsert=True
        )
        if result.upserted_id: logger.info(f"Job {job_id}: Logged new job with status PROCESSING.")
        elif result.matched_count > 0: logger.info(f"Job {job_id}: Updated existing job status to PROCESSING.")
        else: logger.warning(f"Job {job_id}: Upsert for initial status did not match or insert.")
        return True
    except Exception as e:
        logger.error(f"Job {job_id}: Failed to log initial/processing job status: {e}", exc_info=True)
        return False

def update_job_status(db_client, job_id, status, result_s3_key=None, error_details=None):
    try:
        db = db_client[DOCDB_DATABASE_NAME]
        collection = db[DOCDB_JOBS_COLLECTION_NAME]
        update_fields = {'status': status, 'updatedAt': datetime.now(timezone.utc).isoformat()}
        if result_s3_key: update_fields['resultS3Key'] = result_s3_key
        if error_details:
            update_fields['errorDetails'] = str(error_details) if not isinstance(error_details, (dict, list, str)) else error_details
        result = collection.update_one({'_id': job_id},{'$set': update_fields})
        if result.matched_count == 0: logger.warning(f"Job ID {job_id} not found for status update to {status}.")
        else: logger.info(f"Job {job_id}: Updated status to {status}.")
    except Exception as e: logger.error(f"Job {job_id}: Failed to update job status to {status}: {e}", exc_info=True)

def get_defined_attributes_map(db_client):
    attributes_map = {}; logger.info("Fetching attribute definitions...")
    try:
        db = db_client[DOCDB_DATABASE_NAME]; collection = db[DOCDB_ATTRIBUTES_COLLECTION_NAME]
        for attr_def in collection.find({}):
            if 'name' in attr_def: attributes_map[attr_def['name']] = attr_def
        logger.info(f"Fetched {len(attributes_map)} attribute definitions.")
    except Exception as e: logger.error(f"Failed to fetch attribute definitions: {e}")
    return attributes_map

def validate_value(value_str, header_name, attr_def, row_number):
    attr_type = attr_def.get('type'); attr_name = attr_def.get('name', header_name); is_required = attr_def.get('isRequired', False)
    if value_str is None or str(value_str).strip() == "": return (False, None, f"Row {row_number+1}, Column '{attr_name}': Value is required but is empty.") if is_required else (True, None, None)
    cleaned_value = str(value_str).strip()
    if attr_type == 'short_text':
        if len(cleaned_value) > SHORT_TEXT_MAX_LENGTH: return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': Value exceeds max length of {SHORT_TEXT_MAX_LENGTH}."
        return True, cleaned_value, None
    elif attr_type in ['long_text', 'rich_text']: return True, cleaned_value, None
    elif attr_type == 'number':
        try: num_value = float(cleaned_value); return True, int(num_value) if num_value.is_integer() else num_value, None
        except ValueError: return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': Value '{cleaned_value}' is not a valid number."
    elif attr_type == 'single_select':
        options = attr_def.get('options', []);
        if not options: return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': No options defined."
        if cleaned_value not in options: return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': Value '{cleaned_value}' not in allowed options: {options}."
        return True, cleaned_value, None
    elif attr_type == 'multiple_select':
        options = attr_def.get('options', []);
        if not options: return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': No options defined."
        selected_values = [v.strip() for v in cleaned_value.split(';') if v.strip()]
        if not selected_values and is_required : return False, cleaned_value, f"Row {row_number+1}, Column '{attr_name}': Value is required."
        invalid_selections = [sv for sv in selected_values if sv not in options]
        if invalid_selections: return False, selected_values, f"Row {row_number+1}, Column '{attr_name}': Values {invalid_selections} are not in allowed options: {options}."
        return True, selected_values, None
    elif attr_type == 'measure': return True, cleaned_value, None
    return True, cleaned_value, None


def lambda_handler(event, context):
    logger.info(f"Received SQS event: {json.dumps(event)}") # Log the whole SQS event

    for record_index, record in enumerate(event.get('Records', [])):
        logger.info(f"Processing record {record_index + 1} of {len(event.get('Records', []))}")
        job_id = None # Initialize job_id for this record's scope
        try:
            message_body_str = record.get('body')
            if not message_body_str:
                logger.error(f"Record {record_index + 1}: SQS record missing 'body'. Skipping record.")
                continue
            
            logger.info(f"Record {record_index + 1}: Raw SQS message body string: {message_body_str}")
            payload = json.loads(message_body_str)
            
            # --- Enhanced Logging for Payload ---
            logger.info(f"Record {record_index + 1}: Parsed SQS message payload (type: {type(payload)}): {json.dumps(payload)}")
            if isinstance(payload, dict):
                logger.info(f"Record {record_index + 1}: Keys in parsed payload: {list(payload.keys())}")
                for key_check in ['jobId', 's3Bucket', 's3Key']:
                    is_present = key_check in payload
                    value_retrieved = payload.get(key_check)
                    logger.info(f"Record {record_index + 1}: Payload check for '{key_check}': Present={is_present}, Value='{value_retrieved}' (Type: {type(value_retrieved)})")
            # --- End Enhanced Logging ---

            job_id = payload.get('jobId') # Now job_id is set for this record's context
            s3_bucket = payload.get('s3Bucket')
            s3_key = payload.get('s3Key')
            original_file_name = payload.get('originalFileName', os.path.basename(s3_key or "unknown_file"))
            submitted_at_iso = payload.get('submittedAt', datetime.now(timezone.utc).isoformat()) 
            
            db_client = None
            try:
                db_client = get_db_client()
            except Exception as db_conn_err:
                logger.error(f"CRITICAL: Job {job_id}: Could not connect to DocumentDB. Error: {db_conn_err}")
                raise # Re-raise to make Lambda fail this record processing, SQS will retry/DLQ
            
            if not job_id or not s3_bucket or not s3_key:
                err_msg = "Missing jobId, s3Bucket, or s3Key in SQS message payload."
                logger.error(f"Job {job_id or 'UNKNOWN'}: {err_msg}") # Use 'UNKNOWN' if job_id is None
                if db_client and job_id: 
                    update_job_status(db_client, job_id, "FAILED", error_details={'error': err_msg})
                continue # Skip this record, move to next if any in batch

            if not log_initial_job_status(db_client, job_id, s3_bucket, s3_key, original_file_name, submitted_at_iso):
                logger.error(f"Job {job_id}: CRITICAL - Failed to log initial 'PROCESSING' status. Halting processing for this job.")
                continue 

            if not PROCESSED_RESULTS_BUCKET:
                err_msg = "PROCESSED_RESULTS_BUCKET environment variable not set."
                logger.error(f"Job {job_id}: {err_msg}")
                update_job_status(db_client, job_id, "FAILED", error_details={'error': err_msg})
                continue

            defined_attributes_map = get_defined_attributes_map(db_client)
            if not defined_attributes_map:
                err_msg = "Could not retrieve attribute definitions for validation."
                logger.error(f"Job {job_id}: {err_msg}")
                update_job_status(db_client, job_id, "FAILED", error_details={'error': err_msg})
                continue
                
            defined_attribute_names_set = set(defined_attributes_map.keys())
            logger.info(f"Job {job_id}: Attempting to process file from Bucket: {s3_bucket}, Key: {s3_key}")

            try:
                s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                file_content = s3_response['Body'].read().decode('utf-8')
                logger.info(f"Job {job_id}: Successfully downloaded S3 object. Content length: {len(file_content)}")
            except ClientError as e:
                err_msg = f"Error downloading file from S3 for job {job_id}: {e}"
                logger.error(err_msg)
                error_detail_for_db = {'error': 'S3 download error', 's3_error_code': e.response.get('Error',{}).get('Code')}
                if e.response['Error']['Code'] == 'NoSuchKey': error_detail_for_db['error'] = 'File not found in S3.'
                update_job_status(db_client, job_id, "FAILED", error_details=error_detail_for_db)
                raise # Let SQS handle retry for this record

            products_for_result = []
            validation_errors_list = []
            actual_headers_from_csv = []
            ignored_headers_from_csv = []
            headers_for_result = []
            processing_outcome_message = ""
            final_job_status = "COMPLETED_WITH_ISSUES" 

            try:
                csv_file = io.StringIO(file_content); reader = csv.DictReader(csv_file)
                if not reader.fieldnames: processing_outcome_message = "CSV file is empty or has no headers."
                else:
                    actual_headers_from_csv = reader.fieldnames
                    for header in actual_headers_from_csv:
                        if header in defined_attribute_names_set: headers_for_result.append(header)
                        else: ignored_headers_from_csv.append(header)
                    if not headers_for_result: processing_outcome_message = "No columns in the CSV match defined attributes."
                    else:
                        logger.info(f"Job {job_id}: Valid CSV Headers: {headers_for_result}, Ignored: {ignored_headers_from_csv}")
                        total_rows_read = 0
                        for i, row_data_dict in enumerate(reader):
                            total_rows_read +=1; current_row_product_data = {}; is_current_row_valid = True; row_specific_errors = []
                            for header_name in headers_for_result:
                                cell_value_str = row_data_dict.get(header_name)
                                attribute_definition = defined_attributes_map[header_name]
                                is_cell_valid, cleaned_value, error_msg = validate_value(cell_value_str, header_name, attribute_definition, i)
                                if not is_cell_valid: is_current_row_valid = False; 
                                if error_msg: row_specific_errors.append(error_msg) 
                                current_row_product_data[header_name] = cleaned_value
                            for header_name in headers_for_result: 
                                attribute_definition = defined_attributes_map[header_name]
                                if attribute_definition.get('isRequired', False) and \
                                   (current_row_product_data.get(header_name) is None or str(current_row_product_data.get(header_name,"")).strip() == ""):
                                    is_current_row_valid = False; err_msg = f"Row {i+1}, Column '{header_name}': Value is required but is missing or empty."
                                    if not any(f"Column '{header_name}'" in specific_err and "Value is required" in specific_err for specific_err in row_specific_errors if specific_err): row_specific_errors.append(err_msg)
                            if is_current_row_valid: products_for_result.append(current_row_product_data)
                            else:
                                if row_specific_errors: validation_errors_list.extend(row_specific_errors)
                        processing_outcome_message = f"Processed {total_rows_read} rows. Found {len(products_for_result)} valid products and {len(validation_errors_list)} validation issues."
                        if not validation_errors_list and not ignored_headers_from_csv and total_rows_read > 0 and len(products_for_result) == total_rows_read :
                            final_job_status = "COMPLETED"                 
            except csv.Error as e: err_msg = f"Failed to parse CSV file structure for job {job_id}: {str(e)}"; logger.error(err_msg); update_job_status(db_client, job_id, "FAILED", error_details={'error': err_msg}); continue 
            except Exception as e: err_msg = f"Error during CSV data processing for job {job_id}: {str(e)}"; logger.error(err_msg, exc_info=True); update_job_status(db_client, job_id, "FAILED", error_details={'error': err_msg}); continue

            result_data_to_save = {
                'jobId': job_id, 's3Key': s3_key, 'processingTimestamp': datetime.now(timezone.utc).isoformat(),
                'message': processing_outcome_message, 'fileName': original_file_name,
                'headers': headers_for_result, 'products': products_for_result,
                'originalHeaders': actual_headers_from_csv, 'ignoredHeaders': ignored_headers_from_csv,
                'validationErrors': validation_errors_list
            }
            result_s3_key_path = f"{PROCESSED_RESULTS_PREFIX.rstrip('/')}/{job_id}-result.json"
            try:
                s3_client.put_object(Bucket=PROCESSED_RESULTS_BUCKET, Key=result_s3_key_path, Body=json.dumps(result_data_to_save, indent=2), ContentType='application/json')
                logger.info(f"Job {job_id}: Successfully saved processing results to S3: s3://{PROCESSED_RESULTS_BUCKET}/{result_s3_key_path}")
                update_job_status(db_client, job_id, final_job_status, result_s3_key=result_s3_key_path)
            except Exception as s3_put_e:
                err_msg = f"Job {job_id}: Failed to save processing results to S3: {s3_put_e}"; logger.error(err_msg, exc_info=True)
                update_job_status(db_client, job_id, "FAILED", error_details={'error': "Failed to save results to S3.", 's3Error': str(s3_put_e)})
                raise s3_put_e 

            logger.info(f"Job {job_id}: Processing finished for this SQS message.")

        except Exception as main_record_processing_error:
            logger.error(f"Job {job_id or 'UNKNOWN_JOB_ID_IN_ERROR'}: Unhandled error during processing of SQS record: {main_record_processing_error}", exc_info=True)
            if db_client and job_id: # job_id might be None if parsing payload failed very early
                 update_job_status(db_client, job_id, "FAILED", error_details={'error': 'Unhandled exception during processing.', 'exception': str(main_record_processing_error)})
            raise main_record_processing_error # Re-raise so SQS can handle retry/DLQ

    return {'status': 'Batch processing completed (or attempted).'}
