# product-management-portal-backend
This is the backend for Product management portal 

## Project Description

This project is a web-based platform designed to help users manage product information (SKUs) and enrich it using AI. The system allows users to import product data from CSV/Excel files, define custom attributes for their products, and then leverage AI to automatically populate or enhance these attributes. The core goal is to transform basic product information (like name, images, brand, barcode) into a richer, more complete dataset suitable for e-commerce or other business needs.

This system addresses the challenge of incomplete product data by providing tools for:
- Bulk import of existing product information.
- Flexible attribute definition to match diverse product types.
- AI-driven enrichment to automatically generate or suggest values for product attributes.
- A user interface to manage attributes, view products, and oversee the enrichment process.

## Features Implemented

* **Backend (AWS Serverless):**
    * **Attribute Management APIs (CRUD):**
        * `POST /attributes`: Create a new attribute definition (includes description).
        * `GET /attributes`: Retrieve all attribute definitions.
        * `PUT /attributes/{attributeId}`: Update an attribute definition (description, flags, options, unit).
        * `DELETE /attributes/{attributeId}`: Delete an attribute definition.
    * **Product Data APIs:**
        * `GET /products`: Retrieve products with server-side filtering, sorting, and pagination.
        * `DELETE /products/{productId}`: Delete a specific product.
        * `POST /products/approve`: Save products (parsed from CSV and user-approved) to DocumentDB.
    * **Asynchronous File Import & Validation Workflow:**
        * `POST /uploads/presigned-url`: Lambda to generate S3 pre-signed URL for client-side uploads.
        * `POST /uploads/process-file`: Lambda (`lambda_initiate_processing`) to:
            * Accept S3 key of uploaded file.
            * Send a message to an SQS queue with job details.
            * Return `202 Accepted` with a `jobId`.
        * Worker Lambda (`lambda_process_s3_file_v2`) triggered by SQS:
            * Logs initial job status to DocumentDB (`processing_jobs` collection).
            * Fetches attribute definitions from DocumentDB.
            * Downloads and parses the CSV from S3.
            * Validates CSV headers and data rows against defined attributes (type, required, options, length for short_text).
            * Saves detailed processing results (valid products, ignored headers, validation errors) as a JSON file to a separate S3 results bucket.
            * Updates job status in DocumentDB ("COMPLETED", "COMPLETED_WITH_ISSUES", "FAILED") with a link to the S3 results file.
    * **Job Status Polling API:**
        * `GET /processing-jobs/{jobId}`: Lambda (`lambda_get_job_status_with_presigned_url`) to:
            * Fetch job status from the `processing_jobs` collection.
            * If job is complete, generate a pre-signed GET URL for the S3 results file and include it in the response.
    * **AI Enrichment Suggestion API (Mocked/OpenAI):**
        * `POST /products/enrich`: Lambda (`lambda_ai_enrich_openai_alt`) to:
            * Fetch selected products and attribute definitions.
            * Generate mock AI suggestions for empty text-based attributes (or call OpenAI if API key is configured).
            * Return a preview of products with AI suggestions.
    * **Save Approved Enriched Products API:**
        * `POST /products/bulk-update` (or `PUT /products/bulk-update` as per API Gateway config): Lambda (`lambda_save_enriched_products`) to:
            * Receive product data (with user-approved AI suggestions).
            * Update the products in DocumentDB.

## Tech Stack

* **Backend (AWS Serverless):**
    * **API Gateway:** REST APIs as the frontend interface.
    * **AWS Lambda:** Python runtime for business logic.
        * Libraries: `boto3` (AWS SDK), `pymongo` (for DocumentDB), `csv`, `openai` (optional for live AI).
    * **Amazon S3:**
        * Storage for uploaded CSV/Excel files.
        * Storage for processed JSON result files.
    * **Amazon DocumentDB (with MongoDB compatibility):**
        * `products` collection: Stores product data.
        * `attribute_definitions` collection: Stores user-defined attribute schemas.
        * `processing_jobs` collection: Tracks status of asynchronous file processing.
    * **Amazon SQS (Simple Queue Service):** Decouples file processing initiation from the actual processing task, improving API responsiveness.
    * **Amazon Bedrock / OpenAI API (Optional for Live AI):** For AI-powered data enrichment. The current implementation defaults to a mock if live AI is not configured.
    * **IAM:** For managing permissions between AWS services.
    * **CloudWatch:** For logging and monitoring.
    * **VPC (Virtual Private Cloud):** For network isolation and secure communication between Lambda and DocumentDB.
        * VPC Endpoints (for SQS, S3, and potentially Bedrock/Secrets Manager) for private connectivity.

## Backend Architecture Overview

The backend is designed with a serverless-first approach on AWS:
1.  **API Gateway** serves as the entry point for all client requests.
2.  Each API endpoint is integrated with an **AWS Lambda** function that contains the specific business logic.
3.  **Amazon S3** is used for storing uploaded raw files and the JSON results of file processing. Client-side uploads to S3 are facilitated by pre-signed URLs generated by a Lambda function.
4.  **Amazon DocumentDB** is the primary database, storing product information, attribute definitions, and file processing job statuses in separate collections.
5.  **Amazon SQS** is used to decouple the initial file upload notification from the potentially long-running file parsing and validation process. An initiator Lambda sends a message to SQS, and a worker Lambda consumes messages from this queue to process files asynchronously.
6.  For AI enrichment, the system is designed to call an AI service (like Amazon Bedrock or an external API like OpenAI). The current implementation provides mock AI responses as live AI is not configured.
7.  All services are secured using **IAM roles and policies**, and network traffic between Lambda and DocumentDB is managed within a **VPC** using Security Groups and potentially VPC Endpoints.

### Backend Setup (High-Level)
1.  **AWS Account:** Ensure you have an AWS account.
2.  **DocumentDB Cluster:** Set up an Amazon DocumentDB cluster within a VPC. Note its endpoint, credentials, and configure security groups to allow access from Lambda.
3.  **S3 Buckets:** Create S3 buckets for:
    * Uploaded raw files (e.g., `my-product-portal-uploads`). Configure CORS for PUT operations from your frontend.
    * Processed result files (e.g., `my-product-portal-results`). Configure CORS for GET operations from your frontend (or ensure results are fetched via pre-signed GET URLs generated by the backend).
4.  **SQS Queue:** Create an SQS queue for asynchronous file processing. Configure a Dead-Letter Queue (DLQ) for it.
5.  **IAM Roles & Policies:** Create IAM roles for each Lambda function with the necessary permissions (CloudWatch Logs, S3 access, DocumentDB access via VPC, SQS access, Lambda invocation, Bedrock/OpenAI access if using live AI).
6.  **Lambda Functions:** Deploy the Python Lambda functions provided (see "Backend Code Repository Setup" below for organization).
    * Ensure all Lambda functions are configured with necessary environment variables (DB connection strings, SQS URL, other Lambda names, S3 bucket names, AI model IDs, API keys if applicable).
    * Package dependencies (like `pymongo`, `openai`) and the `global-bundle.pem` with the Lambdas or use Lambda Layers.
7.  **API Gateway:**
    * Create a REST API.
    * Configure resources and methods as outlined in the backend architecture (e.g., `/attributes`, `/products`, `/uploads/presigned-url`, `/uploads/process-file`, `/processing-jobs/{jobId}`, `/products/enrich`, `/products/bulk-update`).
    * Integrate methods with their respective Lambda functions using Lambda Proxy Integration.
    * Enable CORS for all necessary endpoints.
    * Deploy the API to a stage (e.g., `Dev`).
8.  **(Optional) AI Service Setup:**
    * If using Amazon Bedrock: Request model access in the Bedrock console for the desired models.
    * If using OpenAI: Obtain an API key. Store it securely (e.g., AWS Secrets Manager) and configure the Lambda to access it.

### Backend Code Repository Setup

    * Place the Python code for each Lambda function into its respective `lambda_function.py` file (or a suitably named file).
    * The `global-bundle.pem` file (for DocumentDB TLS connections) can be placed in each Lambda's folder that requires it, or managed via a Lambda Layer.
    * A `requirements.txt` file in each Lambda's directory can list its specific Python dependencies (e.g., `pymongo`, `openai`). `boto3` is included in the Lambda runtime by default.

  **Dependency Management and Packaging for Lambdas:**
    * For each Lambda function, you'll need to create a deployment package (.zip file) that includes its `lambda_function.py` and any dependencies listed in its `requirements.txt` (plus `global-bundle.pem` if needed).
    * **Recommended approach:** Use AWS Lambda Layers for common dependencies like `pymongo` and `openai` to keep your function .zip files small and ensure compatibility (see the "Lambda Packaging Guide" artifact ID: `lambda_packaging_openai_guide`).
    * If not using layers, you would typically run `pip install -r requirements.txt -t ./package` inside each Lambda's subfolder, then zip the contents of the `package` directory along with the `lambda_function.py` and `global-bundle.pem`.
