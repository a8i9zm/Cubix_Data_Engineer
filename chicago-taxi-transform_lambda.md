# README

## Overview

This repository contains a set of AWS Lambda functions designed to process and transform taxi and weather data for Chicago. The functions retrieve raw data, perform necessary transformations, and upload the processed data to Amazon S3.

## Functions

### 1. `get_taxi_data`

Fetches taxi trip data for a given date from the Chicago Data Portal.

**Parameters:**
- `formatted_datetime (str)`: The date in `YYYY-MM-DD` format.

**Returns:**
- `List`: A list containing the taxi trip data as JSON.

### 2. `get_weather_data`

Retrieves weather data for a specific date from the Open Meteo archive API.

**Parameters:**
- `formatted_datetime (str)`: The date in `YYYY-MM-DD` format.

**Returns:**
- `List`: A list containing hourly weather data for the specified date.

### 3. `upload_to_s3`

Uploads data to an Amazon S3 bucket.

**Parameters:**
- `data (List)`: The data to upload.
- `folder_name (str)`: The name of the folder in S3.
- `file_name (str)`: The name of the file to be uploaded.

### 4. `transform_weather_data`

Transforms raw weather data into a Pandas DataFrame.

**Parameters:**
- `weather_data (json)`: The raw weather data in JSON format.

**Returns:**
- `pd.DataFrame`: A DataFrame with the transformed weather data.

### 5. `taxi_trips_transformations`

Performs transformations on a DataFrame containing taxi trip data.

**Parameters:**
- `taxi_trips (pd.DataFrame)`: A DataFrame containing taxi trip data.

**Returns:**
- `pd.DataFrame`: A transformed DataFrame with necessary columns and datetime adjustments.

### 6. `update_taxi_trips_with_master_data`

Updates the taxi trips DataFrame with master data for payment types and taxi companies.

**Parameters:**
- `taxi_trips (pd.DataFrame)`: DataFrame containing taxi trips data.
- `payment_type_master (pd.DataFrame)`: DataFrame containing master data for payment types.
- `company_master (pd.DataFrame)`: DataFrame containing master data for taxi companies.

**Returns:**
- `pd.DataFrame`: Updated taxi trips DataFrame with additional columns for payment type IDs and company IDs.

### 7. `update_master`

Updates the master DataFrame with new unique values.

**Parameters:**
- `master (pd.DataFrame)`: The master DataFrame.
- `taxi_trips (pd.DataFrame)`: DataFrame containing taxi trips data.
- `id_column (str)`: The ID column name.
- `value_column (str)`: The value column name.

**Returns:**
- `pd.DataFrame`: Updated master DataFrame.

### 8. `read_csv_from_s3`

Reads a CSV file from an S3 bucket and returns it as a Pandas DataFrame.

**Parameters:**
- `bucket (str)`: The name of the S3 bucket.
- `path (str)`: The path within the S3 bucket.
- `filename (str)`: The name of the CSV file.

**Returns:**
- `pd.DataFrame`: DataFrame containing the CSV contents.

### 9. `upload_dataframe_to_s3`

Uploads a Pandas DataFrame to an S3 bucket.

**Parameters:**
- `dataframe (pd.DataFrame)`: The DataFrame to upload.
- `bucket (str)`: The S3 bucket name.
- `path (str)`: The path within the S3 bucket.

### 10. `upload_master_data_to_s3`

Uploads master data to S3 and maintains versioning.

**Parameters:**
- `bucket (str)`: The S3 bucket name.
- `path (str)`: The path within the S3 bucket.
- `file_type (str)`: The type of master data.
- `dataframe (pd.DataFrame)`: The master DataFrame to upload.

### 11. `upload_and_move_file_on_s3`

Uploads a DataFrame to S3 and moves the original raw file to another location in S3.

**Parameters:**
- `dataframe (pd.DataFrame)`: The DataFrame to upload.
- `datetime_column (str)`: The datetime column for naming.
- `bucket (str)`: The S3 bucket name.
- `file_type (str)`: The type of file.
- `filename (str)`: The name of the raw file.
- `source_path (str)`: The source path in S3.
- `target_path_raw (str)`: The target raw path in S3.
- `target_path_transformed (str)`: The target transformed path in S3.

## AWS Lambda Function

### `lambda_handler`

Main handler function for the AWS Lambda. It processes taxi and weather data, applies necessary transformations, and uploads the results to S3.

**Parameters:**
- `event (dict)`: Event data (unused in this context).
- `context (LambdaContext)`: Runtime information provided by AWS Lambda.

## Prerequisites

- AWS credentials configured.
- Pandas library installed.
- `boto3` library installed.

## Environment Variables

- `CHICAGO_API_TOKEN`: API token for accessing the Chicago Data Portal.

## Deployment

1. Set up AWS Lambda and configure the function with the provided script.
2. Set environment variables.
3. Trigger the Lambda function via an event or on a schedule.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

This README file covers all the functions within your script, detailing their purpose, parameters, and return values. It also provides deployment and prerequisite information.