from io import StringIO

import json
import boto3

import pandas as pd 


def transform_weather_data(weather_data: json) -> pd.DataFrame:

    weather_data_filtered = {
    'datetime': weather_data['hourly']['time'],
    'temperature': weather_data['hourly']['temperature_2m'],
    'wind_speed': weather_data['hourly']['wind_speed_10m'],
    'precipitation': weather_data['hourly']['precipitation'],
    'rain': weather_data['hourly']['rain']
    }

    weather_df = pd.DataFrame(weather_data_filtered)

    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])

    return weather_df

def taxi_trips_transformations(taxi_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Perform transformations on a DataFrame containing taxi trip data.

    Parameters:
    - taxi_trips (pd.DataFrame): A pandas DataFrame containing taxi trip data.

    Returns:
    - pd.DataFrame: A DataFrame with the following transformations applied:
        - Columns 'pickup_census_tract', 'dropoff_census_tract', 'pickup_centroid_location',
          and 'dropoff_centroid_location' are dropped.
        - Rows with missing values are dropped.
        - Columns 'pickup_community_area' and 'dropoff_community_area' are renamed to
          'pickup_community_area_id' and 'dropoff_community_area_id' respectively.
        - A new column 'datetime_for_weather' is created, containing the hourly timestamp
          of the 'trip_start_timestamp' column.
    """
    taxi_trips.drop(['pickup_census_tract','dropoff_census_tract', 'pickup_centroid_location', 'dropoff_centroid_location'], axis=1, inplace=True)
    
    taxi_trips.dropna(inplace=True)
    
    taxi_trips.rename(columns={
        'pickup_community_area': 'pickup_community_area_id',
        'dropoff_community_area': 'dropoff_community_area_id'},
        inplace=True
        )
    
    taxi_trips['datetime_for_weather'] = pd.to_datetime(taxi_trips['trip_start_timestamp']).dt.floor('h')

    return taxi_trips

def update_taxi_trips_with_master_data(taxi_trips: pd.DataFrame, payment_type_master: pd.DataFrame, company_master: pd.DataFrame) -> pd.DataFrame:
    """
    Updates the taxi_trips DataFrame with master data for payment types and taxi companies.

    Parameters:
        taxi_trips (pd.DataFrame): DataFrame containing taxi trips data.
        payment_type_master (pd.DataFrame): DataFrame containing master data for payment types.
        company_master (pd.DataFrame): DataFrame containing master data for taxi companies.

    Returns:
        pd.DataFrame: Updated taxi_trips DataFrame with additional columns for payment type IDs and company IDs.

    Raises:
        ValueError: If 'payment_type' or 'company' columns are not found in taxi_trips DataFrame or master DataFrames.
        TypeError: If taxi_trips, payment_type_master, or company_master is not a pandas DataFrame.
    """
    # Check if taxi_trips, payment_type_master, and company_master are DataFrames
    if not isinstance(taxi_trips, pd.DataFrame) or not isinstance(payment_type_master, pd.DataFrame) or not isinstance(company_master, pd.DataFrame):
        raise TypeError("All input parameters should be pandas DataFrames.")

    # Check if 'payment_type' and 'company' columns exist in taxi_trips and master DataFrames
    if 'payment_type' not in taxi_trips.columns or 'company' not in taxi_trips.columns:
        raise ValueError("Both 'payment_type' and 'company' columns should be present in taxi_trips DataFrame.")
    if 'payment_type' not in payment_type_master.columns:
        raise ValueError("'payment_type' column not found in payment_type_master DataFrame.")
    if 'company' not in company_master.columns:
        raise ValueError("'company' column not found in company_master DataFrame.")

    # Merge taxi_trips with payment_type_master and company_master
    taxi_trips_id = taxi_trips.merge(payment_type_master, on='payment_type')
    taxi_trips_id = taxi_trips_id.merge(company_master, on='company')

    # Drop unnecessary columns
    taxi_trips_id.drop(['payment_type', 'company'], axis=1, inplace=True)

    return taxi_trips_id

def update_master(master: pd.DataFrame, taxi_trips: pd.DataFrame, id_column: str, value_column: str) -> pd.DataFrame:
   
    max_id = master[id_column].max()

    new_values_list = []
    for value in taxi_trips[value_column].values:
        if value not in master[value_column].values:
            new_values_list.append(value)

    new_values_df = pd.DataFrame({
    id_column: range(max_id +1, max_id + len(new_values_list) +1),
    value_column : new_values_list
    }
    )

    updated_master = pd.concat([master, new_values_df], ignore_index=True)

    return updated_master

def read_csv_from_s3(bucket: str, path: str, filename: str) -> pd.DataFrame:
    """
    Reads a CSV file from an S3 bucket and returns it as a pandas DataFrame.

    Parameters:
        bucket (str): The name of the S3 bucket.
        path (str): The path within the S3 bucket where the file is located.
        filename (str): The name of the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the contents of the CSV file.

    """
    s3 = boto3.client('s3')
    
    full_path = f'{path}{filename}'
    
    object = s3.get_object(Bucket=bucket, Key=full_path)
    object = object['Body'].read().decode('utf-8')
    output_df = pd.read_csv(StringIO(object))
    
    return output_df
    
def upload_dataframe_to_s3(dataframe: pd.DataFrame, bucket: str, path: str):
    s3 = boto3.client('s3')
    
    buffer = StringIO()
    dataframe.to_csv(buffer, index=False)
    df_content = buffer.getvalue()
    s3.put_object(Bucket=bucket, Key=path, Body=df_content)

def upload_master_data_to_s3(bucket: str, path: str, file_tpye: str, dataframe: pd.DataFrame):
    
    s3 = boto3.client('s3')
    
    master_file_path = f'{path}{file_tpye}_master.csv'
    previous_master_file_path = f'transformed_data/master_table_previous_version/{file_tpye}_master_previous_version.csv'
    
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': master_file_path},
        Key = previous_master_file_path
    )
    
        
    # buffer = StringIO()
    # dataframe.to_csv(buffer, index=False)
    # df_content = buffer.getvalue()
    # s3.put_object(Bucket=bucket, Key=master_file_path, Body=df_content)
    
    upload_dataframe_to_s3(bucket=bucket, dataframe=dataframe, path=master_file_path)
    
    
def upload_and_move_file_on_s3(dataframe: pd.DataFrame, datetime_column: str, bucket: str, file_type: str, filename: str, source_path: str, target_path_raw: str, target_path_transformed: str):
    s3 = boto3.client('s3')
    
    formatted_date = dataframe[datetime_column].iloc[0].strftime('%Y-%m-%d')
    new_path_with_filename = f'{target_path_transformed}{file_type}_{formatted_date}.csv'
    
    # buffer = StringIO()
    # dataframe.to_csv(buffer, index=False)
    # df_content = buffer.getvalue()
    # s3.put_object(Bucket=bucket, Key=new_path_with_filename, Body=df_content)
    
    upload_dataframe_to_s3(bucket=bucket, dataframe=dataframe, path=new_path_with_filename)
    
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key':f'{source_path}{filename}'},
        Key = f'{target_path_raw}{filename}'
    )
    
    s3.delete_object(Bucket=bucket, Key=f'{source_path}{filename}')


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    bucket = 'cubix-chicago-taxi-a8i9zm'
    raw_weather_folder = 'raw_data/to_processed/weather_data/'
    raw_taxi_trips_folder = 'raw_data/to_processed/taxi_data/'
    
    target_taxi_trips_folder = 'raw_data/processed/taxi_data/'
    target_weather_folder = 'raw_data/processed/weather_data/'
    
    transformed_taxi_trips_folder = 'transformed_data/taxi_trips/'
    transformed_weather_folder = 'transformed_data/weather/'
    
    test = s3.list_objects(Bucket=bucket, Prefix=raw_weather_folder)['Contents']
    payment_type_master_folder = 'transformed_data/payment_type/'
    company_master_folder = 'transformed_data/company/'
    
    payment_type_master_file_name = 'payment_type_master.csv'
    company_master_file_name = 'company_master.csv'
    
    payment_type_master = read_csv_from_s3(bucket=bucket, path=payment_type_master_folder, filename=payment_type_master_file_name )
    company_master = read_csv_from_s3(bucket=bucket, path=company_master_folder, filename=company_master_file_name)
    

    # Taxi data transformation and loading
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_taxi_trips_folder)['Contents']:
        taxi_trip_key = file['Key']
        
        if taxi_trip_key.split('/')[-1].strip() != '':
            if taxi_trip_key.split('.')[1] == 'json':
                
                filename = taxi_trip_key.split('/')[-1]
                
                response = s3.get_object(Bucket=bucket, Key=taxi_trip_key)
                content = response['Body']
                taxi_trip_data_json = json.loads(content.read())
                
                taxi_trips_raw_data = pd.DataFrame(taxi_trip_data_json)
                taxi_trips_transformed = taxi_trips_transformations(taxi_trips_raw_data)
                
                company_master_updated = update_master(company_master,taxi_trips_transformed,'company_id','company')
                payment_type_master_updated = update_master(payment_type_master,taxi_trips_transformed,id_column='payment_type_id',value_column='payment_type')
                
                taxi_trips = update_taxi_trips_with_master_data(taxi_trips_transformed,payment_type_master_updated,company_master_updated)
                
                #formatted_date = taxi_trips['datetime_for_weather'].iloc[0].strftime('%Y-%m-%d')
                
                upload_and_move_file_on_s3(
                    dataframe=taxi_trips, 
                    datetime_column='datetime_for_weather', 
                    bucket=bucket, 
                    file_type='taxi', 
                    filename=filename, 
                    source_path=raw_taxi_trips_folder, 
                    target_path_raw=target_taxi_trips_folder, 
                    target_path_transformed=transformed_taxi_trips_folder
                )
                
                print('taxi_trips is uploaded and moved')
            

                 
                upload_master_data_to_s3(bucket=bucket, path=payment_type_master_folder, file_tpye='payment_type', dataframe=payment_type_master_updated)
                print('Payment_type_master has been updated')
                
                upload_master_data_to_s3(bucket=bucket, path=company_master_folder, file_tpye='company', dataframe=company_master_updated)
                print('Company_master has been updated')
                
                #upload taxi_trips to S3
                  
               
    # Weather data transformation and loading
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_weather_folder)['Contents']:
        weather_key = file['Key']
        
        if weather_key.split('/')[-1].strip() != '':
            if weather_key.split('.')[1] == 'json':
                
                filename = weather_key.split('/')[-1]
                
                
                response = s3.get_object(Bucket=bucket, Key=weather_key)
                content = response['Body']
                weather_data_json = json.loads(content.read())
                
                weather_data = transform_weather_data(weather_data_json)
                    
                upload_and_move_file_on_s3(
                    dataframe=weather_data, 
                    datetime_column='datetime', 
                    bucket=bucket, 
                    file_type='weather', 
                    filename=filename, 
                    source_path=raw_weather_folder, 
                    target_path_raw=target_weather_folder, 
                    target_path_transformed=transformed_weather_folder
                )
                    
                print('weather is uploaded and moved')
                    
                    # upload weather to s3 function
                    
                    #print(weather_data['datetime'].iloc[0].strftime('%Y-%m-%d'))
