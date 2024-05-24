import json, os, requests
from dateutil.relativedelta import relativedelta 
from datetime import datetime
import boto3
from typing import Dict, List

def get_taxi_data(formatted_datetime:str) -> List:
    '''
    Retrieves taxi data for the given date.
    
    Parameters:
        formatted_datetime(str): The date in 'YYYY-MM-DD' format.
        
    Returns:
        List: A list containing the taxi_data as a JSON.
    
    '''
    taxi_url = f"https://data.cityofchicago.org/resource/ajtu-isnz.json?$where=trip_start_timestamp >= '{formatted_datetime}T00:00:00.000' AND trip_start_timestamp <= '{formatted_datetime}T23:59:59.000'&$limit=30000"
    headers = {'X-App-Token': os.environ.get('CHICAGO_API_TOKEN')}
    
    response_taxi = requests.get(taxi_url, headers=headers)
    
    taxi_data = response_taxi.json()
    
    return taxi_data
    

def get_weather_data(formatted_datetime: str) -> List:
    '''
    Retrieve weather data for a specific datetime from the Open Meteo archive API.

    Parameters:
        formatted_datetime (str): A formatted datetime string in UTC format (YYYY-MM-DDTHH:MM:SS).

    Returns:
        List: A list containing weather data for the specified datetime. Each item in the list represents
              weather data for a specific hour and includes temperature at 2m (in Kelvin), wind speed at 10m (in m/s),
              precipitation (in mm), and rain (in mm/h).

    Example:
        weather_data = get_weather_data("2024-01-01T00:00:00")

    Note:
        The latitude and longitude coordinates used in the API request are for Chicago, IL, USA.
        Adjust the latitude and longitude values as needed for your location.
    '''
    
    weather_url = 'https://archive-api.open-meteo.com/v1/era5'
    
    params = {
        'latitude': 41.85,
        'longitude': -87.65,
        'start_date': formatted_datetime,
        'end_date': formatted_datetime,
        'hourly': 'temperature_2m,wind_speed_10m,precipitation,rain'
    }
    
    response_weather = requests.get(weather_url, params=params)
    
    weather_data = response_weather.json()
    
    return weather_data
    

def upload_to_s3(data:List, folder_name:str, file_name:str) -> None:
    """
    Uploads data to an Amazon S3 bucket.

    Parameters:
        data (List): The data to upload to S3.
        folder_name (str): The name of the folder in S3 where the file will be stored.
        file_name (str): The name of the file to be uploaded.

    Returns:
        None
    """
    client = boto3.client('s3')
    
    client.put_object(
        Bucket='cubix-chicago-taxi-a8i9zm',
        Key=f'raw_data/to_processed/{folder_name}/{file_name}',
        Body=json.dumps(data)
        )

def lambda_handler(event, context):
    current_datetime = datetime.now() - relativedelta(months=2)
    
    formatted_datetime = current_datetime.strftime('%Y-%m-%d')
    
    taxi_data_api_call = get_taxi_data(formatted_datetime) 
    weather_data_api_call = get_weather_data(formatted_datetime)
    
    taxi_file_name = f'taxi_raw_{formatted_datetime}.json'
    weather_file_name = f'weather_raw_{formatted_datetime}.json'
    
    upload_to_s3(taxi_data_api_call, 'taxi_data', taxi_file_name)
    upload_to_s3(weather_data_api_call, 'weather_data', weather_file_name)