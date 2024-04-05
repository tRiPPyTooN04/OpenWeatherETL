from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
import sys

sys.path.append('/home/saxen/projects/openWeatherPipeline/')
from python_script import extract_weather_data
from python_script import transform_weather_data

default_args = {
    'owner': 'Saxen Dcruz',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def create_endpoint(api_key, city_id):
    base_url = '/data/2.5/weather?id='
    full_url = base_url + str(city_id) + "&appid=" + api_key
    return full_url

api_key="{{var.value.get('api_key')}}"
print(api_key)

# Load city IDs from file
with open("/home/saxen/projects/openWeatherPipeline/data/city.list.json/city.list.json", "r") as file:
    city_data = json.load(file)
city_ids = [city["id"] for city in city_data]

def extract_weather_data_for_all_cities(city_ids, api_key):
    # Initialize an empty list to store all weather data
    weather_data_list = []
    # Call the extract_weather_data function with the retrieved values
    for i in range(0, min(1000, len(city_ids)), 100):      # since only 1000 calls are for free
       chunk = city_ids[i:i+100]
       chunk_weather_data = extract_weather_data(chunk, api_key)
       weather_data_list.extend(chunk_weather_data)
    return weather_data_list

def process_weather_data(weather_data_list, **kwargs):
    if weather_data_list:
        transformed_data_list = [transform_weather_data(data) for data in weather_data_list]
        df_data = pd.concat(transformed_data_list, ignore_index=True)
        df_data.to_csv("/home/saxen/projects/openWeatherPipeline/current_weather_data_multiple_cities.csv", index=False)
    else:
        print("No weather data to process.")


with DAG('weather_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    is_weather_api_ready_task = HttpSensor(
        task_id='is_weather_api_ready_task',
        http_conn_id='weathermap_api',
        endpoint=create_endpoint(api_key, "833"),  # Using a single city for the sensor check
        timeout=20,
        retries=3,
        mode='poke'
    )

    extract_weather_data_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data_for_all_cities,
        op_args=[city_ids, api_key],
        provide_context=True
    )

    process_weather_data_task = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        provide_context=True
    )

    is_weather_api_ready_task >> extract_weather_data_task >> process_weather_data_task
