import json
from datetime import datetime
import pandas as pd
import requests
import sys

def kelvin_to_celsius(temp_in_kelvin):
    return temp_in_kelvin - 273.15

def extract_weather_data(chunk, api_key):
    base_url = "https://api.openweathermap.org/data/2.5/weather?id="
    weather_data_list = []

    for city_id in chunk:
        full_url = base_url + str(city_id) + "&appid=" + api_key

        try:
            proxies = {'http':'http://12.34.45.67:1234','https':'http://12.34.45.67:1234'}
            r = requests.get(full_url,proxies=proxies)
            r.raise_for_status()  # Raise an exception for HTTP errors
            data = r.json()
            weather_data_list.append(data)

        except requests.exceptions.HTTPError as e:
            print(f"Failed to fetch data for city ID {city_id}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for city ID {city_id}: {e}")

    return weather_data_list

def transform_weather_data(data):
    city_name = data.get("name", "Unknown")
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # Create a DataFrame directly from the transformed data
    df_data = pd.DataFrame({
        "City": [city_name],
        "Description": [weather_description],
        "Temperature (C)": [temp_celsius],
        "Feels Like (C)": [feels_like_celsius],
        "Minimum Temp (C)": [min_temp_celsius],
        "Maximum Temp (C)": [max_temp_celsius],
        "Pressure": [pressure],
        "Humidity": [humidity],
        "Wind Speed": [wind_speed],
        "Time of Record": [time_of_record],
        "Sunrise (Local Time)": [sunrise_time],
        "Sunset (Local Time)": [sunset_time]
    })
    return df_data
