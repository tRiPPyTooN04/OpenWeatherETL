
import json 
import sys 
sys.path.append('/home/saxen/projects/openWeatherPipeline/')
from python_script import extract_weather_data

with open("/home/saxen/projects/openWeatherPipeline/data/city.list.json/city.list.json", "r") as file:
    city_data = json.load(file)
city_ids = [city["id"] for city in city_data]
for i in range(0, len(city_ids), 100):
        chunk = city_ids[i:i+100]
print(chunk)


def extract_weather_data_for_all_cities(city_ids, api_key):
    # Initialize an empty list to store all weather data
    weather_data_list = []
    # Call the extract_weather_data function with the retrieved values
    for i in range(0, min(1000, len(city_ids)), 100):      # since only 1000 calls are for free
       chunk = city_ids[i:i+100]
       chunk_weather_data = extract_weather_data(chunk, api_key)
       weather_data_list.extend(chunk_weather_data)
       print(chunk_weather_data)
    return weather_data_list