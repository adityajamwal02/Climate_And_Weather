import os
import random
import requests

def generate_random_coordinates():
    latitude = random.uniform(-90, 90)
    longitude = random.uniform(-180, 180)
    return latitude, longitude

def get_valid_coordinates(num_coordinates):
    coordinates = []
    while len(coordinates) < num_coordinates:
        latitude, longitude = generate_random_coordinates()
        if abs(latitude) <= 90 and abs(longitude) <= 180:
            coordinates.append((latitude, longitude))
    return coordinates

def get_weather_info(latitude, longitude):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={API_KEY}"
    response = requests.get(url)
    weather_data = response.json()
    return weather_data

folder_name = "WeatherData"
os.makedirs(folder_name, exist_ok=True)

random_coordinates = get_valid_coordinates(10)

for i, (latitude, longitude) in enumerate(random_coordinates, 1):
    weather_data = get_weather_info(latitude, longitude)
    file_name = f"{folder_name}/Coordinate_{i}.txt"
    with open(file_name, 'w') as file:
        file.write(str(weather_data))

    print(f"JSON response for Coordinate {i} is saved in '{file_name}'")
