# Getting API response in pyspark (python script) in json format and modifying it according to further needs. 
# Parsing and Slicing throught the API data, popping and deleting the keys, rest including into json format later to be converted into parquet.
# Below code gives a jist about the same, fetching and accessing an Open Sourced API (weather) to study climatic and environmental changes across globe.

def get_weather(latitude, longitude, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}"
    response = requests.get(url)
    weather_data = response.json()
    
    coord_entry = {'lon': longitude, 'lat': latitude}
    weather_data.update(coord_entry)
    weather_data['summary'] = weather_data['weather'][0].pop('description')
    weather_data['icon'] = weather_data['weather'][0].pop('icon')
    weather_data['temperature'] = weather_data['main'].pop('temp')
    weather_data['apparentTemperature'] = weather_data['main'].pop('feels_like')
    weather_data['temp_min'] = weather_data['main'].pop('temp_min')
    weather_data['temp_max'] = weather_data['main'].pop('temp_max')
    weather_data['pressure'] = weather_data['main'].pop('sea_level')
    weather_data['humidity'] = weather_data['main'].pop('humidity')
    weather_data['visibility'] = weather_data.pop('visibility')
    weather_data['wind_speed'] = weather_data['wind'].pop('speed')
    weather_data['wind_direction'] = weather_data['wind'].pop('deg')
    weather_data['wind_gust'] = weather_data['wind'].pop('gust')
    weather_data['cloudCover'] = weather_data['clouds'].pop('all')
    weather_data['time'] = weather_data.pop('dt')
    weather_data['timezone'] = weather_data.pop('timezone')
    
    keys_to_delete=['weather','coord','base','main','visibility','wind','clouds','dt','sys','timezone','id','name','cod','rain','snow','extreme']
    
    for key in keys_to_delete:
        weather_data.pop(key, None)
    
    return weather_data
