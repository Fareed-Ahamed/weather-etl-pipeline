import json
import requests
from constants.constants import openweather_base_url, cities_array, temp_units, secret_key_id
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class ExtractClass():
    def get_openweather_api_key(self):
        aws_hook = AwsBaseHook(aws_conn_id='AWS-DNA-SB', client_type='secretsmanager')
        client = aws_hook.get_conn()
        response = client.get_secret_value(SecretId=secret_key_id)
        secrets = json.loads(response['SecretString'])
        return secrets

    def extract_weather_data(self):
        openweather_api_key = self.get_openweather_api_key()['openweather_api_key']
        print(openweather_api_key)
        cities = cities_array
        base_url = openweather_base_url
        weather_data = {}
        for city in cities:
            try:
                response = requests.get(base_url, params={
                    'q': city,
                    'appid': openweather_api_key,
                    'units': temp_units
                })
                response.raise_for_status()
                data = response.json()
                weather_data[city] = data
                print(f"Weather data for {city}: {weather_data[city]}")
            except requests.exceptions.RequestException as e:
                weather_data[city] = f"Error: {e}"
            except KeyError:
                weather_data[city] = "Error: Unexpected response format."
        print(f"Weather data: {weather_data}")
        return weather_data