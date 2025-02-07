from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timezone
import psycopg2
import json
from constants.constants import airflow_bucket_name
from constants.query_constants import raw_table_insert_query, processed_data_load_query
from utils.dbutils import DBUtils


class LoadClass:
    def push_data_to_s3(self, data, path_type):
        try:
            s3_hook = AwsBaseHook(aws_conn_id='AWS-DNA-SB', client_type='s3')
            s3_client = s3_hook.get_conn()
            bucket_name = airflow_bucket_name

            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            year, month, day, hour = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H').split('/')
            object_key = f"{path_type}/{year}/{month}/{day}/{hour}/weather_{timestamp}.json"

            s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=json.dumps(data))
            print(f"Data pushed to S3: {bucket_name}/{object_key}")
        except Exception as e:
            print(f"Error pushing data to S3: {e}")
            raise

    def insert_raw_data_to_db(self, weather_data):
        failed_data = []
        try:
            db_utils = DBUtils()
            postgres_secrets = db_utils.get_postgres_secrets()
            conn = psycopg2.connect(
                host=postgres_secrets['host'],
                port=postgres_secrets['port'],
                database=postgres_secrets['dbname'],
                user=postgres_secrets['username'],
                password=postgres_secrets['password']
            )
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise

        try:
            cur = conn.cursor()
            for city, city_weather_data in weather_data.items():
                try:
                    parsed_data = self.parse_weather_data(city_weather_data)

                    # Data quality checks
                    if not self.validate_weather_data(parsed_data):
                        print(f"Data quality check failed for city: {city}")
                        failed_data.append(city_weather_data)
                        continue

                    query = raw_table_insert_query
                    cur.execute(query, (
                        parsed_data["city"],
                        parsed_data["temperature"],
                        parsed_data["weather_condition"],
                        parsed_data["humidity"],
                        parsed_data["wind_speed"],
                        parsed_data["visibility"],
                        parsed_data["created_at"]
                    ))
                    print(f"Inserted data for city: {parsed_data['city']}")
                except Exception as e:
                    print(f"Error processing data for city {city}: {e}")
                    failed_data.append(city_weather_data)

            conn.commit()
        except Exception as e:
            print(f"Error inserting raw weather data into PostgreSQL: {e}")
            conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

            if failed_data:
                self.push_data_to_s3(failed_data, "failed")

    def parse_weather_data(self, city_weather_data):
        try:
            parsed_data = {
                "city": city_weather_data["name"],
                "temperature": city_weather_data["main"]["temp"],
                "weather_condition": city_weather_data["weather"][0]["description"],
                "humidity": city_weather_data["main"]["humidity"],
                "wind_speed": city_weather_data["wind"]["speed"],
                "visibility": city_weather_data.get("visibility", None),
                "created_at": datetime.now(),
            }
            return parsed_data
        except KeyError as e:
            print(f"Error parsing weather data: Missing key {e}")
            raise
        except Exception as e:
            print(f"Error parsing weather data: {e}")
            raise

    def validate_weather_data(self, parsed_data):
        try:
            if parsed_data["temperature"] is None or parsed_data["humidity"] is None:
                return False
            if parsed_data["wind_speed"] < 0:
                return False
            return True
        except Exception as e:
            print(f"Error validating weather data: {e}")
            return False

    def load_data(self, transformed_data):
        failed_data = []
        try:
            db_utils = DBUtils()
            postgres_secrets = db_utils.get_postgres_secrets()
            conn = psycopg2.connect(
                host=postgres_secrets['host'],
                port=postgres_secrets['port'],
                database=postgres_secrets['dbname'],
                user=postgres_secrets['username'],
                password=postgres_secrets['password']
            )
            cur = conn.cursor()

            insert_query = processed_data_load_query

            for record in transformed_data:
                try:
                    now = datetime.now()

                    # Data quality checks
                    if not all(record):
                        print(f"Data quality check failed for record: {record}")
                        failed_data.append(record)
                        continue

                    cur.execute(insert_query, (record[0], record[1], record[2], record[3], record[4], record[5], now, now))
                except Exception as e:
                    print(f"Error inserting record {record}: {e}")
                    failed_data.append(record)

            conn.commit()
            print("Data loaded into weather_processed table successfully.")
            cur.close()
            conn.close()

        except Exception as e:
            print(f"Error during data loading: {e}")
            raise

        if failed_data:
            self.push_data_to_s3(failed_data, "failed")
