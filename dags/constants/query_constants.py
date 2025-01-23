raw_table_insert_query = """
                        INSERT INTO weatherschema.weather_raw
                        (id, city, temperature, weather_condition, humidity, wind_speed, visibility, created_at)
                        VALUES(
                            nextval('weatherschema.weather_raw_id_seq'::regclass),
                            %s, %s, %s, %s, %s, %s, %s
                        );
                    """

raw_data_transformation_query = """
                SELECT
                    city,
                    TO_CHAR(DATE(created_at), 'YYYY-MM-DD') AS date,
                    AVG(temperature) AS avg_temperature,
                    SUM(wind_speed) AS total_wind_speed,
                    AVG(humidity) AS avg_humidity,
                    CASE
                        WHEN MAX(temperature) > 40 THEN 'Heatwave'
                        WHEN MIN(temperature) < -10 THEN 'Cold Wave'
                        WHEN MAX(wind_speed) > 75 THEN 'Storm'
                        ELSE 'Normal'
                    END AS extreme_conditions
                FROM
                    weatherschema.weather_raw
                GROUP BY
                    city, DATE(created_at);
            """

processed_data_load_query = """
                INSERT INTO weatherschema.weather_processed
                (id, city, "date", avg_temperature, total_wind_speed, avg_humidity, extreme_conditions, created_at, updated_at)
                VALUES(nextval('weatherschema.weather_processed_id_seq'::regclass), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (city, "date")
                DO UPDATE SET
                    avg_temperature = EXCLUDED.avg_temperature,
                    total_wind_speed = EXCLUDED.total_wind_speed,
                    avg_humidity = EXCLUDED.avg_humidity,
                    extreme_conditions = EXCLUDED.extreme_conditions,
                    updated_at = EXCLUDED.updated_at;
            """