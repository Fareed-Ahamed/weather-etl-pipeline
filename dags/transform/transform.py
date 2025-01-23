from utils.dbutils import DBUtils
from constants.query_constants import raw_data_transformation_query
import psycopg2


class TransformClass:
    def transform_data(self):
        """
        Perform data transformation: calculate metrics and return the transformed data.
        """
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

            transformation_query = raw_data_transformation_query

            cur.execute(transformation_query)
            transformed_data = cur.fetchall()
            print("Transformation completed successfully.")
            cur.close()
            conn.close()
            return transformed_data

        except Exception as e:
            print(f"Error during data transformation: {e}")
            raise