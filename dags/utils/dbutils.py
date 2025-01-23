from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from constants.constants import weather_db_secret_name
import json

class DBUtils:
    def get_postgres_secrets(self):
        try:
            aws_hook = AwsBaseHook(aws_conn_id='AWS-DNA-SB', client_type='secretsmanager')
            client = aws_hook.get_conn()
            response = client.get_secret_value(SecretId=weather_db_secret_name)
            secrets = json.loads(response['SecretString'])
            return secrets
        except Exception as e:
            print(f"Error fetching PostgreSQL secrets: {e}")
            raise