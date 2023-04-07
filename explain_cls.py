import json
import os
from fnmatch import fnmatch

import boto3 as boto3
from trino import dbapi
from trino.auth import BasicAuthentication
from trino.exceptions import TrinoUserError


class Explain_n_Executer:
    def __init__(self, model_path):
        self.model_path = model_path

    def execute_query(self):
        """
        Function that executes a query passed as a string against the trino server. We would like to use aws secrets
        manager to authenticate.
        """
        username = os.environ.get('TRINO_USERNAME')
        password = os.environ.get('TRINO_PASSWORD')

        # Creating a connection to the trino server
        trino_host = os.environ.get('TRINO_URL')
        conn = dbapi.connect(
            host=trino_host,
            port=443,
            auth=BasicAuthentication(username, password),
            http_scheme="https",
            client_tags=["routingGroup=sandbox"],
        )
        # try executing the query and returning the response. return error if it fails.
        try:
            cursor = conn.cursor()
            cursor.execute(self.sql)
            return cursor.fetchall()
        except TrinoUserError as e:
            return f"error: {e.message}"
        except Exception as e:
            return f"NON_TRINO_ERROR : {e}"


    def get_sql(self):
        """
        Function that returns the SQL query from a model path.
        """
        with open(self.model_path, 'r') as f:
            sql = f.read()
        self.sql = sql.replace("`", "").replace(".from", '."from"')


    @staticmethod
    def get_secret():
        """
        Function that fetches a secret from AWS Secrets Manager given a secret ARN.
        Note: may not be needed if we can use the user/pass to authenticate.
        """
        secret_arn = os.environ.get('TRINO_SECRET_ARN')
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
        secret = get_secret_value_response['SecretString']
        return secret


    def explain_query(self):
        """
        Function that explains a query and returns the response.
        """
        resp = self.execute_query("EXPLAIN (TYPE LOGICAL, FORMAT JSON) " + self.sql)
        if type(resp) == str:
            return resp
        self.explain = json.loads(resp[0][0])
