import os
from fnmatch import fnmatch

import boto3 as boto3
from trino import dbapi
from trino.auth import BasicAuthentication

compiled_models_dir = "/Users/couralex/src/spellbook/target/compiled/spellbook/models/"


def get_models(compiled_models_dir):
    """
    Function that returns a list of paths to all the compiled models in the compiled_models_dir.
    """
    pattern = "*.translated.sql"
    model_paths = []
    # Walking the directory to find all spells
    for path, subdirs, files in os.walk(compiled_models_dir):
        for name in files:
            if fnmatch(name, pattern):
                model_paths.append(os.path.join(path, name))

    return model_paths


def execute_query(query):
    """
    Function that executes a query passed as a string against the trino server. We would like to use aws secrets
    manager to authenticate.
    """
    # Fetching the secret from AWS Secrets Manager
    secret = get_secret()
    # Creating a connection to the trino server
    trino_host = os.environ.get('TRINO_URL')
    conn = dbapi.connect(
        host=trino_host,
        port=443,
        auth=BasicAuthentication("dune", "dune"),
        http_scheme="https",
        client_tags=["routingGroup=sandbox"],
    )
    # Executing the query
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


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


executed_query = execute_query("SELECT 1")
print(executed_query)