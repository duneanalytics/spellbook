"""
Client to run compile models against the dunesql query translator.

The query translator needs to be running on localhost:8000.

Instructions here: https://github.com/duneanalytics/query-translator#local-development
"""
import json
import requests


class query_translator_client:
    def __init__(self):
        self.host = "http://localhost:8000"


    def translate(self, query_path):
        sql_query = get_sql(query_path)
        payload = {
            "query": sql_query,
            "dataset": "spark",
            "dialect": "spark",
            "spellbook": True
        }
        response = requests.post(self.host, json=payload)
        if response.status_code == 200:
            # print("Success")
            query = json.loads(response.text)["translated"]
            self.translation = query
        else:
            print(f"Request failed with error code {response.status_code}")
            print(json.loads(response.text))
            self.translation = None

def get_sql(query_path):
    """
    returns the sql query from a model path
    """
    with open(query_path, 'r') as f:
        sql = f.read()
    return sql

# c = query_translator_client()
# c.translate("/Users/couralex/src/spellbook/target/compiled/spellbook/models/apeswap/ethereum/apeswap_ethereum_trades.sql")
# print(c.translation)