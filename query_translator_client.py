"""
Client to run compile models against the dunesql query translator.

The query translator needs to be running on localhost:8000.

Instructions here: https://github.com/duneanalytics/query-translator#local-development
"""
import json
import os
from fnmatch import fnmatch

import requests


def get_sql(query_path):
    """
    returns the sql query from a model path
    """
    with open(query_path, 'r') as f:
        sql = f.read()
    return sql


class QueryTranslatorClient:
    def __init__(self):
        self.host = "http://localhost:8000"
        self.spark_spells_dir = os.path.join(os.getcwd(), "models")
        self.trino_spells_dir = os.path.join(os.getcwd(), "trinomodels")

    def translate(self, query_path):
        """
        Trtanslate query at query path. Returns translated sql text.
        """
        sql_query = get_sql(query_path)
        payload = {
            "query": sql_query,
            "dataset": "spark",
            "dialect": "spark",
            "spellbook": True
        }
        response = requests.post(self.host, json=payload)
        if response.status_code == 200:
            translated_query = json.loads(response.text)["translated"]
            return translated_query
        else:
            print(f"Request failed with error code {response.status_code}")
            print(json.loads(response.text))
            return -1

    def translate_all_spells(self):
        """
        Finds all *.sql files in provided directory and
        """
        pattern = "*.sql"
        for path, subdirs, files in os.walk(self.spark_spells_dir):
            for name in files:
                if fnmatch(name, pattern):
                    # print(os.path.join(path, name))
                    translated = self.translate(os.path.join(path, name))
                    if translated != -1:
                        # Write translated sql to trino directory
                        with open(os.path.join(path, name), "w") as f:
                            f.write(translated)


c = QueryTranslatorClient()
# QueryTranslatorClient().translate_all_spells()
c.translate("/Users/couralex/src/spellbook/target/compiled/spellbook/models/apeswap/ethereum/apeswap_ethereum_trades.sql")
# print(c.translation)