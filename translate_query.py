#!/usr/bin/env python3

"""
Client to run compile models against the dunesql query translator.

The query translator needs to be running on localhost:8000.

Instructions here: https://github.com/duneanalytics/query-translator#local-development
"""
import json
import os
from fnmatch import fnmatch
import sqlfluff
import requests
import sys


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
        self.spark_spells_dir = os.path.join(os.getcwd(), "target/compiled/spellbook")
        # set containing all the queries that have been translated
        self.translated_queries = self.get_translated_queries()

    def get_translated_queries(self):
        """
        Returns a set containing all the queries that have been translated
        """
        translated_queries = set()
        for path, subdirs, files in os.walk(self.spark_spells_dir.replace("target", "trinotarget")):
            for name in files:
                if fnmatch(name, "*.sql"):
                    query_path = os.path.join(path, name)
                    translated_queries.add(query_path)
        return translated_queries


    def translate(self, spell_path):
        """
        Translate query at query path. Returns translated sql text.
        """
        full_path = os.path.join(self.spark_spells_dir, spell_path)
        sql_query = get_sql(full_path)
        if sql_query.count("\n") > 500:
            print(f"Skipping {spell_path} because it has more than 500 lines")
            return -1
        payload = {
            "query": sql_query,
            "dataset": "spark",
            "dialect": "spark",
            "spellbook": True
        }
        response = requests.post(self.host, json=payload)
        if response.status_code == 200:
            translated_query = json.loads(response.text)["translated"]
            try:
                return sqlfluff.fix(translated_query)
            except Exception as e:
                print(f"Error sqlfluff: {e}")
                return translated_query

        else:
            print(f"{spell_path}; {json.loads(response.text)}")
            return -1

    def translate_all_spells(self):
        """
        Finds all *.sql files in compiled directory and translates them.

        NOTE: this will be slow!
        """
        pattern = "*.sql"
        for path, subdirs, files in os.walk(self.spark_spells_dir):
            for name in files:
                if fnmatch(name, pattern):
                    to_translate = os.path.join(path, name)
                    translated_path = to_translate.replace("target", "trinotarget")
                    # only translate if we haven't translated it before
                    if translated_path not in self.translated_queries:
                        translated = self.translate(to_translate)
                        if translated != -1:
                            # Write translated sql to trino directory
                            # Create all directories if they don't exist
                            os.makedirs(os.path.dirname(translated_path), exist_ok=True)
                            with open(translated_path, "w") as f:
                                f.write(translated)
                            self.translated_queries.add(translated_path)


if __name__ == "__main__":
    client = QueryTranslatorClient()
    # get file path from command line argument
    print(client.translate(sys.argv[1]))

