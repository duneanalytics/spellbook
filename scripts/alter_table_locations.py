import argparse
import json
import subprocess

from string import Template


class TableLocationManager:
    """
    Quick script to hopefully be used a single time to generate a macro to move tables to s3 locations.
    """
    def __init__(self, s3_base: str):
        self.s3_base = s3_base

    def fetch_tables_dict(self):
        bash_response = subprocess.run(
            f'dbt list --output json --select config.materialized:incremental config.materialized:table --exclude resource_type:test',
            capture_output=True, shell=True).stdout.decode("utf-8")
        table_strings = bash_response.split('\n')[:-1]
        tables_dict = {}
        for table_string in table_strings:
            tables_dict[json.loads(table_string)['name']] = json.loads(table_string)
        return tables_dict

    def get_s3_location(self, table_dict):
        schema = table_dict['config'].get('schema')
        name = table_dict['config'].get('alias', table_dict['config'].get('name'))
        s3_location = f's3a://{self.s3_base}/{schema}/{name}'
        return s3_location

    @staticmethod
    def get_partitions(table_dict):
        partition = table_dict['config'].get('partition_by')
        if partition is not None:
            return f"PARTITIONED BY ({' '.join(partition)})"
        else:
            return ""

    def get_alter_command(self, table_dict):
        table_name = f"{table_dict['config']['schema']}.{table_dict['config'].get('alias', table_dict['name'])}"
        s3_path = self.get_s3_location(table_dict)
        partition = self.get_partitions(table_dict)
        alter_template = Template("""
        {% set $var %}
        ALTER TABLE $table_name $partition SET LOCATION $s3_path;
        {% endset %}
        {% do run_query($var) %}
        """)
        alter_command = alter_template.substitute(var=table_dict['name'].replace('.', ''),
                                                  table_name=table_name,
                                                  partition=partition,
                                                  s3_path=s3_path)
        return alter_command

    def generate_macro_file(self, tables_dict):
        f = open("../macros/dune/alter_table_locations.sql", 'w')
        f.write("{% macro alter_table_locations() %}")
        for table, table_dict in tables_dict.items():
            alter_command = self.get_alter_command(table_dict)
            f.write(alter_command)
        f.write("{% endmacro %}")
        f.close()

    def main(self):
        tables_dict = self.fetch_tables_dict()
        self.generate_macro_file(tables_dict)


parser = argparse.ArgumentParser()
parser.add_argument('--s3_base')
args = parser.parse_args()
manager = TableLocationManager(s3_base=args.s3_base)
manager.main()
