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
            f'dbt list --output json --select config.materialized:table',
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


    def get_alter_command(self, table_dict):
        table_name = f"{table_dict['config']['schema']}.{table_dict['config'].get('alias', table_dict['name'])}"
        s3_path = self.get_s3_location(table_dict)
        alter_template = Template("""
        {% set $var %}
        CREATE TABLE $clone_name DEEP CLONE $table_name LOCATION "$s3_path";
        {% endset %}
        {% do run_query($var) %}
        """)
        alter_command = alter_template.substitute(var=table_dict['name'].replace('.', ''),
                                                  clone_name=table_name+'_clone',
                                                  table_name=table_name,
                                                  s3_path=s3_path)
        return alter_command

    def get_delete_command(self, table_dict):
        table_name = f"{table_dict['config']['schema']}.{table_dict['config'].get('alias', table_dict['name'])}"
        s3_path = self.get_s3_location(table_dict)
        drop_template = Template("""
        {% set $var %}
        DROP TABLE $table_name;
        {% endset %}
        {% do run_query($var) %}
        """)
        drop_command = drop_template.substitute(var=table_dict['name'].replace('.', '')+'_drop',
                                                  table_name=table_name,
                                                  s3_path=s3_path)
        return drop_command

    def get_rename_command(self, table_dict):
        table_name = f"{table_dict['config']['schema']}.{table_dict['config'].get('alias', table_dict['name'])}"
        rename_template = Template("""
        {% set $var %}
        ALTER TABLE $clone_name RENAME TO $table_name;
        {% endset %}
        {% do run_query($var) %}
        """)
        rename_command = rename_template.substitute(var=table_dict['name'].replace('.', '')+'_rename',
                                                  clone_name=table_name+'_clone',
                                                  table_name=table_name)
        return rename_command

    def generate_macro_file(self, tables_dict):
        f = open("../dbt_macros/dune/alter_table_locations.sql", 'w')
        f.write("{% macro alter_table_locations() %}")
        for table, table_dict in tables_dict.items():
            # alter_command = self.get_alter_command(table_dict)
            # f.write(alter_command)
            drop_command = self.get_delete_command(table_dict)
            f.write(drop_command)
            rename_command = self.get_rename_command(table_dict)
            f.write(rename_command)
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
