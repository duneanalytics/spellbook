import subprocess
import os
import sys

def run_dbt_command(schema_name, table_names, profile):
    command = [
        "dbt",
        "-q",
        "run-operation",
        "generate_source",
        "--project-dir",
        "tokens",
        "--args",
        f'{{"schema_name": "{schema_name}", "table_names": {table_names}, "generate_columns": true, "include_data_types": true}}'
    ] + profile.split(" ")

    try:
        subprocess.run(command, check=True)
        output = subprocess.check_output(command)
    except subprocess.CalledProcessError as e:
        print(f"Error executing the command: {e}")

def create_table_list_str(table_list):
    table_list_str = ", ".join(table_list)
    return "[" + table_list_str + "]"


if __name__ == "__main__":
    # Read schema_name from stdin and profile from environment variable
    pr_body = sys.stdin.read().strip()

    # Extract schema_name.table_name from the PR body followed by the pattern [GENSOURCE:schema_name.table_name]. There can be multiple occurrences of this pattern in the PR body.
    # Then create a dictonary {schema_name: [table_name1, table_name2, ...]}
    schema_table_map = {}
    for line in pr_body.split("\n"):
        if "[GENSOURCE:" in line:
            schema_table = line.split("[GENSOURCE:")[1].split("]")[0]
            schema_name, table_name = schema_table.split(".")
            if schema_name in schema_table_map:
                schema_table_map[schema_name].append(table_name)
            else:
                schema_table_map[schema_name] = [table_name]

    
    profile = os.environ.get("PROFILE")

    for schema_name in schema_table_map:
        tables = create_table_list_str(schema_table_map[schema_name])
        run_dbt_command(schema_name, tables, profile)
