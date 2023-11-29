import subprocess
import json
import os

# Constants
DEV_ENV = 'dev'
PROD_ENV = 'prod'

def generate_tables_query(env):
    if env == DEV_ENV:
        return """
            SELECT DISTINCT REPLACE(d."NAME", 'dbt_jeff_dude_', '') || '.' || t."TBL_NAME"
            FROM "TBLS" t
            JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
            WHERE t."OWNER_TYPE" = 'USER'
                AND t."OWNER" = 'admin'
                AND t."TBL_TYPE" = 'EXTERNAL_TABLE'
                AND d."NAME" LIKE 'dbt_jeff_dude_dex%';
        """
    elif env == PROD_ENV:
        return """
            SELECT DISTINCT d."NAME" || '.' || t."TBL_NAME"
            FROM "TBLS" t
            JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
            JOIN "TABLE_PARAMS" tp ON tp."TBL_ID" = t."TBL_ID" 
            WHERE tp."PARAM_KEY" = 'dune.data_explorer.category' 
                AND tp."PARAM_VALUE" = 'abstraction' 
                AND t."OWNER_TYPE" = 'USER' 
                AND t."OWNER" = 'spellbook'
                AND t."TBL_TYPE" = 'EXTERNAL_TABLE';
        """
    else:
        raise ValueError("Invalid environment. Use 'dev' or 'prod'.")

def generate_views_query(env):
    if env == DEV_ENV:
        return """
            SELECT DISTINCT REPLACE(d."NAME", 'dbt_jeff_dude_', '') || '.' || t."TBL_NAME"
            FROM "TBLS" t
            JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
            WHERE t."OWNER_TYPE" = 'USER'
                AND t."OWNER" = 'admin'
                AND t."TBL_TYPE" = 'VIRTUAL_VIEW'
                AND d."NAME" LIKE 'dbt_jeff_dude_dex%';
        """
    elif env == PROD_ENV:
        return """
            SELECT DISTINCT d."NAME" || '.' || t."TBL_NAME"
            FROM "TBLS" t
            JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
            JOIN "TABLE_PARAMS" tp ON tp."TBL_ID" = t."TBL_ID" 
            WHERE tp."PARAM_KEY" = 'dune.data_explorer.category' 
                AND tp."PARAM_VALUE" = 'abstraction' 
                AND t."OWNER_TYPE" = 'USER' 
                AND t."OWNER" = 'spellbook'
                AND t."TBL_TYPE" = 'VIRTUAL_VIEW';
        """
    else:
        raise ValueError("Invalid environment. Use 'dev' or 'prod'.")

def run_psql_command(sql_query, env):
    postgres_host = "dev-spellbook-metastore-db" if env == DEV_ENV else "prod-metastore-db"
    postgres_password = os.environ.get("SH_METASTORE_DEV_PASS") if env == DEV_ENV else os.environ.get("SH_METASTORE_PROD_PASS")

    psql_command = [
        'psql',
        '-h', postgres_host,
        '-p', '5432',
        '-U', 'hive',
        '-t',
        '-c', sql_query
    ]

    psql_process = subprocess.run(
        psql_command,
        text=True,
        env=dict(os.environ, PGPASSWORD=postgres_password),
        capture_output=True
    )

    if psql_process.returncode != 0:
        print("Error executing psql command:")
        print("psql_process.stderr:", psql_process.stderr)
        return []

    result_lines = psql_process.stdout.splitlines()

    if not result_lines[-1]:
        result_lines.pop()

    return result_lines

# Main script
env = DEV_ENV  # Change this based on your needs

# Step 1: List dbt models and output in JSON format
dbt_command = ['dbt', 'ls', '--resource-type', 'model', '--output', 'json']
# dbt_command = ['dbt', 'ls', '--resource-type', 'model', '--output', 'json', '--exclude', 'dex_trades_beta']
dbt_output_bytes = subprocess.check_output(dbt_command)
dbt_output_str = dbt_output_bytes.decode('utf-8')
dbt_lines = dbt_output_str.splitlines()
dbt_json_objects = [line for line in dbt_lines if line.strip().startswith('{')]
dbt_data_list = [json.loads(obj) for obj in dbt_json_objects]

# Iterate through each JSON object and categorize based on 'materialized'
view_models_dbt = []
table_models_dbt = []

for data in dbt_data_list:
    materialized = data.get('config', {}).get('materialized', '').lower()
    schema = data.get('config', {}).get('schema', 'schema_not_found')
    alias = data.get('config', {}).get('alias', 'alias_not_found')

    if materialized == 'view':
        view_models_dbt.append(f"{schema}.{alias}")
    elif materialized == 'table' or materialized == 'incremental':
        table_models_dbt.append(f"{schema}.{alias}")

# Generate SQL queries
tables_sql_query = generate_tables_query(env)
views_sql_query = generate_views_query(env)

# Run psql queries for tables and views
psql_tables = run_psql_command(tables_sql_query, env)
psql_views = run_psql_command(views_sql_query, env)

# Trim whitespace from PostgreSQL tables and views
psql_tables = [table.strip() for table in psql_tables]
psql_views = [view.strip() for view in psql_views]

# Compare psql_views vs. view_models_dbt
print("\nViews in PostgreSQL but not in DBT:")
for view in psql_views:
    if view not in view_models_dbt:
        print(f"DROP VIEW IF EXISTS {view};")

# Compare psql_tables vs. table_models_dbt
print("\nTables in PostgreSQL but not in DBT:")
for table in psql_tables:
    if table not in table_models_dbt:
        print(f"DROP TABLE IF EXISTS {table};")
