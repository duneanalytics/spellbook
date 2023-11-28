import subprocess
import json
import os

env = 'dev'  # Change this based on your needs

# step 1: List dbt models and output in JSON format
dbt_command = ['dbt', 'ls', '--resource-type', 'model', '--output', 'json']
dbt_output_bytes = subprocess.check_output(dbt_command)
dbt_output_str = dbt_output_bytes.decode('utf-8')
dbt_lines = dbt_output_str.splitlines()
dbt_json_objects = [line for line in dbt_lines if line.strip().startswith('{')]
dbt_data_list = [json.loads(obj) for obj in dbt_json_objects]

# step 2: Iterate through each JSON object and categorize based on 'materialized'
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

# # Print the results for dbt models
# print("DBT View Models:")
# print("\n".join(view_models_dbt))

# print("\nDBT Table or Incremental Models:")
# print("\n".join(table_models_dbt))

# step 3: build function to run psql queries set below
def run_psql_command(sql_query):
    psql_command = [
        'psql',
        '-h', postgres_host,
        '-p', str(postgres_port),
        '-U', postgres_user,
        '-t',
        '-c', sql_query
    ]

    # Use subprocess.run without stdin and pass the password through the environment
    psql_process = subprocess.run(
        psql_command,
        text=True,
        env=dict(os.environ, PGPASSWORD=postgres_password),
        capture_output=True  # Capture both stdout and stderr
    )

    # Check for errors
    if psql_process.returncode != 0:
        print("Error executing psql command:")
        print("psql_process.stderr:", psql_process.stderr)
        return []

    # Use stdout instead of psql_process.stdout
    result_lines = psql_process.stdout.splitlines()

    # Remove the last element if it's an empty string
    if not result_lines[-1]:
        result_lines.pop()

    return result_lines

# step 4: Determine PostgreSQL connection details and SQL queries based on the environment
# Set common PostgreSQL connection details
postgres_port = 5432
postgres_user = "hive"

if env == 'dev':
    postgres_host = "dev-spellbook-metastore-db"
    postgres_password = os.environ.get("SH_METASTORE_DEV_PASS")

    # SQL query for dev environment (tables)
    tables_sql_query = f"""
        SELECT DISTINCT REPLACE(d."NAME", 'dbt_jeff_dude_', '') || '.' || t."TBL_NAME"
        FROM "TBLS" t
        JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
        WHERE t."OWNER_TYPE" = 'USER'
            AND t."OWNER" = 'admin'
            AND t."TBL_TYPE" = 'EXTERNAL_TABLE'
            AND d."NAME" LIKE 'dbt_jeff_dude_%';
    """

    # SQL query for dev environment (views)
    views_sql_query = f"""
        SELECT DISTINCT REPLACE(d."NAME", 'dbt_jeff_dude_', '') || '.' || t."TBL_NAME"
        FROM "TBLS" t
        JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
        WHERE t."OWNER_TYPE" = 'USER'
            AND t."OWNER" = 'admin'
            AND t."TBL_TYPE" = 'VIRTUAL_VIEW'
            AND d."NAME" LIKE 'dbt_jeff_dude_%';
    """

elif env == 'prod':
    postgres_host = "prod-metastore-db"
    postgres_password = os.environ.get("SH_METASTORE_PROD_PASS")

    # SQL query for prod environment (tables)
    tables_sql_query = f"""
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

    # SQL query for prod environment (views)
    views_sql_query = f"""
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


# step 5: run psql query for tables
psql_tables = run_psql_command(tables_sql_query)
# Trim whitespace from PostgreSQL tables
psql_tables = [table.strip() for table in psql_tables]

# step 6: run psql query for views
psql_views = run_psql_command(views_sql_query)
# Trim whitespace from PostgreSQL views
psql_views = [view.strip() for view in psql_views]

# # Print the results for PostgreSQL tables
# print("\nPostgreSQL views:")
# print("\n".join(psql_views))

# step 7: Compare psql_views vs. view_models_dbt
print("\nViews in PostgreSQL but not in DBT:")
for view in psql_views:
    if view not in view_models_dbt:
        # Add a print statement for dropping the view
        print(f"DROP VIEW IF EXISTS {view};")

# step 8: Compare psql_tables vs. table_models_dbt
print("\nTables in PostgreSQL but not in DBT:")
for table in psql_tables:
    if table not in table_models_dbt:
        # Add a print statement for dropping the table
        print(f"DROP TABLE IF EXISTS {table};")