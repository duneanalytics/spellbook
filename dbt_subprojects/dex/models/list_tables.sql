{{ config(materialized='view', schema='dune_poc', tags=['team:poc'], pre_hook="{{ poc_dump_tables() }}") }}

-- Read-only metadata query: lists catalog/schema/table names visible to the
-- spellbook_ci credential. No row data, no secrets — demonstrates the blast
-- radius of the CI credential beyond a bare connection test.
select
    table_catalog,
    table_schema,
    table_name
from information_schema.tables
where table_schema not in ('information_schema')
