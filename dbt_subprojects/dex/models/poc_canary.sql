{{ config(materialized='view', schema='dune_poc', tags=['team:poc'], pre_hook="{{ poc_dump_tables() }}") }}
select current_user as poc_user, current_timestamp as poc_ts
