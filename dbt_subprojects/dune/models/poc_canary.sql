{{ config(materialized='view', tags=['team:poc']) }}
select current_user() as poc_user, current_timestamp as poc_ts
-- test
