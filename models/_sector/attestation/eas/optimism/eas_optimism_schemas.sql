{{
  config(
    schema = 'eas_optimism',
    alias = 'schemas',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with eas as (
  {{
    eas_schemas(
      blockchain = 'optimism',
      project = 'eas',
      version = '1',
      decoded_project_name = 'attestationstation_v1'
    )
  }}
)
, dedupe as (
  select distinct
    schema_uid
  from
  (
    select
      schema_uid
      , count(1) as count
    from
      eas
    group by
      schema_uid
    having
      count(1) > 1
    )
)
select
  *
from
  eas
where
  schema_uid not in (
    select
      schema_uid
    from
      dedupe
  )