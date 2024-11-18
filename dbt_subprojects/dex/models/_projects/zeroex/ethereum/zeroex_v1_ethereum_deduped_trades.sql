{{  config(
        schema = 'zeroex_v1_ethereum',
        alias = 'deduped_trades',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set blockchain = 'ethereum' %}
{% set table_name = 'zeroex_' ~ blockchain ~ '_api_fills' %}

WITH 
deduped_trades as (
    {{
        zeroex_v1_deduped_trades(
            table_name = table_name,
            start_date = zeroex_v3_start_date
            
        )
    }}

)
select 
    *
from deduped_trades 
