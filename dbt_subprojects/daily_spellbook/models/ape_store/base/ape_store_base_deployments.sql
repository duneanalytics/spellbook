{{
    config(
        alias='deployments',
        schema='ape_store_base',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key='token'
    )
}}

{% set blockchain = 'base' %}

select
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    date_trunc('month', evt_block_time) as block_month,
    '{{blockchain}}' as blockchain,
    token,
    id,
    evt_tx_from as deployer,
    evt_tx_hash as tx_hash
from {{ source("ape_store_base", "Router_evt_CreateToken") }}
