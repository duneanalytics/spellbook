{{
    config(
        alias='deployments',
        schema='ape_store_ethereum',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key='token'
    )
}}

{% set project_start_date = '2024-09-06' %}
{% set blockchain = 'ethereum' %}

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
join ethereum.transactions on (
    evt_block_time = block_time
    and evt_tx_hash = hash
    and (
        {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
        {% else %}
        block_time >= timestamp '{{project_start_date}}'
        {% endif %}
    )
)
where 
    {% if is_incremental() %}
    {{ incremental_predicate('evt_block_time') }}
    {% else %}
    evt_block_time >= timestamp '{{project_start_date}}'
    {% endif %}
