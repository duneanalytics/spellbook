{{ 
    config(
        schema = 'safe_optimism',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    ) 
}}

{%- set network_config = get_safe_network_config('optimism') -%}
{%- set project_start_date = network_config.start_date -%}
{% set date_filter = false %}

{{
    safe_native_transfers(
        blockchain = 'optimism',
        native_token_symbol = 'ETH',
        project_start_date = project_start_date,
        date_filter = date_filter
    )
}}

union all

select
    'optimism' as blockchain,
    'ETH' as symbol,
    s.address,
    try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
    CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
    r.evt_block_time as block_time,
    CAST(r.value AS INT256) as amount_raw,
    r.evt_tx_hash as tx_hash,
    cast(r.evt_index as varchar) as trace_address,
    p.price * CAST(r.value AS DOUBLE) / 1e18 AS amount_usd
from {{ source('erc20_optimism', 'evt_Transfer') }} r
join {{ ref('safe_optimism_safes') }} s
    on r.to = s.address
left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = 'ETH'
    and p.minute = date_trunc('minute', r.evt_block_time)
where
    LOWER(CAST(r.contract_address AS VARCHAR)) = LOWER('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000')
    and r.value > UINT256 '0'
    {% if not is_incremental() %}
    and r.evt_block_time > TIMESTAMP '{{project_start_date}}'
    {% elif is_incremental() %}
        {% if date_filter %}
        and r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}
    {% endif %}

union all

select
    'optimism' as blockchain,
    'ETH' as symbol,
    s.address,
    try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
    CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
    r.evt_block_time as block_time,
    -CAST(r.value AS INT256) as amount_raw,
    r.evt_tx_hash as tx_hash,
    cast(r.evt_index as varchar) as trace_address,
    -p.price * CAST(r.value AS DOUBLE) / 1e18 AS amount_usd
from {{ source('erc20_optimism', 'evt_Transfer') }} r
join {{ ref('safe_optimism_safes') }} s
    on r."from" = s.address
left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = 'ETH'
    and p.minute = date_trunc('minute', r.evt_block_time)
where
    LOWER(CAST(r.contract_address AS VARCHAR)) = LOWER('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000')
    and r.value > UINT256 '0'
    {% if not is_incremental() %}
    and r.evt_block_time > TIMESTAMP '{{project_start_date}}'
    {% elif is_incremental() %}
        {% if date_filter %}
        and r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}
    {% endif %}