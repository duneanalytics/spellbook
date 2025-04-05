{{ config(
    
    schema = 'lifi_v2_optimism',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "lifi_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-10-27' %} -- min(evet_block_time) in swapped & swapped generic events

WITH 

{% set trade_event_tables = [
    source('lifi_optimism', 'LiFiDiamond_v2_evt_AssetSwapped')
    ,source('lifi_optimism', 'LiFiDiamond_v2_evt_LiFiSwappedGeneric')
] %}

dexs as (
    {% for trade_tables in trade_event_tables %}
        SELECT 
            evt_block_time as block_time,
            CAST(NULL as VARBINARY) as maker, 
            toAmount as token_bought_amount_raw,
            fromAmount as token_sold_amount_raw,
            CAST(NULL as double) as amount_usd,
            CASE 
                WHEN toAssetId IN (0x, 0x0000000000000000000000000000000000000000)
                THEN 0x4200000000000000000000000000000000000006 -- weth
                ELSE toAssetId
            END as token_bought_address,
            CASE 
                WHEN fromAssetId IN (0x, 0x0000000000000000000000000000000000000000)
                THEN 0x4200000000000000000000000000000000000006 -- weth
                ELSE fromAssetId
            END as token_sold_address,
            contract_address as project_contract_address,
            evt_tx_hash as tx_hash, 
            ARRAY[-1] AS trace_address,
            evt_index
        FROM {{ trade_tables }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    'optimism' as blockchain,
    'lifi' as project,
    '2' as version,
    cast(date_trunc('DAY', dexs.block_time) as date) as block_date,
    cast(date_trunc('MONTH', dexs.block_time) as date) as block_month,
    dexs.block_time,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    tx."from" AS taker, -- no taker in swap event
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('optimism', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
