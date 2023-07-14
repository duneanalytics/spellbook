{{ config(
	tags=['legacy'],
	
    schema = 'lifi_v2_fantom',
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "lifi_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-10-20' %} -- min(evet_block_time) in swapped & swapped generic events

WITH 

{% set trade_event_tables = [
    source('lifi_fantom', 'LiFiDiamond_v2_evt_AssetSwapped')
    ,source('lifi_fantom', 'LiFiDiamond_v2_evt_LiFiSwappedGeneric')
] %}

dexs as (
    {% for trade_tables in trade_event_tables %}
        SELECT 
            evt_block_time as block_time,
            '' as maker, 
            toAmount as token_bought_amount_raw,
            fromAmount as token_sold_amount_raw,
            CAST(NULL as double) as amount_usd,
            CASE 
                WHEN toAssetId IN ('0', 'O', '0x0000000000000000000000000000000000000000')
                THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm 
                ELSE toAssetId
            END as token_bought_address,
            CASE 
                WHEN fromAssetId IN ('0', 'O', '0x0000000000000000000000000000000000000000')
                THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm 
                ELSE fromAssetId
            END as token_sold_address,
            contract_address as project_contract_address,
            evt_tx_hash as tx_hash, 
            CAST(ARRAY() as array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_tables }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    'fantom' as blockchain,
    'lifi' as project,
    '2' as version,
    try_cast(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    tx.from AS taker, -- no taker in swap event
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('fantom', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_erc20_legacy') }} erc20a
    on erc20a.contract_address = dexs.token_bought_address
    and erc20a.blockchain = 'fantom'
left join {{ ref('tokens_erc20_legacy') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'fantom'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'fantom'
    {% if not is_incremental() %}
    and p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'fantom'
    {% if not is_incremental() %}
    and p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}