{{ config(
    schema = 'paraswap_v5_fantom',
    alias = 'trades',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "paraswap_v5",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-01-21' %} -- min(evet_block_time) in bought & swapped events

WITH 

{% set trade_event_tables = [
    source('paraswap_fantom', 'AugustusSwapperV5_evt_BoughtV3')
    ,source('paraswap_fantom', 'AugustusSwapperV5_evt_SwappedV3')
    ,source('paraswap_fantom', 'AugustusSwapperV5_evt_SwappedDirect')
] %}

dexs as (
    {% for trade_tables in trade_event_tables %}
        SELECT 
            evt_block_time as block_time,
            evt_block_number as block_number,
            beneficiary as taker, 
            initiator as maker, 
            receivedAmount as token_bought_amount_raw,
            srcAmount as token_sold_amount_raw,
            CAST(NULL as double) as amount_usd,
            CASE 
                WHEN destToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 -- wftm 
                ELSE destToken
            END as token_bought_address,
            CASE 
                WHEN srcToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 -- wftm 
                ELSE srcToken
            END as token_sold_address,
            contract_address as project_contract_address,
            evt_tx_hash as tx_hash, 
            CAST(ARRAY[-1] as array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_tables }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    'fantom' as blockchain,
    'paraswap' as project,
    '5' as version,
    cast(date_trunc('day', dexs.block_time) as date) as block_date,
    cast(date_trunc('month', dexs.block_time) as date) as block_month,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx."from") AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('fantom', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    and dexs.block_number = tx.block_number
    {% if not is_incremental() %}
    and tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
left join {{ source('tokens', 'erc20') }} erc20a
    on erc20a.contract_address = dexs.token_bought_address
    and erc20a.blockchain = 'fantom'
left join {{ source('tokens', 'erc20') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'fantom'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'fantom'
    {% if not is_incremental() %}
    and p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'fantom'
    {% if not is_incremental() %}
    and p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}