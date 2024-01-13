{{ config(
    alias = 'trades'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2022-05-12' %}

with dexs as (
    -- Constant Product Pool
    SELECT
        'trident-cpp' as version,
        t.evt_block_time as block_time,
        recipient as taker,
        CAST(NULL AS VARBINARY)as maker,
        amountOut as token_bought_amount_raw,
        amountIn as token_sold_amount_raw,
        null as amount_usd,
        tokenOut as token_bought_address,
        tokenIn as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    FROM
        {{ source('sushi_optimism', 'ConstantProductPool_evt_Swap') }} t
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}

    UNION ALL
    -- Stable Pool
    SELECT
        'trident-sp' as version,
        t.evt_block_time as block_time,
        recipient as taker,
        CAST(NULL AS VARBINARY)as maker,
        amountOut as token_bought_amount_raw,
        amountIn as token_sold_amount_raw,
        null as amount_usd,
        tokenOut as token_bought_address,
        tokenIn as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    FROM
        {{ source('sushi_optimism', 'StablePool_evt_Swap') }} t
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)
select
    'optimism' as blockchain,
    'sushiswap' as project,
    version,
    try_cast(date_trunc('DAY', dexs.block_time) as date) as block_date,
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
    dexs.token_bought_amount_raw  AS token_bought_amount_raw,
    dexs.token_sold_amount_raw  AS token_sold_amount_raw,
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
    dexs.evt_index
from dexs
inner join {{ source('optimism', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
left join {{ source('tokens', 'erc20') }} erc20a
    on erc20a.contract_address = dexs.token_bought_address
    and erc20a.blockchain = 'optimism'
left join {{ source('tokens', 'erc20') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'optimism'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    and p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    and p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    