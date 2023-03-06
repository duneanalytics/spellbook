{{ config(
    schema='balancer_v1_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2020-03-13' %}

with v1 as (
    select
        '1' as version,
        tokenOut as token_bought_address,
        tokenAmountOut as token_bought_amount_raw,
        tokenIn as token_sold_address,
        tokenAmountIn as token_sold_amount_raw,
        contract_address as project_contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_index
    from {{ source('balancer_v1_ethereum', 'BPool_evt_LOG_SWAP') }}
    {% if not is_incremental() %}
        where evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
prices as (
    select * from {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
    {% if not is_incremental() %}
        and minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
        and minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)


select
    'ethereum' as blockchain,
    'balancer' as project,
    version,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
    token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
    CAST(token_bought_amount_raw AS DECIMAL(38,0)) as token_bought_amount_raw,
    CAST(token_sold_amount_raw AS DECIMAL(38,0)) as token_sold_amount_raw,
    coalesce(
        (token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    token_bought_address,
    token_sold_address,
    tx.from as taker,
    cast(null as varchar(5)) as maker,
    project_contract_address,
    evt_tx_hash as tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    evt_index,
    '' as trace_address
from v1 trades
inner join {{ source('ethereum', 'transactions') }} tx
    on trades.evt_tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_erc20') }} erc20a
    on trades.token_bought_address = erc20a.contract_address
    and erc20a.blockchain = 'ethereum'
left join {{ ref('tokens_erc20') }} erc20b
    on trades.token_sold_address = erc20b.contract_address
    and erc20b.blockchain = 'ethereum'
left join prices p_bought
    ON p_bought.minute = date_trunc('minute', trades.evt_block_time)
    and p_bought.contract_address = trades.token_bought_address
left join prices p_sold
    on p_sold.minute = date_trunc('minute', trades.evt_block_time)
    and p_sold.contract_address = trades.token_sold_address
