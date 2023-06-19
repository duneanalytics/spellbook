{{ config(
    schema = 'xchange_bnb'
    ,alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "xchange",
                                \'["mike-x7f"]\') }}'
    )
}}

{% set project_start_date = '2023-05-31' %}

with dexs as (
    -- Xchange
    SELECT
        t.evt_block_time as block_time,
        t.to as taker,
        '' as maker,
        case when amount0Out  = 0 then amount1Out else amount0Out end as token_bought_amount_raw,
        case when amount0In = 0 then amount1In else amount0In end as token_sold_amount_raw,
        null as amount_usd,
        case when amount0Out  = 0 then f.token1 else f.token0 end as token_bought_address,
        case when amount0In = 0 then f.token1 else f.token0 end as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        '' as trace_address,
        t.evt_index
    FROM
        {{ source('xchange_bnb', 'XchangePair_evt_Swap') }} t
        inner join {{ source('xchange_bnb', 'XchangeFactory_evt_PairCreated') }} f 
            on f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)
select
    'bnb' as blockchain,
    'xchange' as project,
    '1' as version,
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
    coalesce(dexs.taker, tx.from) AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('bnb', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_erc20') }} erc20a 
    on erc20a.contract_address = dexs.token_bought_address 
    and erc20a.blockchain = 'bnb'
left join {{ ref('tokens_erc20') }} erc20b 
    on erc20b.contract_address = dexs.token_sold_address 
    and erc20b.blockchain = 'bnb'
left join {{ source('prices', 'usd') }} p_bought 
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    and p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold 
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    and p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    