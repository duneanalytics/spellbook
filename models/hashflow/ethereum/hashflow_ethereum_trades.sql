{{ config(
    tags=[ 'prod_exclude'],
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "hashflow",
                                \'["justabi", "jeff-dude"]\') }}'
    )
}}

{#
## model not yet migrated to dunesql syntax and excluded in prod on both engines due to complexity
## needs refactoring to read from decoded tables
 #}

{% set project_start_date = '2021-04-28' %}

with hashflow_trades as (
    select *
    from {{ ref('hashflow_ethereum_raw_trades') }}
    where fill_status is true -- successful trade
    {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

ethereum_transactions as (
    select *
    from {{ source('ethereum', 'transactions') }}
    where block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

erc20_tokens as (
    select *
    from {{ source('tokens', 'erc20') }}
    where blockchain = 'ethereum'
)

select
    'ethereum' AS blockchain,
    'hashflow' as project,
    '1' as version,
    block_date,
    hashflow_trades.block_time,
    hashflow_trades.maker_symbol as token_bought_symbol,
    hashflow_trades.taker_symbol as token_sold_symbol,
    case when lower(hashflow_trades.maker_symbol) > lower(hashflow_trades.taker_symbol)
            then concat(hashflow_trades.taker_symbol, '-', hashflow_trades.maker_symbol)
        else concat(hashflow_trades.maker_symbol, '-', hashflow_trades.taker_symbol) end as token_pair,
    hashflow_trades.maker_token_amount as token_bought_amount,
    hashflow_trades.taker_token_amount as token_sold_amount,
    CAST(hashflow_trades.maker_token_amount * power(10, erc20a.decimals) AS DECIMAL(38,0)) as token_bought_amount_raw,
    CAST(hashflow_trades.taker_token_amount * power(10, erc20b.decimals) AS DECIMAL(38,0)) as token_sold_amount_raw,
    hashflow_trades.amount_usd,
    hashflow_trades.maker_token as token_bought_address,
    hashflow_trades.taker_token as token_sold_address,
    hashflow_trades.trader as taker,
    hashflow_trades.pool as maker,
    hashflow_trades.router_contract as project_contract_address,
    hashflow_trades.tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    '' as trace_address,
    case when hashflow_trades.composite_index <> -1 then hashflow_trades.composite_index end as evt_index
from hashflow_trades
inner join ethereum_transactions tx
    on hashflow_trades.tx_hash = tx.hash
left join erc20_tokens erc20a
    on erc20a.contract_address = hashflow_trades.maker_token
left join erc20_tokens erc20b
    on erc20b.contract_address = hashflow_trades.taker_token
;