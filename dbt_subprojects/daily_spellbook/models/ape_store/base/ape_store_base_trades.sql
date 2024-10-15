{{ config(
    alias = 'trades',
    schema = 'ape_store_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'blockchain', 'tx_hash', 'tx_index']
   )
}}

{# TODO: revert back to 2024-04-04 #}
{% set project_start_date = '2024-09-20' %}
{% set blockchain = 'base' %}
{% set weth_contract_address = '0x4200000000000000000000000000000000000006' %}

with
    deployments as (
        select * from {{ref('ape_store_base_deployments')}}
    ),
    bondingcurvetrades as (
        select
            evt_block_time as block_time,
            date_trunc('day', evt_block_time) as block_date,
            date_trunc('month', evt_block_time) as block_month,
            '{{blockchain}}' as blockchain,
            'Bonding Curve' as platform,
            if(amount0out > 0, 'Buy', 'Sell') as type,
            (amount1in + amount1out) / 1e18 * price as amount_usd,
            (amount0out + amount1out) / 1e18 as token_bought_amount,
            if(amount0out > 0, token.symbol, 'WETH') token_bought_symbol,
            if(
                amount0out > 0, token, {{ weth_contract_address }}
            ) as token_bought_address,
            (amount0in + amount1in) / 1e18 as token_sold_amount,
            if(amount0out > 0, 'WETH', token.symbol) as token_sold_symbol,
            if(
                amount0out > 0, {{ weth_contract_address }}, token
            ) as token_sold_address,
            sender as user,
            evt_tx_hash as tx_hash,
            evt_index as tx_index
        from {{ source('ape_store_base', 'Router_evt_swap') }} as swaps
        left join
            {{ source('tokens', 'erc20') }} as token
            on (
                token.blockchain = '{{blockchain}}'
                and token.contract_address = swaps.token
            )
        left join
            {{ source('prices', 'usd') }} as ethPrice
            on (
                ethPrice.blockchain = '{{blockchain}}'
                and ethPrice.contract_address = {{ weth_contract_address }}
                and ethPrice.minute = date_trunc('minute', evt_block_time)
                and (
                    {% if is_incremental() %}
                    {{ incremental_predicate('ethPrice.minute') }}
                    {% else %}
                    ethPrice.minute >= timestamp '{{project_start_date}}'
                    {% endif %}
                )
            )
        where
            {% if is_incremental() %}
            {{ incremental_predicate('evt_block_time') }}
            {% else %}
            evt_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    dextrades as (
        -- Buys
        select distinct
            trades.block_time,
            date_trunc('day', trades.block_time) as block_date,
            date_trunc('month', trades.block_time) as block_month,
            trades.blockchain,
            'DEX' as platform,
            if(token_sold_address = {{ weth_contract_address }}, 'Buy', 'Sell') as type,
            amount_usd,
            token_bought_amount,
            token_bought_symbol,
            token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_address,
            tx_from as user,
            trades.tx_hash,
            evt_index as tx_index
        from {{ source('dex', 'trades') }} as trades
        join
            deployments
            on (
                trades.blockchain = deployments.blockchain
                and trades.block_month >= deployments.block_month
                and token_bought_address = token
            )
        where
            trades.blockchain = '{{blockchain}}'
            and {% if is_incremental() %}
                {{ incremental_predicate("trades.block_time") }}
            {% else %} trades.block_time >= timestamp '{{project_start_date}}'
            {% endif %}

        union all

        -- Sells
        select distinct
            trades.block_time,
            date_trunc('day', trades.block_time) as block_date,
            date_trunc('month', trades.block_time) as block_month,
            trades.blockchain,
            'DEX' as platform,
            if(token_sold_address = {{ weth_contract_address }}, 'Buy', 'Sell') as type,
            amount_usd,
            token_bought_amount,
            token_bought_symbol,
            token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_address,
            tx_from as user,
            trades.tx_hash,
            evt_index as tx_index
        from {{ source('dex', 'trades') }} as trades
        join
            deployments
            on (
                trades.blockchain = deployments.blockchain
                and trades.block_month >= deployments.block_month
                and token_sold_address = token
            )
        where
            trades.blockchain = '{{blockchain}}'
            and {% if is_incremental() %}
                {{ incremental_predicate("trades.block_time") }}
            {% else %} trades.block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    )
select *
from bondingcurvetrades
union all
select *
from dextrades
