{{ config(
    schema = 'lifi_arbitrum',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with source_data as (
    {{ lifi_extract_bridge_data('arbitrum') }}
),

tokens_mapped as (
    select
        *,
        case
            when sendingAssetId = '0x0000000000000000000000000000000000000000'
            then '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH
            when sendingAssetId = '0x3405a1bd46b85c5c029483fbecf2f3e611026e45'
            then '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC
            else sendingAssetId
        end as sendingAssetId_adjusted
    from source_data
),

price_data as (
    select 
        tokens_mapped.*,
        p.price * cast(tokens_mapped.minAmount as double) / power(10, p.decimals) as amount_usd
    from tokens_mapped
    left join {{ source('prices', 'usd') }} p 
        on cast(p.contract_address as varchar) = tokens_mapped.sendingAssetId_adjusted
        and p.blockchain = 'arbitrum'
        and p.minute = date_trunc('minute', tokens_mapped.block_time)
)

{{
    add_tx_columns(
        model_cte = 'price_data'
        , blockchain = 'arbitrum'
        , columns = ['from']
    )
}}
