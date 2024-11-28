{{ config(
    schema = 'lifi_gnosis',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with source_data as (
    {{ lifi_extract_bridge_data('gnosis') }}
),

price_data as (
    select 
        source_data.*,
        p.price * cast(source_data.minAmount as double) / power(10, p.decimals) as amount_usd
    from source_data
    left join {{ source('prices', 'usd') }} p 
        on cast(p.contract_address as varchar) = source_data.sendingAssetId
        and p.blockchain = 'gnosis'
        and p.minute = date_trunc('minute', source_data.block_time)
)

{{
    add_tx_columns(
        model_cte = 'price_data'
        , blockchain = 'gnosis'
        , columns = ['from']
    )
}}
