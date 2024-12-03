{{ config(
    schema = 'lifi_avalanche_c',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with source_data as (
    {{ lifi_extract_bridge_data('avalanche_c') }}
),

tokens_mapped as (
    select
        *,
        case
            when sendingAssetId = '0x0000000000000000000000000000000000000000'
            then '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- WAVAX
            else sendingAssetId
        end as sending_asset_price_address
    from source_data
),

price_data as (
    select 
        tokens_mapped.*,
        p.price * cast(tokens_mapped.minAmount as double) / power(10, p.decimals) as amount_usd
    from tokens_mapped
    left join {{ source('prices', 'usd') }} p 
        on cast(p.contract_address as varchar) = tokens_mapped.sending_asset_price_address
        and p.blockchain = 'avalanche_c'
        and p.minute = date_trunc('minute', tokens_mapped.block_time)
)

{{
    add_tx_columns(
        model_cte = 'price_data'
        , blockchain = 'avalanche_c'
        , columns = ['from']
    )
}}
