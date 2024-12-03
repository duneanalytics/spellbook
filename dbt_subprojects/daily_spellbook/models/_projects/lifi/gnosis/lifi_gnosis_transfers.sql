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

tokens_mapped as (
    select
        *,
        case
            when sendingAssetId = '0x0000000000000000000000000000000000000000'
            then '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' -- WXDAI
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
        and p.blockchain = 'gnosis'
        and p.minute = date_trunc('minute', tokens_mapped.block_time)
)

{{
    add_tx_columns(
        model_cte = 'price_data'
        , blockchain = 'gnosis'
        , columns = ['from']
    )
}}
