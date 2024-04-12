{{
    config(
        schema = 'oneinch',
        alias = 'fusion_executors',
        materialized = 'table',
        unique_key = ['resolver_address', 'resolver_executor', 'chain_id']
    )
}}

with

executors as (
    select
        promoter as resolver_address
        , promotee as resolver_executor
        , chainId as chain_id
        , min(evt_block_time) as first_promoted_at
        , max(evt_block_time) as last_promoted_at
    from (
        select promoter, promotee, chainId, evt_block_time
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV1_evt_Promotion') }}
        union all
        select promoter, promotee, chainId, evt_block_time
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV2_evt_Promotion') }}
    )
    group by 1, 2, 3
)

-- output --

select 
    blockchain
    , resolver_address
    , resolver_executor
    , chain_id
    , first_promoted_at
    , last_promoted_at
from executors
left join {{ ref('oneinch_blockchains') }} using(chain_id)