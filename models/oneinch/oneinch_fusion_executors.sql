{{
    config(
        schema = 'oneinch',
        alias = 'fusion_executors',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['resolver_executor', 'chain_id'],
        
    )
}}

with

executors as (
    select
        "from" as resolver_address
        , substr(input, 49, 20) as resolver_executor
        , cast(bytearray_to_uint256(substr(input, 5, 32)) as double) as chain_id -- blockchain id
        , count(*) as executor_promotions
        , min(block_time) as first_promoted_at
        , max(block_time) as last_promoted_at
        , max(tx_hash) as tx_hash_example
    from {{ source('ethereum', 'traces') }}
    where
        "to" in (0xcb8308fcb7bc2f84ed1bea2c016991d34de5cc77, 0xF55684BC536487394B423e70567413faB8e45E26) -- WhitelistRegistry
        and substr(input, 1, 4) = 0xf204bdb9 -- promote(chainId, promotee)
        and block_time >= timestamp '2022-12-25'
        and tx_success
        and success
        and call_type = 'call'
    group by 1, 2, 3
)


select
    fr.address as resolver_address
    , fr.name as resolver_name
    , fr.status as resolver_status
    , fr.last_changed_at as resolver_last_changed_at
    , fr.kyc as resolver_kyc
    , resolver_executor
    , coalesce(blockchain, cast(chain_id as varchar)) as blockchain
    , chain_id
    , executor_promotions
    , first_promoted_at
    , last_promoted_at
    , tx_hash_example
from {{ ref('oneinch_fusion_resolvers') }} as fr
join executors on fr.address = executors.resolver_address
left join {{ ref('evms_info') }} using(chain_id)
order by fr.name, resolver_executor