{{
    config(
        schema = 'oneinch',
        alias = alias('fusion_executors'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['resolver_executor', 'chain_id'],
        tags = ['dunesql']
    )
}}

with

executors as (
    select
        "from" as resolver_address
        , substr(input, 49, 20) as resolver_executor
        , cast(bytearray_to_uint256(substr(input, 5, 32)) as double) as id -- blockchain id
        , count(*) as executor_promotions
        , min(block_time) as first_time
        , max(block_time) as last_time
        , max(tx_hash) as tx_hash_example
    from {{ source('ethereum', 'traces') }}
    where
        "to" = 0xcb8308fcb7bc2f84ed1bea2c016991d34de5cc77
        and substr(input, 1, 4) = 0xf204bdb9 -- promote(chainId, promotee)
        and block_time >= timestamp '2022-12-25'
        and tx_success
        and success
        and call_type = 'call'
    group by 1, 2, 3
)

select
      resolver_address
    , resolver_name
    , resolver_status
    , resolver_last_changed_at
    , resolver_kyc
    , resolver_executor
    , coalesce(blockchain, cast(id as varchar)) as blockchain
    , id as chain_id
    , executor_promotions
    , first_time
    , last_time
    , tx_hash_example
from {{ ref('oneinch_fusion_resolvers') }}
join executors using(resolver_address)
left join {{ ref('evms_info') }} using(id)
order by resolver_name, resolver_executor