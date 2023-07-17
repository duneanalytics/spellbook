{{
    config(
        schema='oneinch',
        alias = alias('fusion_executors'),
        materialized='table',
        file_format='delta',
        unique_key = ['resolver_executor', 'chain_id']
    )
}}


---- SPARK QUERY ------
with

chains as (
    select *
    from (values
        ('ethereum', 1), -- mainnet
        ('bnb', 56),
        ('ethereum_kovan', 42), -- kovan
        ('optimism', 10), -- optimistic
        ('polygon', 137), -- matic
        ('arbitrum', 42161),
        ('gnosis', 100), -- xdai
        ('avalanche_c', 43114), -- avax
        ('fantom', 250),
        ('aurora', 1313161554),
        ('klaytn', 8217)
    ) as t(chain, chain_id)
)

, names as (
    select *
    from (values
        ('0xf63392356a985ead50b767a3e97a253ff870e91a', '1inch Labs', true),
        ('0xa260f8b7c8f37c2f1bc11b04c19902829de6ac8a', 'Arctic Bastion', true),
        ('0xcfa62f77920d6383be12c91c71bd403599e1116f', 'The Open DAO resolver', true),
        ('0xe023f53f735c196e4a028233c2ee425957812a41', 'Seawise', true),
        ('0x754bcbaf851f94ca0065d0d06d53b168daab17b8', 'Alpha', false),
        ('0xb2153caa185484fd377f488d89143a7fd76695ce', 'Laertes', true),
        ('0xc975671642534f407ebdcaef2428d355ede16a2c', 'OTEX', true),
        ('0xd7f6f541d4210550ca56f7b4c4a549efd4cafb49', 'The T Resolver', true),
        ('0x21b7db78e76dcd100f717206ee655daab2de118c', 'Spider Labs', true),
        ('0x12e5ceb5c14f3a1a9971da154f6530c1cf7274ac', 'Rosato LLC', true),
        ('0xee230dd7519bc5d0c9899e8704ffdc80560e8509', 'Kinetex Labs Resolver', true),
        ('0xaf3803348f4f1f527a8b6f611c30c8702bacd2af', 'Resolver 0xaf38..d2af', true),
        ('0xa6219c7d74edeb12d74a3c664f7aaeb7d01ab902', 'Resolver 0xa621..b902', false),
        ('0x74c629c4096e234029c78c7760dc0aadb717adb0', 'Resolver 0x74c6..adb0', false),
        ('0x7c7047337995c338c1682f12bc38d4e4108309bb', 'Resolver 0x7c70..09bb', false)
    ) as t(resolver_address, resolver_name, kyc)
)

, traces as (
    select 
        tx_hash
        , `from` as resolver_address
        , '0x'||substring(input, 99, 40) as resolver_executor
        , bytea2numeric_v3(substring(input, 11, 64)) as chain_id
    from {{ source('ethereum', 'traces') }}
    where `to` = '0xcb8308fcb7bc2f84ed1bea2c016991d34de5cc77'
        and substring(input, 1, 10) = '0xf204bdb9'
        and block_time >= '2022-12-25'
        and tx_success
        and success
)

select distinct
    resolver_address
    , resolver_executor
    , coalesce(chain, cast(chain_id as string)) as blockchain
    , chain_id
    , resolver_name
    , kyc
    , max(tx_hash) over(partition by resolver_executor, chain_id) as tx_hash_example
from traces
left join names using(resolver_address)
left join chains using(chain_id)
order by resolver_name, resolver_executor




---- TRINO QUERY -----

/*
with

chains as (
    select *
    from (values
        ('ethereum', 1), -- mainnet
        ('bnb', 56),
        ('ethereum_kovan', 42), -- kovan
        ('optimism', 10), -- optimistic
        ('polygon', 137), -- matic
        ('arbitrum', 42161),
        ('gnosis', 100), -- xdai
        ('avalanche_c', 43114), -- avax
        ('fantom', 250),
        ('aurora', 1313161554),
        ('klaytn', 8217)
    ) as t(chain, chain_id)
)

, names as (
    select *
    from (values
        (0xf63392356a985ead50b767a3e97a253ff870e91a, '1inch Labs', true),
        (0xa260f8b7c8f37c2f1bc11b04c19902829de6ac8a, 'Arctic Bastion', true),
        (0xcfa62f77920d6383be12c91c71bd403599e1116f, 'The Open DAO resolver', true),
        (0xe023f53f735c196e4a028233c2ee425957812a41, 'Seawise', true),
        (0x754bcbaf851f94ca0065d0d06d53b168daab17b8, 'Alpha', false),
        (0xb2153caa185484fd377f488d89143a7fd76695ce, 'Laertes', true),
        (0xc975671642534f407ebdcaef2428d355ede16a2c, 'OTEX', true),
        (0xd7f6f541d4210550ca56f7b4c4a549efd4cafb49, 'The T Resolver', true),
        (0x21b7db78e76dcd100f717206ee655daab2de118c, 'Spider Labs', true),
        (0x12e5ceb5c14f3a1a9971da154f6530c1cf7274ac, 'Rosato LLC', true),
        (0xee230dd7519bc5d0c9899e8704ffdc80560e8509, 'Kinetex Labs Resolver', true),
        (0xaf3803348f4f1f527a8b6f611c30c8702bacd2af, 'Resolver 0xaf38..d2af', true),
        (0xa6219c7d74edeb12d74a3c664f7aaeb7d01ab902, 'Resolver 0xa621..b902', false),
        (0x74c629c4096e234029c78c7760dc0aadb717adb0, 'Resolver 0x74c6..adb0', false),
        (0x7c7047337995c338c1682f12bc38d4e4108309bb, 'Resolver 0x7c70..09bb', false)
    ) as t(resolver_address, resolver_name, kyc)
)

, traces as (
    select 
        tx_hash
        , "from" as resolver_address
        , substr(input, 49, 20) as resolver_executor
        , cast(bytearray_to_uint256(substr(input, 5, 32)) as double) as chain_id
    from {{ source('ethereum', 'traces') }}
    where "to" = 0xcb8308fcb7bc2f84ed1bea2c016991d34de5cc77
        and substr(input, 1, 4) = 0xf204bdb9
        and block_time >= timestamp '2022-12-25'
        and tx_success
        and success
)


select distinct 
    resolver_address
    , resolver_executor
    , coalesce(chain, cast(chain_id as varchar)) as blockchain
    , chain_id
    , resolver_name
    , kyc
    , max(tx_hash) over(partition by resolver_executor, chain_id) as tx_hash_example
from traces
left join names using(resolver_address)
left join chains using(chain_id)
order by resolver_name, resolver_executor
*/