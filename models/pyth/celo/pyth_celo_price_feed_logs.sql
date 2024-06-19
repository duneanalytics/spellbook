{{ config(
    schema = 'pyth_celo',
    alias = 'price_feed_logs',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'chain', 'trace_address', 'trace_from'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["celo"]\',
                            "project",
                            "pyth",
                            \'["synthquest"]\') }}'
        )
}}


{% set project_start_date = '2022-12-06' %}

with price_feed_ids as (
    select identifier, category, token1, token2, "hash", pending, expo from (
        select 
            identifier
            , category
            , token1
            , token2
            , from_hex("hash") as "hash"
            , pending
            , expo
            , row_number() over (order by last_used desc) as change
        FROM
            {{ ref('pyth_celo_active_price_feed_ids') }}

    )
    where change = 1 -- ensure no duplicates if exponent changed, use most recent exponent 
)


, pyth_contracts as (
    select 
          chain
        , contract_address
        , chain_type
    FROM
        {{ ref('pyth_price_feed_contracts') }}
    where chain_type = 'evm' and chain = 'celo'

)


select
       pcc.chain_type
     , pcc.chain
     , ids.identifier
     , ids.category
     , ids.token1
     , ids.token2
     , log.block_time
     , log.block_number
     , case when log.topic0 = 0xef1f4c97783da7fc718b0334ca9b16bb0c5d1da6db75e96012b3613bb12707a0 then
                from_unixtime(bytearray_to_uint256(bytearray_substring(log.data,1 + 32 + 32 + 32,32))) 
            else
                from_unixtime(bytearray_to_uint256(bytearray_substring(log.data,1,32))) 
            end as publish_time
     , case when log.topic0 = 0xef1f4c97783da7fc718b0334ca9b16bb0c5d1da6db75e96012b3613bb12707a0 then
                (bytearray_to_uint256(bytearray_substring(log.data,1 + 32 + 32 + 32 + 32,32))) / pow(10,abs(ids.expo))
            else
                (bytearray_to_uint256(bytearray_substring(log.data,1 + 32,32))) / pow(10,abs(ids.expo))
            end as price
     , case when log.topic0 = 0xef1f4c97783da7fc718b0334ca9b16bb0c5d1da6db75e96012b3613bb12707a0 then
                (bytearray_to_uint256(bytearray_substring(log.data,1 + 32 + 32 + 32 + 32 + 32,32))) / pow(10,abs(ids.expo))
            else
                (bytearray_to_uint256(bytearray_substring(log.data,1 + 32 + 32,32))) / pow(10,abs(ids.expo))
            end as conf
     , log.tx_hash
     , log.topic0 as price_id
     , log.index
     , log.tx_index
     , log.tx_to
     , log.tx_from
     , bc.namespace
     , bc.name
from {{ source('celo', 'logs') }} log
inner join pyth_contracts pcc on log.contract_address = pcc.contract_address
inner join price_feed_ids ids on log.topic1 = ids."hash"
    and log.topic0 in (0xd06a6b7f4918494b3719217d1802786c1f5112a6c1d88fe2cfec00b4584f6aec, 0xef1f4c97783da7fc718b0334ca9b16bb0c5d1da6db75e96012b3613bb12707a0)--PriceFeedUpdate, legacy PriceFeedUpdate
left join {{ source('celo', 'contracts') }} bc on log.tx_to = bc.address
where 
{% if is_incremental() %}
{{ incremental_predicate('log.block_time') }}
{% else %}
log.block_time >= DATE '{{project_start_date}}'
{% endif %}

order by log.block_time desc