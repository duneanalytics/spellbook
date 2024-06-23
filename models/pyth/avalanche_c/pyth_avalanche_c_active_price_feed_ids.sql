{{ config(
    schema = 'pyth_avalanche_c',
    alias = 'active_price_feed_ids',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'chain', 'trace_address', 'trace_from', 'last_used'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.last_used')],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                            "project",
                            "pyth",
                            \'["synthquest"]\') }}'
        )
}}


{% set project_start_date = '2022-09-06' %}

with price_feed_ids as (
    select 
          identifier
        , category
        , token1
        , token2
        , from_hex("hash") as "hash"
        , pending 
    FROM
        {{ ref('pyth_evms_price_feed_ids') }}

)

, pyth_contracts as (
    select 
          chain
        , contract_address
        , chain_type
    FROM
        {{ ref('pyth_price_feed_contracts') }}
    where chain_type = 'evm' and chain = 'avalanche_c'

)

select identifier, category, token1, token2, "hash", pending, price_id, expo, first_used, last_used from (
    select 
      min(block_time) as first_used
    , max(block_time) as last_used
    , (bytearray_substring(input,1 + 4,32)) as price_id
    , bytearray_to_int256(bytearray_substring(output,1 + 32 + 32,32)) as expo
    from {{ source('avalanche_c', 'traces') }} tr
    inner join pyth_contracts pcc on tr.to = pcc.contract_address -- filter for only pyth contracts
    where 
        (bytearray_substring(input,1 ,4))  in (0xa4ae35e0 --getPrice()
                                               , 0x96834ad3 --getPriceUnsafe()
                                                )
        and tx_success = true
        and error is null
        {% if is_incremental() %}
        and {{ incremental_predicate('tr.block_time') }}
        {% else %}
        and tr.block_time >= DATE '{{project_start_date}}'
        {% endif %}
    group by (bytearray_substring(input,1 + 4,32)), bytearray_to_int256(bytearray_substring(output,1 + 32 + 32,32))
)
left join price_feed_ids ids on ids."hash" = price_id
