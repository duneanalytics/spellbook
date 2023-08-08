{{
    config(
        alias='set_name_latest'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['address']
        ,post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["sankinyue"]\') }}'
    )
}}

with 
set_name_detail as (
    -- setName
    select block_time
        , "from"                                                as addr
        , to                                                    as registrar
        , from_utf8(bytearray_rtrim(substr(input, 5 + 2 * 32))) as name
        , last_tx_hash
    from ethereum.traces
    where block_number >= 3787060 -- 0x9062c0a6dbd6108336bcbe4593a3d1ce05512069 min(block_number)
        and to in (
              0x9062c0a6dbd6108336bcbe4593a3d1ce05512069 -- ENS: Old Reverse Registrar
            , 0x084b1c3c81545d370f3634392de611caabff8148 -- ENS: Old Reverse Registrar 2
            , 0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb -- ENS: Reverse Registrar
        )
        and substr(input, 1, 4) = 0xc47f0027 -- setName
        and substr(from_utf8(bytearray_rtrim(substr(input, 5 + 2 * 32))), -4) = '.eth'
        and success = true 
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        
    union all
    
    -- setNameForAddr
    select block_time       
        , substr(data, 5 + 12, 20) as addr
        -- , substr(data, 5 + 32 * 1 + 12, 20) as owner 
        -- , substr(data, 5 + 32 * 2 + 12, 20) as resolver
        , to as registrar
        , from_utf8(bytearray_rtrim(substr(data, 5 + 5 * 32))) as name
        , hash as tx_hash                                      
    from ethereum.transactions
    where block_number >= 16925606 -- 0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb min(block_number)
        and to = 0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb -- ENS: Reverse Registrar
        and substr(data, 1, 4) = 0x7a806d6b -- setNameForAddr
        and substr(from_utf8(bytearray_rtrim(substr(data, 5 + 5 * 32))), -4) = '.eth'
        and success = true 
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

set_name_rn as ( 
    select
          row_number() over (partition by name order by block_time desc) as name_rn
        , row_number() over (partition by addr order by block_time desc) as addr_rn
        , block_time
        , addr
        , registrar
        , name
        , tx_hash 
    from set_name_detail 
)

select -- count(*)
      block_time as last_block_time
    , addr
    , registrar
    , name
    , tx_hash as last_tx_hash
from set_name_rn 
where name_rn = 1 
    and addr_rn = 1 
order by last_block_time desc 