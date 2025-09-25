{{
    config(
        schema = 'oneinch',
        alias = 'aave_tokens',
        materialized = 'table',
        unique_key = ['blockchain', 'atoken_address']
    )
}}



-- source tables are like 50 rows, new token deployment is a rare events, so no needs for incremental model, this could be even faster as a table
with

initializations as (
    {% for blockchain in oneinch_meta_cfg_macro()['blockchains']['aave'] %}
        select 
            '{{ blockchain }}' as blockchain
            , contract_address as atoken_address
            , max_by(underlyingAsset, evt_block_time) as underlying_address -- c?? ...token_address / asset
            , max_by(aTokenDecimals, evt_block_time) as atoken_decimals
            , max_by(aTokenSymbol, evt_block_time) as atoken_symbol
            , max_by(aTokenName, evt_block_time) as atoken_name
            , max_by(evt_block_time, evt_block_time) as block_time
        from {{ source('aave_v3_' + blockchain, 'AToken_evt_Initialized') }}
        where underlyingAsset is not null
        group by 1, 2 -- take latest event only
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- output --

select
    blockchain
    , atoken_address
    , underlying_address
    , atoken_decimals
    , atoken_symbol
    , atoken_name
    , block_time
from initializations