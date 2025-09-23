{{
    config(
        schema = 'oneinch',
        alias = 'aave_tokens',
        materialized = 'table',
        unique_key = ['blockchain', 'atoken_address']
    )
}}


-- source tables are like 50 rows, neww token deployment is a rare events, so no needs for incremental model, this could be even faster as a table
with t as (
    {% for blockchain in ['arbitrum', 'avalanche_c', 'base', 'ethereum', 'gnosis', 'linea', 'optimism', 'polygon', 'scroll',  'zksync']  %}
        select 
            '{{ blockchain }}' as blockchain
            , contract_address AS atoken_address
            , underlyingAsset AS underlying_address
            , aTokenDecimals AS atoken_decimals
            , aTokenSymbol AS atoken_symbol
            , aTokenName AS atoken_name
            , evt_block_time as block_time
            , row_number() over (partition by contract_address, underlyingAsset order by evt_block_time desc) as rn
        from {{ source('aave_v3_' + blockchain, 'AToken_evt_Initialized') }}
        where underlyingAsset is not null
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from t
where rn = 1 -- take latest event only
