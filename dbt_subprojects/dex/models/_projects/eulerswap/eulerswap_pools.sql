{{ config(
        schema = 'eulerswap',
        alias = 'pools'
        )
}}

{% set eulerswap_models = [
ref('eulerswap_ethereum_pools')
, ref('eulerswap_bnb_pools')
, ref('eulerswap_unichain_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in eulerswap_models %}
    SELECT
        blockchain
        , project 
        , version 
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , next_block_number
        , pool 
        , isActive
        , hook
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)