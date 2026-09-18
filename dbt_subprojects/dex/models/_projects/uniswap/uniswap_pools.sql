{{ config(
        schema = 'uniswap',
        alias = 'pools'
        )
}}

{% set uniswap_models = [
ref('uniswap_arbitrum_pools')
, ref('uniswap_avalanche_c_pools')
, ref('uniswap_base_pools')
, ref('uniswap_blast_pools')
, ref('uniswap_bnb_pools')
, ref('uniswap_celo_pools')
, ref('uniswap_ethereum_pools')
, ref('uniswap_gnosis_pools')
, ref('uniswap_ink_pools')
, ref('uniswap_linea_pools')
, ref('uniswap_mantle_pools')
, ref('uniswap_monad_pools')
, ref('uniswap_optimism_pools')
, ref('uniswap_plasma_pools')
, ref('uniswap_polygon_pools')
, ref('uniswap_scroll_pools')
, ref('uniswap_sonic_pools')
, ref('uniswap_unichain_pools')
, ref('uniswap_worldchain_pools')
, ref('uniswap_zksync_pools')
, ref('uniswap_zora_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in uniswap_models %}
    SELECT
        blockchain
        , project
        , version
        , id as pool
        , fee
        , token0
        , token1
        , creation_block_time
        , creation_block_number
        , contract_address
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)