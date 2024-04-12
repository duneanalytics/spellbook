{{ config(
        
        alias = 'pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool'],
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}


{% set dex_pool_models = [
 ref('uniswap_pools')
 ,ref('spiritswap_fantom_pools')
 ,ref('spookyswap_fantom_pools')
 ,ref('equalizer_fantom_pools')
 ,ref('wigoswap_fantom_pools')
 ,ref('spartacus_exchange_fantom_pools')
 ,ref('mento_celo_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in dex_pool_models %}
    SELECT
        blockchain
        , project
        , version
        , pool
        , fee
        , token0
        , token1
        , creation_block_time
        , creation_block_number
        , contract_address
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE creation_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
