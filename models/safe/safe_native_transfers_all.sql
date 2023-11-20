{{ config(
        schema = 'safe',
        alias = 'native_transfers_all',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","celo","ethereum","gnosis","goerli","optimism","polygon","zksync"]\',
                                "project",
                                "safe",
                                \'["kryptaki", "danielpartida"]\') }}'
        )
}}

{% set safe_native_transfers_models = [
 ref('safe_arbitrum_eth_transfers')
,ref('safe_avalanche_c_avax_transfers')
,ref('safe_base_eth_transfers')
,ref('safe_bnb_bnb_transfers')
,ref('safe_celo_celo_transfers')
,ref('safe_ethereum_eth_transfers')
,ref('safe_gnosis_xdai_transfers')
,ref('safe_goerli_eth_transfers')
,ref('safe_optimism_eth_transfers')
,ref('safe_polygon_matic_transfers')
,ref('safe_zksync_eth_transfers')
] %}


SELECT *
FROM (
    {% for native_transfers_model in safe_native_transfers_models %}
    SELECT
        blockchain,
        symbol,
        address,
        block_date,
        block_time,
        amount_raw,
        amount_usd,
        tx_hash,
        trace_address
    FROM {{ native_transfers_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
