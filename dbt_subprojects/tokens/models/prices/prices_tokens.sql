{{ config(
        schema='prices',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static'],
        post_hook = '{{ expose_spells(\'[
                                        "arbitrum"
                                        , "avalanche_c"
                                        , "base"
                                        , "bitcoin"
                                        , "blast"
                                        , "bnb"
                                        , "boba"
                                        , "cardano"
                                        , "celo"
                                        , "corn"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "gnosis"
                                        , "ink"
                                        , "kaia"
                                        , "linea"
                                        , "nova"
                                        , "optimism"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "solana"
                                        , "sonic"
                                        , "unichain"
                                        , "viction"
                                        , "worldchain"
                                        , "zksync"
                                        , "zkevm"
                                        , "zora"
                                        , "abstract"
                                    ]\',
                                    "sector",
                                    "prices",
                                    \'["aalan3", "jeff-dude", "umer_h_adil", "0xBoxer", "rantum", "lgingerich", "hildobby", "cryptokoryo", "0xRob", "hosuke", "Henrystats"]\') }}'
        )
}}

{% set prices_models = [
ref('prices_native_tokens')
,ref('prices_arbitrum_tokens')
,ref('prices_avalanche_c_tokens')
,ref('prices_bitcoin_tokens')
,ref('prices_bnb_tokens')
,ref('prices_cardano_tokens')
,ref('prices_ethereum_tokens')
,ref('prices_fantom_tokens')
,ref('prices_flare_tokens')
,ref('prices_gnosis_tokens')
,ref('prices_optimism_tokens')
,ref('prices_polygon_tokens')
,ref('prices_solana_tokens')
,ref('prices_celo_tokens')
,ref('prices_base_tokens')
,ref('prices_zksync_tokens')
,ref('prices_zora_tokens')
,ref('prices_scroll_tokens')
,ref('prices_linea_tokens')
,ref('prices_zkevm_tokens')
,ref('prices_mantle_tokens')
,ref('prices_blast_tokens')
,ref('prices_sei_tokens')
,ref('prices_nova_tokens')
,ref('prices_worldchain_tokens')
,ref('prices_kaia_tokens')
,ref('prices_tron_tokens')
,ref('prices_ronin_tokens')
,ref('prices_boba_tokens')
,ref('prices_viction_tokens')
,ref('prices_corn_tokens')
,ref('prices_sonic_tokens')
,ref('prices_ink_tokens')
,ref('prices_unichain_tokens')
,ref('prices_abstract_tokens')
,ref('prices_berachain_tokens')
] %}


SELECT *
FROM
(
    {% for model in prices_models %}
    SELECT
        token_id
        , blockchain
        , symbol
        , contract_address
        , decimals
    FROM {{ model }}
    WHERE contract_address IS DISTINCT FROM 0x0000000000000000000000000000000000000000    -- safeguard as native tokens currently have null address
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
