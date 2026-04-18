{{ config(
        schema = 'nft',
        alias = 'transfers',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli", "base", "celo", "zksync", "zora", "scroll", "linea", "blast", "mantle", "sei", "ronin", "worldchain", "kaia"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby", "0xRob", "rantum","petertherock"]\') }}'
)
}}

-- ci-stamp: 1
{% set nft_models = [
 ref('nft_ethereum_transfers')
,ref('nft_bnb_transfers')
,ref('nft_avalanche_c_transfers')
,ref('nft_gnosis_transfers')
,ref('nft_optimism_transfers')
,ref('nft_arbitrum_transfers')
,ref('nft_polygon_transfers')
,ref('nft_fantom_transfers')
,ref('nft_goerli_transfers')
,ref('nft_base_transfers')
,ref('nft_zksync_transfers')
,ref('nft_zora_transfers')
,ref('nft_celo_transfers')
,ref('nft_scroll_transfers')
,ref('nft_linea_transfers')
,ref('nft_blast_transfers')
,ref('nft_mantle_transfers')
,ref('nft_sei_transfers')
,ref('nft_ronin_transfers')
,ref('nft_worldchain_transfers')
,ref('nft_kaia_transfers')
] %}

{% for nft_model in nft_models %}
SELECT
      blockchain
    , block_time
    , block_month
    , block_date
    , block_number
    , token_standard
    , transfer_type
    , evt_index
    , contract_address
    , token_id
    , amount
    , "from"
    , to
    , executed_by
    , tx_hash
    , unique_transfer_id
FROM {{ nft_model }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
