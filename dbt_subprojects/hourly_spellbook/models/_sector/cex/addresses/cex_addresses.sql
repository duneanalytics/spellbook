{{ config(
        schema = 'cex',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "polygon", "bitcoin", "fantom", "aptos", "celo", "zora", "zksync", "zkevm", "linea", "solana", "scroll", "tron", "base", "mantle", "worldchain", "sei", "berachain", "ink", "katana", "nova", "opbnb", "unichain"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')
}}

{% set chains = [
 ref('cex_algorand_addresses')
 , ref('cex_aptos_addresses')
 , ref('cex_arbitrum_addresses')
 , ref('cex_avalanche_c_addresses')
 , ref('cex_base_addresses')
 , ref('cex_bitcoin_addresses')
 , ref('cex_bnb_addresses')
 , ref('cex_cardano_addresses')
 , ref('cex_celo_addresses')
 , ref('cex_cosmos_addresses')
 , ref('cex_elrond_addresses')
 , ref('cex_eos_addresses')
 , ref('cex_ethereum_addresses')
 , ref('cex_fantom_addresses')
 , ref('cex_gnosis_addresses')
 , ref('cex_injective_addresses')
 , ref('cex_linea_addresses')
 , ref('cex_litecoin_addresses')
 , ref('cex_optimism_addresses')
 , ref('cex_polkadot_addresses')
 , ref('cex_polygon_addresses')
 , ref('cex_ripple_addresses')
 , ref('cex_scroll_addresses')
 , ref('cex_solana_addresses')
 , ref('cex_tron_addresses')
 , ref('cex_zilliqa_addresses')
 , ref('cex_zkevm_addresses')
 , ref('cex_zksync_addresses')
 , ref('cex_zora_addresses')
 , ref('cex_worldchain_addresses')
 , ref('cex_mantle_addresses')
 , ref('cex_sei_addresses')
  , ref('cex_near_addresses')
  , ref('cex_berachain_addresses')
  , ref('cex_ink_addresses')
  , ref('cex_katana_addresses')
  , ref('cex_nova_addresses')
  , ref('cex_opbnb_addresses')
  , ref('cex_unichain_addresses')
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT blockchain
    , CAST(address AS varbinary) AS address
    , cex_name
    , distinct_name
    , added_by
    , added_date
    FROM {{ chain }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)