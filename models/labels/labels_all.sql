{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["soispoke","hildobby"]\') }}')
}}


{% set labels_all_models = [
 'labels_cex',
 'labels_funds',
 'labels_bridges'
 'labels_ofac_sanctionned_ethereum',
 'labels_multisig_ethereum',
 'labels_hackers_ethereum',
 'labels_mev_ethereum',
 'labels_aztec_v2_contracts_ethereum',

 'labels_nft',
 'labels_safe_ethereum',
 'labels_tornado_cash',
 'labels_contracts',
 'labels_miners',
 'labels_airdrop_1_receivers_optimism',
 'labels_arbitrage_traders',
 'labels_flashbots_ethereum',
 'labels_ens',
 'labels_validators',
 'labels_sandwich_attackers'
] %}


SELECT *
FROM (
    {% for labels_model in labels_all_models %}
    SELECT
        blockchain, 
        address, 
        name, 
        category, 
        contributor, 
        source, 
        created_at, 
        updated_at
    FROM {{ ref(labels_model) }}
    WHERE name IS NOT NULL
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)
