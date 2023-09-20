{{ config(
        tags = ['dunesql'],
        alias = alias('approvals'),
        schema = 'nft',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli", "celo", "base"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby", "tomfutago"]\') }}')
}}

{% set nft_models = [
 ref('nft_ethereum_approvals')
,ref('nft_bnb_approvals')
,ref('nft_avalanche_c_approvals')
,ref('nft_gnosis_approvals')
,ref('nft_optimism_approvals')
,ref('nft_arbitrum_approvals')
,ref('nft_polygon_approvals')
,ref('nft_fantom_approvals')
,ref('nft_goerli_approvals')
,ref('nft_celo_approvals')
,ref('nft_base_approvals')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_number
        , address
        , token_standard
        , approval_for_all
        , contract_address
        , token_id
        , approved
        , operator
        , tx_hash
        --, tx_from
        --, tx_to
        , evt_index
    FROM {{ nft_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
