{{ config(

        alias = 'approvals',
        schema = 'nft',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli", "celo", "base", "zksync"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby", "tomfutago", "0xRob"]\') }}')
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
,ref('nft_zksync_approvals')
,ref('nft_scroll_approvals')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_month
        , block_number
        , address
        , token_standard
        , approval_for_all
        , contract_address
        , token_id
        , approved
        , operator
        , tx_hash
        , evt_index
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
