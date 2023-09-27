{{ config(
	tags=['legacy', 'remove'],
        schema = 'nft',
        alias = alias('approvals', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number','tx_hash','evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}')
}}

{% set nft_models = [
 ref('nft_ethereum_approvals_legacy')
,ref('nft_bnb_approvals_legacy')
,ref('nft_avalanche_c_approvals_legacy')
,ref('nft_gnosis_approvals_legacy')
,ref('nft_optimism_approvals_legacy')
,ref('nft_arbitrum_approvals_legacy')
,ref('nft_polygon_approvals_legacy')
,ref('nft_fantom_approvals_legacy')
,ref('nft_goerli_approvals_legacy')
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
        , tx_hash
        --, tx_from
        --, tx_to
        , evt_index
    FROM {{ nft_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
