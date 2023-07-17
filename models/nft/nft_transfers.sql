{{ config(
        tags = ['dunesql'],
        alias =alias('transfers'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'unique_transfer_id'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby", "0xRob"]\') }}'
)
}}

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
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
          blockchain
        , block_time
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
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
