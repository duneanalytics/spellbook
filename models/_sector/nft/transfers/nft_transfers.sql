{{ config(

        schema = 'nft',
        alias ='transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index', 'token_id', 'amount'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli", "base", "celo", "zksync"]\',
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
,ref('nft_base_transfers')
,ref('nft_zksync_transfers')
,ref('nft_zora_transfers')
,ref('nft_celo_transfers')
,ref('nft_scroll_transfers')
] %}

SELECT *
FROM (
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
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
