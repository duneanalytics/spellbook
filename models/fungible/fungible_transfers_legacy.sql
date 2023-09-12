{{ config(
        tags=['legacy'],
        schema = 'fungible',
        alias = alias('transfers', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "base"]\',
                                    "sector",
                                    "fungible",
                                    \'["hildobby"]\') }}'
)
}}

{% set fungible_models = [
 ref('fungible_ethereum_transfers_legacy')
,ref('fungible_bnb_transfers_legacy')
,ref('fungible_avalanche_c_transfers_legacy')
,ref('fungible_gnosis_transfers_legacy')
,ref('fungible_optimism_transfers_legacy')
,ref('fungible_arbitrum_transfers_legacy')
,ref('fungible_polygon_transfers_legacy')
,ref('fungible_fantom_transfers_legacy')
,ref('fungible_base_transfers_legacy')
] %}

SELECT *
FROM (
    {% for fungible_model in fungible_models %}
    SELECT blockchain
    , block_time
    , block_number
    , amount_raw
    , amount
    , usd_price
    , usd_amount
    , contract_address
    , symbol
    , decimals
    , token_standard
    , tx_from
    , "from"
    , to
    , tx_hash
    , evt_index
    FROM {{ fungible_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
