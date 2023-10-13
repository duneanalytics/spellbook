{{ config(
    schema = 'sofi',
    tags = ['dunesql'],
    alias = alias('trades'),
    post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "sofi",
                                \'["hildobby"]\') }}'
    )
}}

{% set chain_specific_models = [
     (ref('sofi_base_trades'))
] %}

SELECT *
FROM (
    {% for chain_specific_model in chain_specific_models %}
    SELECT block_time
    , block_number
    , project
    , trader
    , subject
    , tx_from
    , tx_to
    , trade_side
    , amount
    , amount_usd
    , share_amount
    , subject_fee_amount
    , subject_fee_amount_usd
    , protocol_fee_amount
    , protocol_fee_amount_usd
    , currency_contract
    , currency_symbol
    , supply
    , tx_hash
    , evt_index
    , contract_address
    FROM {{ chain_specific_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
