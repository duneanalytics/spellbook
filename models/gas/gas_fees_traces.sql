{{ config(
        schema = 'gas',
        alias ='fees_traces',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","ethereum","bnb","fantom","optimism","arbitrum","polygon"]\',
                                "sector",
                                "gas",
                                \'["hildobby"]\') }}'
        )
}}

{% set gas_fees_models = [
'gas_arbitrum_fees_traces',
'gas_avalanche_c_fees_traces',
'gas_bnb_fees_traces',
'gas_ethereum_fees_traces',
'gas_fantom_fees_traces',
'gas_gnosis_fees_traces',
'gas_optimism_fees_traces',
'gas_polygon_fees_traces'
] %}

SELECT *
FROM (
    {% for gas_model in gas_fees_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_number
        , tx_hash
        , trace_from
        , trace_to
        , tx_from
        , tx_to
        , trace
        , trace_method
        , tx_method
        , trace_input
        , gas_used_original
        , gas_used_trace
        , tx_gas_used
        , gas_used_original_percentage
        , gas_used_trace_percentage
        , tx_gas_price
        , trace_type
        , trace_value
        , trace_success
        , tx_success
        , gas_fee_spent_original
        , gas_fee_spent_original_usd
        , gas_fee_spent_trace
        , gas_fee_spent_trace_usd
    FROM {{ ref(gas_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
