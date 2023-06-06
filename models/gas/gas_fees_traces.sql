{{ config(
        schema = 'gas',
        alias ='fees_traces',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'trace'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","ethereum","bnb","fantom","optimism","arbitrum","polygon"]\',
                                "sector",
                                "gas",
                                \'["hildobby"]\') }}'
        )
}}

{% set gas_fees_traces_models = [
ref('gas_arbitrum_fees_traces'),
ref('gas_avalanche_c_fees_traces'),
ref('gas_bnb_fees_traces'),
ref('gas_ethereum_fees_traces'),
ref('gas_fantom_fees_traces'),
ref('gas_gnosis_fees_traces'),
ref('gas_optimism_fees_traces'),
ref('gas_polygon_fees_traces')
] %}

SELECT *
FROM (
    {% for gas_fees_traces_model in gas_fees_traces_models %}
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
    FROM {{ gas_fees_traces_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
