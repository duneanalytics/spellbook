{{ config(
        alias ='bridge_transfers',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "uniswap",
                                \'["jeff-dude","mtitus6"]\') }}'
        )
}}

{% set hop_transfer_models = [
        'hop_protocol_optimism_bridge_transfers'
] %}

SELECT *
FROM
(
        {% for hop_tf_model in hop_transfer_models %}
        SELECT
                chain_data_source
                , 'Hop Protocol' AS project
                , block_date
                , block_time
                , source_chain_id
                , destination_chain_id
                , source_chain_name
                , destination_chain_name
                , bridged_token_symbol
                , bridged_token_amount
                , bridged_token_fee_amount
                , bridged_amount_usd
                , bridged_token_fee_amount_usd
                , bridged_token_amount_raw
                , bridged_token_fee_amount_raw
                , bridged_token_address
                , bridged_token_fee_address
                , block_number
                , tx_hash
                , tx_from
                , tx_to
                , transfer_id
                , evt_index
                , trace_address
                , tx_method_id
        FROM {{ ref(hop_tf_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)