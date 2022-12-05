{{ config(
        schema = 'hop_protocol',
        alias ='hop_protocol_bridge_transfers',
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
                , CASE WHEN sb.tx_hash IS NOT NULL
                        THEN 1 ELSE 0
                 END AS is_native_bridge
                , h.block_number
                , h.tx_hash
                , tx_from
                , tx_to
                , transfer_id
                , evt_index
                , trace_address
                , tx_method_id
        FROM {{ ref(hop_tf_model) }} h
        LEFT JOIN
        {% if hop_tf_model == 'hop_protocol_optimism_bridge_transfers' %}
                {{ref('ovm_optimism_standard_bridge_transfers')}} sb
        {% endif %}
        -- Add if statements for other chains here
        ON sb.tx_hash = h.tx_hash
        AND sb.block_number = h.block_number
        AND sb.block_date = h.block_date

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)