{{ config(
        alias ='bridges_bridge_transfers',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "bridges",
                                \'["msilb7"]\') }}'
        )
}}

{% set bridge_transfer_models = [
'hop_protocol_bridge_transfers'
] %}


SELECT *
FROM (
    {% for bridge_model in bridge_transfer_models %}
    SELECT
        chain_data_source
        , project
        , block_date
        , block_time
        , source_chain_id
        , destination_chain_id
        , cid_source.chain_name AS source_chain_name
        , cid_dest.chain_name AS destination_chain_name
        , cid_source.chain_name || ' -> ' || cid_dest.chain_name AS bridge_path_name
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
    FROM {{ ref(bridge_model) }} bmod
        LEFT JOIN {{ ref('chain_ids') }} cid_source
            ON cid_source.chain_id = bmod.source_chain_id
        LEFT JOIN {{ ref('chain_ids') }} cid_dest
            ON cid_dest.chain_id = bmod.destination_chain_id
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
