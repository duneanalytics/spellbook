{{ config(
        schema = 'bridges',
        alias ='bridges_bridge_transfers',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "bridges",
                                \'["msilb7"]\') }}'
        )
}}

{% set bridge_protocol_transfer_models = [
'hop_protocol_bridge_transfers'
] %}

{% set native_bridge_transfer_models = [
'ovm_optimism_standard_bridge_transfers'
] %}

WITH bridge_protocols AS (
    SELECT *
    FROM (
        {% for bridge_protocol_model in bridge_protocol_transfer_models %}
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
            , CASE
                WHEN lower(chain_data_source) = lower(cid_source.chain_name) THEN 'Withdrawal'
                WHEN lower(chain_data_source) = lower(cid_dest.chain_name) THEN 'Deposit'
                ELSE 'na'
            END AS transfer_type
            , bridged_token_symbol
            , bridged_token_amount
            , bridged_token_fee_amount
            , bridged_amount_usd
            , bridged_token_fee_amount_usd
            , bridged_token_amount_raw
            , bridged_token_fee_amount_raw
            , bridged_token_address
            , bridged_token_fee_address
            , is_native_bridge
            , block_number
            , tx_hash
            , tx_from
            , tx_to
            , transfer_id
            , evt_index
            , trace_address
            , tx_method_id
        FROM {{ ref(bridge_protocol_model) }} bmod
            LEFT JOIN {{ ref('chain_ids') }} cid_source
                ON cid_source.chain_id = bmod.source_chain_id
            LEFT JOIN {{ ref('chain_ids') }} cid_dest
                ON cid_dest.chain_id = bmod.destination_chain_id
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

, native_bridges AS (
    SELECT nat.*
    FROM (
        {% for native_bridge_model in native_bridge_transfer_models %}
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
            , CASE
                WHEN lower(chain_data_source) = lower(cid_source.chain_name) THEN 'Withdrawal'
                WHEN lower(chain_data_source) = lower(cid_dest.chain_name) THEN 'Deposit'
                ELSE 'na'
            END AS transfer_type
            , bridged_token_symbol
            , bridged_token_amount
            , bridged_token_fee_amount
            , bridged_amount_usd
            , bridged_token_fee_amount_usd
            , bridged_token_amount_raw
            , bridged_token_fee_amount_raw
            , bridged_token_address
            , bridged_token_fee_address
            , is_native_bridge
            , block_number
            , tx_hash
            , tx_from
            , tx_to
            , transfer_id
            , evt_index
            , trace_address
            , tx_method_id
        FROM {{ ref(native_bridge_model) }} bmod
            LEFT JOIN {{ ref('chain_ids') }} cid_source
                ON cid_source.chain_id = bmod.source_chain_id
            LEFT JOIN {{ ref('chain_ids') }} cid_dest
                ON cid_dest.chain_id = bmod.destination_chain_id
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ) nat
    -- Exclude native bridges where a bridge protocol was used. Assign the bridge to the bridge protocol.
    LEFT ANTI JOIN  bridge_protocols prot
        ON prot.block_date = nat.block_date
        AND prot.chain_data_source = nat.chain_data_source
        AND prot.block_number = nat.block_number
        AND prot.tx_hash = nat.tx_hash
        AND prot.bridged_token_address = nat.bridged_token_address
        -- Eventual improvement: See if we can join on event (i.e. tie a Hop event with a standard bridge event)
)


SELECT * FROM bridge_protocols
UNION ALL
SELECT * FROM native_bridges