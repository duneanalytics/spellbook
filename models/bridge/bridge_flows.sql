{{ config(
        schema = 'bridge',
        alias = 'flows',
        post_hook='{{ expose_spells_hide_trino(\'["optimism"]\',
                                "sector",
                                "bridge",
                                \'["msilb7","soispoke"]\') }}'
        )
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

{% set bridge_protocol_flows_models = [
    ref( 'hop_protocol_flows' )
] %}

{% set native_bridge_flows_models = [
    ref( 'optimism_standard_bridge_flows' )
] %}

WITH bridge_protocols AS (
    SELECT *
    FROM (
        {% for bridge_protocol_model in bridge_protocol_flows_models %}
        SELECT
            blockchain
            , project
            , version
            , block_time
            , block_date
            , block_number
            , tx_hash
            , cid_source.chain_name || ' -> ' || cid_dest.chain_name AS bridge_path_name
            , sender
            , receiver
            , CASE
                WHEN lower(blockchain) = lower(cid_source.chain_name) THEN 'withdrawal'
                WHEN lower(blockchain) = lower(cid_dest.chain_name) THEN 'deposit'
                ELSE 'na'
            END AS transfer_type
            , token_symbol
            , CAST(token_amount AS double) AS token_amount
            , CAST(token_amount_usd AS double) AS token_amount_usd
            , CAST(token_amount_raw AS double) AS token_amount_raw
            , CAST(fee_amount AS double) AS fee_amount
            , CAST(fee_amount_usd AS double) AS fee_amount_usd
            , CAST(fee_amount_raw AS double) AS fee_amount_raw
            , token_address
            , fee_address
            , CAST(bmod.source_chain_id AS DECIMAL(38, 0)) AS source_chain_id
            , CAST(bmod.destination_chain_id AS DECIMAL(38, 0)) AS destination_chain_id
            , cid_source.chain_name AS source_chain_name
            , cid_dest.chain_name AS destination_chain_name
            , is_native_bridge
            , tx_from
            , tx_to
            , transfer_id
            , evt_index
            , trace_address
            , tx_method_id
        FROM {{ bridge_protocol_model }} bmod
            LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_source
                ON CAST(cid_source.chain_id AS DECIMAL(38, 0)) = CAST(bmod.source_chain_id AS DECIMAL(38, 0))
            LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_dest
                ON CAST(cid_dest.chain_id AS DECIMAL(38, 0)) = CAST(bmod.destination_chain_id AS DECIMAL(38, 0))
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

, native_bridges AS (
    SELECT nat.*
    FROM (
        {% for native_bridge_model in native_bridge_flows_models %}
        SELECT
            blockchain -- changed
            , project
            , version
            , block_time
            , block_date
            , block_number
            , tx_hash
            , cid_source.chain_name || ' -> ' || cid_dest.chain_name AS bridge_path_name
            , sender -- TO DO
            , receiver -- TO DO
            , CASE
                WHEN lower(blockchain) = lower(cid_source.chain_name) THEN 'withdrawal'
                WHEN lower(blockchain) = lower(cid_dest.chain_name) THEN 'deposit'
                ELSE 'na'
            END AS transfer_type
            , token_symbol
            , CAST(token_amount AS double) AS token_amount
            , CAST(token_amount_usd AS double) AS token_amount_usd -- changed
            , CAST(token_amount_raw AS double) AS token_amount_raw
            , CAST(fee_amount AS double) AS fee_amount
            , CAST(fee_amount_usd AS double) AS fee_amount_usd
            , CAST(fee_amount_raw AS double) AS fee_amount_raw
            , token_address
            , fee_address
            , CAST(bmod.source_chain_id AS DECIMAL(38, 0)) AS source_chain_id
            , CAST(bmod.destination_chain_id AS DECIMAL(38, 0)) AS destination_chain_id
            , cid_source.chain_name AS source_chain_name
            , cid_dest.chain_name AS destination_chain_name
            , is_native_bridge
            , tx_from
            , tx_to
            , transfer_id
            , evt_index
            , trace_address
            , tx_method_id
        FROM {{ native_bridge_model }} bmod
            LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_source
                ON CAST(cid_source.chain_id AS DECIMAL(38, 0)) = CAST(bmod.source_chain_id AS DECIMAL(38, 0))
            LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_dest
                ON CAST(cid_dest.chain_id AS DECIMAL(38, 0)) = CAST(bmod.destination_chain_id AS DECIMAL(38, 0))
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ) nat
    -- Exclude native bridges where a bridge protocol was used. Assign the bridge to the bridge protocol.
    LEFT JOIN bridge_protocols prot
        ON prot.block_date = nat.block_date
        AND prot.blockchain = nat.blockchain
        AND prot.block_number = nat.block_number
        AND prot.tx_hash = nat.tx_hash
        AND prot.token_address = nat.token_address
        -- Eventual improvement: See if we can join on event (i.e. tie a Hop event with a standard bridge event)
    WHERE prot.blockchain IS NULL
)


SELECT * FROM bridge_protocols
UNION ALL
SELECT * FROM native_bridges