{{ config(
    schema = 'hop_protocol_optimism',
    alias = 'bridge_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'tx_hash', 'evt_index', 'transfer_id'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "hop_protocol",
                                \'["msilb7"]\') }}'
    )
}}


SELECT
 'optimism' as chain_data_source
, DATE_TRUNC('day',tf.block_time) AS block_date
, tf.block_time
, source_chain_id
, destination_chain_id
, cid_source.chain_name AS source_chain_name
, cid_dest.chain_name AS destination_chain_name
, erc.symbol AS bridged_token_symbol
, bridged_token_amount_raw / POWER(10,erc.decimals) AS bridged_token_amount
, bridged_token_fee_amount_raw / POWER(10,erc.decimals) AS bridged_token_fee_amount
, p.price*( bridged_token_amount_raw / POWER(10,erc.decimals) ) AS bridged_amount_usd
, p.price*( bridged_token_fee_amount_raw / POWER(10,erc.decimals) ) AS bridged_token_fee_amount_usd
, bridged_token_amount_raw
, bridged_token_fee_amount_raw
, hba.`l2CanonicalToken` AS bridged_token_address
, hba.`l2CanonicalToken` AS bridged_token_fee_address

, tf.block_number
, tx_hash
,t.`from` AS tx_from
,t.`to` AS tx_to
, tf.transfer_id
, tf.evt_index
, tf.trace_address
, substring(t.data,1,10) AS tx_method_id
    
FROM (

    select 
     ts.evt_block_time AS block_time
    ,ts.evt_block_number AS block_number
    ,ts.evt_tx_hash AS tx_hash
    , ts.`amount` AS bridged_token_amount_raw
    , ts.`bonderFee` AS bridged_token_fee_amount_raw
    , ts.recipient AS recipient_address
    ,'' AS trace_address
    ,ts.evt_index
    ,ts.contract_address AS project_contract_address
    , ts.`transferId` AS transfer_id
    , (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'optimism') AS source_chain_id
    ,chainId AS destination_chain_id
    
    FROM {{ source('hop_protocol_optimism', 'L2_OptimismBridge_evt_TransferSent') }} ts 
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14 days')
    {% endif %}
    UNION ALL
    
    select 
     tl.evt_block_time AS block_time
    ,tl.evt_block_number AS block_number
    ,tl.evt_tx_hash AS tx_hash
    , tl.`amount` AS bridged_token_amount_raw
    , tl.`relayerFee` AS bridged_token_fee_amount_raw
    , tl.recipient AS recipient_address
    ,'' AS trace_address
    ,tl.evt_index
    ,tl.contract_address AS project_contract_address
    , '' AS transfer_id
    , 1 AS source_chain_id
    , (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'optimism') AS destination_chain_id
    
    FROM {{ source ('hop_protocol_optimism', 'L2_OptimismBridge_evt_TransferFromL1Completed') }} tl
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14 days')
    {% endif %}
    
    UNION ALL
    
    select 
     wb.evt_block_time AS block_time
    ,wb.evt_block_number AS block_number
    ,wb.evt_tx_hash AS tx_hash
    , wb.`amount` AS bridged_token_amount_raw
    , 0 AS bridged_token_fee_amount_raw
    , COALESCE(arb.recipient,poly.recipient,poly2.recipient,gno.recipient) AS recipient_address
    ,'' AS trace_address
    ,wb.evt_index
    ,wb.contract_address AS project_contract_address
    , wb.transferId AS transfer_id
    , CASE
            WHEN arb.transferId IS NOT NULL THEN (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'arbitrum')
            WHEN poly.transferId IS NOT NULL THEN (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'polygon')
            WHEN poly2.transferId IS NOT NULL THEN (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'polygon')
            WHEN gno.transferId IS NOT NULL THEN (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'gnosis')
        ELSE NULL
        END
        AS source_chain_id
    , (SELECT chain_id FROM chain_ids WHERE lower(chain_name) = 'optimism') AS destination_chain_id
    FROM {{ source ('hop_protocol_optimism', 'L2_OptimismBridge_evt_WithdrawalBonded') }} wb
        LEFT JOIN {{ source ('hop_protocol_arbitrum' ,'L2_ArbitrumBridge_evt_TransferSent') }} arb
            ON arb.evt_block_time BETWEEN (wb.evt_block_time - interval '30 days') AND (wb.evt_block_time + interval '1 day') --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND arb.transferId = wb.transferId
            {% if is_incremental() %}
            AND arb.evt_block_time >= (NOW() - interval '45 days')
              {% endif %}
        LEFT JOIN {{ source ('hop_protocol_polygon', 'L2_PolygonBridge_evt_TransferSent') }} poly
            ON poly.evt_block_time BETWEEN (wb.evt_block_time - interval '30 days') AND (wb.evt_block_time + interval '1 day') --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND poly.transferId = wb.transferId
            {% if is_incremental() %}
            AND poly.evt_block_time >= (NOW() - interval '45 days')
              {% endif %}
        LEFT JOIN {{ source ('hop_polygon', 'L2_PolygonBridge_evt_TransferSent') }} poly2
            ON poly2.evt_block_time BETWEEN (wb.evt_block_time - interval '30 days') AND (wb.evt_block_time + interval '1 day') --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND poly2.transferId = wb.transferId
            {% if is_incremental() %}
            AND poly2.evt_block_time >= (NOW() - interval '45 days')
              {% endif %}
        LEFT JOIN {{ source ('hop_gnosis', 'L2_xDaiBridge_evt_TransferSent') }} gno
            ON gno.evt_block_time BETWEEN (wb.evt_block_time - interval '30 days') AND (wb.evt_block_time + interval '1 day') --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND gno.transferId = wb.transferId
            {% if is_incremental() %}
            AND gno.evt_block_time >= (NOW() - interval '45 days')
              {% endif %}
    {% if is_incremental() %}
    WHERE wb.evt_block_time >= (NOW() - interval '14 days')
    {% endif %}

    ) tf
LEFT JOIN {{ ref('hop_protocol_bridge_addresses') }} hba
            ON tf.project_contract_address = hba.`l2Bridge`
            AND tf.block_number >= hba.bridgeDeployedBlockNumber
LEFT JOIN {{ source('optimism', 'transactions') }} t
        ON t.block_number = tf.block_number
        AND t.hash = tf.tx_hash
        AND t.block_time >= (NOW() - interval '14 days')
LEFT JOIN {{ ref('tokens_erc20') }} erc
    ON erc.blockchain = hba.blockchain
    AND erc.contract_address = hba.`l2CanonicalToken`
    
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = DATE_TRUNC('minute',tf.block_time)
    AND p.contract_address = hba.`l2CanonicalToken`
    AND p.blockchain = hba.blockchain
    AND p.minute >= (NOW() - interval '14 days')
    
LEFT JOIN {{ ref('chain_ids') }} cid_source
    ON cid_source.chain_id = tf.source_chain_id
LEFT JOIN {{ ref('chain_ids') }} cid_dest
    ON cid_dest.chain_id = tf.destination_chain_id
