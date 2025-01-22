{{ config(
    
    schema = 'hop_protocol_optimism',
    alias = 'flows',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'tx_hash', 'evt_index', 'transfer_id'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "hop_protocol",
                                \'["msilb7","soispoke"]\') }}'
    )
}}


SELECT
 'optimism' as blockchain
, 'hop_protocol' as project
, '' as version
, tf.block_time
, CAST(DATE_TRUNC('day',tf.block_time) as DATE) AS block_date
, CAST(DATE_TRUNC('month',tf.block_time) as DATE) AS block_month
, tf.block_number
, tx_hash
, CAST(NULL AS VARBINARY) as sender
, recipient_address as receiver
, erc.symbol AS token_symbol
, bridged_token_amount_raw / POWER(10,erc.decimals) AS token_amount
, p.price*( bridged_token_amount_raw / POWER(10,erc.decimals) ) AS token_amount_usd
, bridged_token_amount_raw as token_amount_raw
, bridged_fee_amount_raw / POWER(10,erc.decimals) AS fee_amount
, p.price*( bridged_fee_amount_raw / POWER(10,erc.decimals) ) AS fee_amount_usd
, bridged_fee_amount_raw as fee_amount_raw
, hba."l2CanonicalToken" AS token_address
, hba."l2CanonicalToken" AS fee_address
, source_chain_id
, destination_chain_id
, cid_source.chain_name AS source_chain_name
, cid_dest.chain_name AS destination_chain_name
, t."from" AS tx_from
, t."to" AS tx_to
, tf.transfer_id
, tf.evt_index
, tf.trace_address
, bytearray_substring(t.data,1,4) AS tx_method_id

FROM (

    -- Withdrawals away from Optimism
    
    select 
     ts.evt_block_time AS block_time
    ,ts.evt_block_number AS block_number
    ,ts.evt_tx_hash AS tx_hash
    , ts."amount" AS bridged_token_amount_raw
    , ts."bonderFee" AS bridged_fee_amount_raw
    , CAST(NULL AS VARBINARY) as sender_address
    , ts.recipient AS recipient_address
    , '' AS trace_address
    ,ts.evt_index
    ,ts.contract_address AS project_contract_address
    , ts."transferId" AS transfer_id
    , (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'optimism') AS source_chain_id
    ,chainId AS destination_chain_id
    
    FROM {{ source('hop_protocol_optimism', 'L2_OptimismBridge_evt_TransferSent') }} ts 
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14' day)
    {% endif %}

    UNION ALL -- Deposits to Optimism from L1
    
    select 
     tl.evt_block_time AS block_time
    ,tl.evt_block_number AS block_number
    ,tl.evt_tx_hash AS tx_hash
    , tl."amount" AS bridged_token_amount_raw
    , tl."relayerFee" AS bridged_fee_amount_raw
    , CAST(NULL AS VARBINARY) as sender_address
    , tl.recipient AS recipient_address
    , '' AS trace_address
    , tl.evt_index
    , tl.contract_address AS project_contract_address
    , 0x AS transfer_id
    , UINT256 '1' AS source_chain_id
    , (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'optimism') AS destination_chain_id
    
    FROM {{ source ('hop_protocol_optimism', 'L2_OptimismBridge_evt_TransferFromL1Completed') }} tl
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14' day)
    {% endif %}
    
    UNION ALL -- Deposits to Optimism from Non-L1 Chains
    
    select 
     wb.evt_block_time AS block_time
    ,wb.evt_block_number AS block_number
    ,wb.evt_tx_hash AS tx_hash
    , wb."amount" AS bridged_token_amount_raw
    , UINT256 '0' AS bridged_fee_amount_raw
    , CAST(NULL AS VARBINARY) as sender_address
    , COALESCE(arb.recipient,poly.recipient,gno.recipient) AS recipient_address
    , '' AS trace_address
    ,wb.evt_index
    ,wb.contract_address AS project_contract_address
    , wb.transferId AS transfer_id
    , CASE
            WHEN arb.transferId IS NOT NULL THEN (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'arbitrum one')
            WHEN poly.transferId IS NOT NULL THEN (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'polygon mainnet')
            WHEN gno.transferId IS NOT NULL THEN (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'gnosis')
        ELSE NULL
        END
        AS source_chain_id
    , (SELECT chain_id FROM {{ ref('chain_info_chain_ids') }} WHERE lower(chain_name) = 'optimism') AS destination_chain_id
    FROM {{ source ('hop_protocol_optimism', 'L2_OptimismBridge_evt_WithdrawalBonded') }} wb
        LEFT JOIN {{ source ('hop_protocol_arbitrum' ,'L2_ArbitrumBridge_evt_TransferSent') }} arb
            ON arb.evt_block_time BETWEEN (wb.evt_block_time - interval '30' day) AND (wb.evt_block_time + interval '1' day) --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND arb.transferId = wb.transferId
            {% if is_incremental() %}
            AND arb.evt_block_time >= (NOW() - interval '45' day)
              {% endif %}
        LEFT JOIN {{ source ('hop_protocol_polygon', 'L2_PolygonBridge_evt_TransferSent') }} poly
            ON poly.evt_block_time BETWEEN (wb.evt_block_time - interval '30' day) AND (wb.evt_block_time + interval '1' day) --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND poly.transferId = wb.transferId
            {% if is_incremental() %}
            AND poly.evt_block_time >= (NOW() - interval '45' day)
              {% endif %}
        LEFT JOIN {{ source ('hop_protocol_gnosis', 'L2_xDaiBridge_evt_TransferSent') }} gno
            ON gno.evt_block_time BETWEEN (wb.evt_block_time - interval '30' day) AND (wb.evt_block_time + interval '1' day) --usually < ~20 mins, but extending longer for safety & OP blocktimestamp fix (used to have ~15 min delay)
            AND gno.transferId = wb.transferId
            {% if is_incremental() %}
            AND gno.evt_block_time >= (NOW() - interval '45' day)
              {% endif %}
    {% if is_incremental() %}
    WHERE wb.evt_block_time >= (NOW() - interval '14' day)
    {% endif %}

    ) tf
LEFT JOIN {{ ref('hop_protocol_addresses') }} hba
            ON tf.project_contract_address = hba."l2Bridge"
            AND tf.block_number >= hba.bridgeDeployedBlockNumber
            AND hba.blockchain = 'optimism'
LEFT JOIN {{ source('optimism', 'transactions') }} t
        ON t.block_time = tf.block_time
        AND t.hash = tf.tx_hash
        {% if is_incremental() %}
        AND t.block_time >= (NOW() - interval '14' day)
        {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc
    ON erc.blockchain = hba.blockchain
    AND erc.contract_address = hba."l2CanonicalToken"
    
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = DATE_TRUNC('minute',tf.block_time)
    AND p.contract_address = hba."l2CanonicalToken"
    AND p.blockchain = hba.blockchain
    {% if is_incremental() %}
    AND p.minute >= (NOW() - interval '14' day)
    {% endif %}

LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_source
    ON cid_source.chain_id = tf.source_chain_id
LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_dest
    ON cid_dest.chain_id = tf.destination_chain_id
