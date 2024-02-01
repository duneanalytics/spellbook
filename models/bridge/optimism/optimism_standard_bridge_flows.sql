{{ config(
    
    alias = 'standard_bridge_flows',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "bridge",
                                \'["msilb7","soispoke"]\') }}'
    )
}}

-- We use logs so that we can handle for custom bridge contracts (i.e. SNX, Lido, DAI)
-- These custom bridge contract will use the same DepositFinalized and WithrawalInitiated events, so the topic will be the same.

WITH bridge_events AS (
    SELECT
        block_time, 
        CAST(DATE_TRUNC('day',block_time) as date) AS block_date, 
        CAST(DATE_TRUNC('month',block_time) as date) AS block_month, 
        sender,
        block_number, 
        tx_hash,
        bridged_token_address, 
        bridged_token_amount_raw, 
        recipient_address, 
        trace_address, a.evt_index, 
        project_contract_address,
        message_nonce_hash AS transfer_id, -- removing the coalesce since this isn't a unique key
        source_chain_id, 
        destination_chain_id
    FROM (
        
        SELECT 
        'deposit' AS tf_type
        , l.block_time
        ,l.block_number
        ,l.tx_hash
        ,bytearray_substring(topic2, 13, 20) AS bridged_token_address
        ,bytearray_to_uint256(bytearray_substring(data,33,32)) as bridged_token_amount_raw
        , bytearray_substring(data, 13, 20) AS recipient_address
        , array[-1] AS trace_address
        , l.index AS evt_index
        , l.contract_address AS project_contract_address
        , NULL AS transfer_id
        , UINT256 '1' AS source_chain_id
        , UINT256 '10' AS destination_chain_id
        ,DENSE_RANK() OVER (PARTITION BY tx_hash ORDER BY index ASC) AS tf_index
        
        FROM {{ source ('optimism', 'logs') }} l
        WHERE topic0 = 0xb0444523268717a02698be47d0803aa7468c00acbed2f8bd93a0459cde61dd89 --Deposit Finalized
        -- Other potentially helpful filters
        AND (topic2 IS NOT NULL) AND (topic3 IS NOT NULL) AND (data IS NOT NULL)
        {% if is_incremental() %}
        AND block_time > NOW() - interval '14' Day
        {% endif %}
        
        UNION ALL
        
        SELECT 
        'withdraw' AS tf_type
        , l.block_time
        , l.block_number
        , l.tx_hash
        , bytearray_substring(topic2, 13, 20) AS bridged_token_address
        , bytearray_to_uint256(bytearray_substring(data,33,32)) as bridged_token_amount_raw
        , bytearray_substring(data, 13, 20) AS recipient_address
        , array[-1] AS trace_address
        , l.index AS evt_index
        , l.contract_address AS project_contract_address
        , NULL AS transfer_id
        , UINT256 '10' AS source_chain_id
        , UINT256 '1' AS destination_chain_id
        ,DENSE_RANK() OVER (PARTITION BY tx_hash ORDER BY index ASC) AS tf_index
        
        FROM {{ source ('optimism', 'logs') }} l
        WHERE topic0 = 0x73d170910aba9e6d50b102db522b1dbcd796216f5128b445aa2135272886497e --Withdrawal Initiated
        -- Other potentially helpful filters
        AND (topic2 IS NOT NULL) AND (topic3 IS NOT NULL) AND (data IS NOT NULL)
        {% if is_incremental() %}
        AND block_time > NOW() - interval '14' Day
        {% endif %}
        
        ) a
    LEFT JOIN {{ ref ('ovm_optimism_cross_domain_messenger_messages') }} m
        ON a.tf_type = m.msg_type
        AND a.block_time = m.l2_block_time
        AND a.block_number = m.l2_block_number
        AND a.tx_hash = m.l2_tx_hash
        AND a.tf_index = m.msg_index
        {% if is_incremental() %}
        AND m.l2_block_time > NOW() - interval '14' Day
        {% endif %}
)

SELECT
 'optimism' as blockchain
, 'standard_bridge' AS project
, '' as version
, tf.block_time
, tf.block_date
, tf.block_month
, tf.block_number
, tx_hash
, COALESCE(sender,CAST(NULL as VARBINARY)) as sender
, COALESCE(tf.recipient_address, CAST(NULL as VARBINARY)) as receiver
, erc.symbol AS token_symbol
, CAST(bridged_token_amount_raw as double)/ POWER(10,erc.decimals) AS token_amount
, p.price*( CAST(bridged_token_amount_raw as double) / POWER(10,erc.decimals) ) AS token_amount_usd
, bridged_token_amount_raw as token_amount_raw
, 0 AS fee_amount
, 0 AS fee_amount_usd
, 0 AS fee_amount_raw
, bridged_token_address as token_address
, CAST(NULL as VARBINARY) AS fee_address
, source_chain_id
, destination_chain_id
, cid_source.chain_name AS source_chain_name
, cid_dest.chain_name AS destination_chain_name
, 1 AS is_native_bridge
, t."from" AS tx_from
, t.to AS tx_to
, tf.transfer_id
, tf.evt_index
, tf.trace_address
, bytearray_substring(t.data,1,4) AS tx_method_id

FROM bridge_events tf

LEFT JOIN {{ source('optimism', 'transactions') }} t
        ON t.block_time = tf.block_time
        AND t.hash = tf.tx_hash
        {% if is_incremental() %}
        AND t.block_time >= (NOW() - interval '14' Day)
        {% endif %}
        
LEFT JOIN {{ source('tokens', 'erc20') }} erc
    ON erc.blockchain = 'optimism'
    AND erc.contract_address = tf.bridged_token_address
    
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = DATE_TRUNC('minute',tf.block_time)
    AND p.blockchain = 'optimism'
    AND p.contract_address = tf.bridged_token_address
    {% if is_incremental() %}
    AND p.minute >= (NOW() - interval '14' Day)
    {% endif %}
    
LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_source
    ON cid_source.chain_id =  tf.source_chain_id
LEFT JOIN {{ ref('chain_info_chain_ids') }} cid_dest
    ON cid_dest.chain_id = tf.destination_chain_id