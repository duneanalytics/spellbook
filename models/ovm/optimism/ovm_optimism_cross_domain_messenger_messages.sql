{{ config(
    tags = ['dunesql'],
    alias = alias('cross_domain_messenger_messages'),
    partition_by = ['l2_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['msg_type','l2_block_date', 'l2_tx_hash', 'evt_index','msg_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ovm_optimism",
                                \'["msilb7"]\') }}'
    )
}}

SELECT 'withdraw' AS msg_type, 'SentMessage' AS event, sender,
    evt_tx_hash AS l2_tx_hash, evt_block_number AS l2_block_number, 
    evt_block_time AS l2_block_time, cast(DATE_TRUNC('day',evt_block_time) as date) AS l2_block_date,
    contract_address, target, cast(messageNonce as varchar) AS message_nonce_hash, evt_index, '2' AS version,
    DENSE_RANK() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index ASC) AS msg_index
    
    FROM {{ source ('ovm_optimism', 'L2CrossDomainMessenger_evt_SentMessage') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14' day)
    {% endif %}

    
UNION ALL

SELECT 'deposit' AS m_type, 'RelayedMessage' AS event, cast(NULL as varbinary) as sender,
    evt_tx_hash AS l2_tx_hash, evt_block_number AS l2_block_number, 
    evt_block_time AS l2_block_time, cast(DATE_TRUNC('day',evt_block_time) as date) AS l2_block_date,
    contract_address, cast(NULL as varbinary) AS target, substring(cast(msgHash as varchar),3) AS message_nonce_hash, evt_index, '2' AS version,
    DENSE_RANK() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index ASC) AS msg_index

    FROM {{ source ('ovm_optimism', 'L2CrossDomainMessenger_evt_RelayedMessage') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= (NOW() - interval '14' day)
    {% endif %}