{{ config(
    schema = 'bridge_zksync_native',
    alias = 'flows',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["zksync"]\',
                                "sector",
                                "bridge",
                                \'["lgingerich"]\') }}'
    )
}}

WITH bridge_events as (
    SELECT
         block_time
        ,CAST(DATE_TRUNC('day', block_time) as date) as block_date
        ,CAST(DATE_TRUNC('month', block_time) as date) as block_month
        ,block_number
        ,tx_hash
        ,evt_index
        ,sender_address
        ,receiver_address
        ,bridged_token_address
        ,bridged_token_amount_raw
        ,source_chain_id
        ,destination_chain_id
        ,source_chain_name
        ,destination_chain_name
        ,tx_from
        ,tx_to
    FROM (
        -- Deposit ETH from Ethereum to zkSync Era
        SELECT
             npr.evt_block_time as block_time
            ,npr.evt_block_number as block_number
            ,npr.evt_tx_hash as tx_hash
            ,npr.evt_index
            ,et."from" as sender_address
            ,COALESCE(d.to, zt.to) as receiver_address -- d.to is null if there is no matching ERC20 deposit tx hash. This is used to handle the logic of an 'if statement'.
            ,0x0000000000000000000000000000000000000000 as bridged_token_address
            ,CAST(JSON_EXTRACT_SCALAR(npr.transaction, '$.reserved[0]') as UINT256) as bridged_token_amount_raw
            ,UINT256 '1' as source_chain_id
            ,UINT256 '324' as destination_chain_id
            ,'ethereum' as source_chain_name
            ,'zksync' as destination_chain_name
            ,et."from" as tx_from
            ,et.to as tx_to
        FROM {{ source('ethereum', 'transactions') }} et
        INNER JOIN {{ source('zksync_v2_ethereum', 'DiamondProxy_evt_NewPriorityRequest') }} npr 
            ON et.hash = npr.evt_tx_hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('npr.evt_block_time') }}
            {% endif %}
        INNER JOIN {{ source('zksync', 'transactions') }} zt 
            ON npr.txHash = zt.hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('zt.block_time') }}
            {% endif %}
        LEFT JOIN {{ source('zksync_v2_ethereum', 'L1ERC20Bridge_evt_DepositInitiated') }} d 
            ON npr.evt_tx_hash = d.evt_tx_hash 
            AND npr.evt_index = d.evt_index
            {% if is_incremental() %}
            AND {{ incremental_predicate('d.evt_block_time') }}
            {% endif %}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('et.block_time') }}
        {% endif %}

        UNION ALL

        -- Deposit ERC-20 from Ethereum to zkSync Era
        SELECT
             d.evt_block_time as block_time
            ,d.evt_block_number as block_number
            ,d.evt_tx_hash as tx_hash
            ,d.evt_index
            ,d."from" as sender_address
            ,d.to as receiver_address
            ,d.l1Token as bridged_token_address
            ,d.amount as bridged_token_amount_raw
            ,UINT256 '1' as source_chain_id
            ,UINT256 '324' as destination_chain_id
            ,'ethereum' as source_chain_name
            ,'zksync' as destination_chain_name
            ,et."from" as tx_from
            ,et.to as tx_to
        FROM {{ source('ethereum', 'transactions') }} et
        INNER JOIN {{ source('zksync_v2_ethereum', 'L1ERC20Bridge_evt_DepositInitiated') }} d 
            ON et.hash = d.evt_tx_hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('d.evt_block_time') }}
            {% endif %}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('et.block_time') }}
        {% endif %}

        UNION ALL

        -- Withdraw ETH from zkSync Era to Ethereum
        SELECT
             w.evt_block_time as block_time
            ,w.evt_block_number as block_number
            ,w.evt_tx_hash as tx_hash
            ,w.evt_index
            ,w._l2Sender as sender_address
            ,w._l1Receiver as receiver_address
            ,w.contract_address as bridged_token_address
            ,w._amount as bridged_token_amount_raw
            ,UINT256 '324' as source_chain_id
            ,UINT256 '1' as destination_chain_id
            ,'zksync' as source_chain_name
            ,'ethereum' as destination_chain_name
            ,zt."from" as tx_from
            ,zt.to as tx_to
        FROM {{ source('zksync', 'transactions') }} zt
        INNER JOIN {{ source('zksync_era_zksync', 'L2EthToken_evt_Withdrawal') }} w 
            ON zt.hash = w.evt_tx_hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('w.evt_block_time') }}
            {% endif %}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('zt.block_time') }}
        {% endif %}

        UNION ALL

        -- Withdraw ERC-20 from zkSync Era to Ethereum
        SELECT
             w.evt_block_time as block_time
            ,w.evt_block_number as block_number
            ,w.evt_tx_hash as tx_hash
            ,w.evt_index
            ,w.l2Sender as sender_address
            ,w.l1Receiver as receiver_address
            ,w.l2Token as bridged_token_address
            ,w.amount as bridged_token_amount_raw
            ,UINT256 '324' as source_chain_id
            ,UINT256 '1' as destination_chain_id
            ,'zksync' as source_chain_name
            ,'ethereum' as destination_chain_name
            ,zt."from" as tx_from
            ,zt.to as tx_to
        FROM {{ source('zksync', 'transactions') }} zt
        INNER JOIN {{ source('zksync_era_zksync', 'L2ERC20Bridge_evt_WithdrawalInitiated') }} w 
            ON zt.hash = w.evt_tx_hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('w.evt_block_time') }}
            {% endif %}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('zt.block_time') }}
        {% endif %}

        ) a
)

SELECT
    'zksync' as blockchain
    ,'native_bridge' as project
    ,'' as version
    ,tf.block_time
    ,tf.block_date
    ,tf.block_month
    ,tf.block_number
    ,tf.tx_hash
    ,tf.evt_index
    ,COALESCE(tf.sender_address,CAST(NULL as VARBINARY)) as sender_address
    ,COALESCE(tf.receiver_address, CAST(NULL as VARBINARY)) as receiver_address
    ,CASE 
        WHEN bridged_token_address = 0x0000000000000000000000000000000000000000 THEN 'ETH'
        ELSE erc.symbol
     END AS token_symbol
    ,CAST(tf.bridged_token_amount_raw as double) / POWER(10, erc.decimals) as token_amount
    ,p.price * (CAST(tf.bridged_token_amount_raw as double) / POWER(10, erc.decimals) ) as token_amount_usd
    ,tf.bridged_token_amount_raw as token_amount_raw
    ,0 as fee_amount
    ,0 as fee_amount_usd
    ,0 as fee_amount_raw
    ,tf.bridged_token_address as token_address
    ,CAST(NULL as VARBINARY) as fee_address
    ,tf.source_chain_id
    ,tf.destination_chain_id
    ,tf.source_chain_name
    ,tf.destination_chain_name
    ,1 as is_native_bridge
    ,tf.tx_from
    ,tf.tx_to
FROM bridge_events tf

LEFT JOIN {{ source('tokens', 'erc20') }} erc
    ON erc.contract_address = 
        CASE
            WHEN tf.bridged_token_address = 0x0000000000000000000000000000000000000000 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- When the token is ETH, match on WETH
            ELSE tf.bridged_token_address
        END
    AND erc.blockchain IN ('ethereum', 'zksync')
    
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = DATE_TRUNC('minute', tf.block_time)
    AND p.contract_address = 
        CASE
            WHEN tf.bridged_token_address = 0x0000000000000000000000000000000000000000 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- When the token is ETH, match on WETH
            ELSE tf.bridged_token_address
        END
    AND p.blockchain IN ('ethereum', 'zksync')
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
