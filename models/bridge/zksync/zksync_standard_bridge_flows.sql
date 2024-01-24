{{ config(
    
    alias = 'standard_bridge_flows',
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
        ,sender
        ,receiver
        ,bridged_token_address
        ,bridged_token_amount_raw
        ,source_chain_id
        ,destination_chain_id
        ,source_chain_name
        ,destination_chain_name
    FROM (
        -- Deposit ETH from Ethereum to zkSync Era
        SELECT
             npr.evt_block_time as block_time
            ,npr.evt_block_number as block_number
            ,npr.evt_tx_hash as tx_hash
            ,et."from" as sender
            ,COALESCE(d.to, zt.to) as receiver -- d.to is null if there is no matching ERC20 deposit tx hash. This is used to handle the log of an 'if statement'.
            ,0x0000000000000000000000000000000000000000 as bridged_token_address
            ,CAST(JSON_EXTRACT_SCALAR(npr.transaction, '$.reserved[0]') as UINT256) as bridged_token_amount_raw
            ,UINT256 '1' as source_chain_id
            ,UINT256 '324' as destination_chain_id
            ,'Ethereum' as source_chain_name
            ,'zkSync Era' as destination_chain_name
        FROM {{ source('zksync_v2_ethereum', 'DiamondProxy_evt_NewPriorityRequest') }} npr
        LEFT JOIN {{ source('ethereum', 'transactions') }} et ON npr.evt_tx_hash = et.hash
        LEFT JOIN {{ source('zksync', 'transactions') }} zt ON npr.txHash = zt.hash
        LEFT JOIN {{ source('zksync_v2_ethereum', 'L1ERC20Bridge_evt_DepositInitiated') }} d ON npr.evt_tx_hash = d.evt_tx_hash
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('npr.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- Deposit ERC-20 from Ethereum to zkSync Era
        SELECT
             d.evt_block_time as block_time
            ,d.evt_block_number as block_number
            ,d.evt_tx_hash as tx_hash
            ,d."from" as sender
            ,d.to as receiver
            ,d.l1Token as bridged_token_address
            ,d.amount as bridged_token_amount_raw
            ,UINT256 '1' as source_chain_id
            ,UINT256 '324' as destination_chain_id
            ,'Ethereum' as source_chain_name
            ,'zkSync Era' as destination_chain_name
        FROM {{ source('zksync_v2_ethereum', 'L1ERC20Bridge_evt_DepositInitiated') }} d
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('d.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- Withdraw ETH from zkSync Era to Ethereum
        SELECT
             w.evt_block_time as block_time
            ,w.evt_block_number as block_number
            ,w.evt_tx_hash as tx_hash
            ,w._l2Sender as sender
            ,w._l1Receiver as receiver
            ,w.contract_address as bridged_token_address
            ,w._amount as bridged_token_amount_raw
            ,UINT256 '324' as source_chain_id
            ,UINT256 '1' as destination_chain_id
            ,'zkSync Era' as source_chain_name
            ,'Ethereum' as destination_chain_name
        FROM {{ source('zksync_era_zksync', 'L2EthToken_evt_Withdrawal') }} w
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('w.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- Withdraw ERC-20 from zkSync Era to Ethereum
        SELECT
             w.evt_block_time as block_time
            ,w.evt_block_number as block_number
            ,w.evt_tx_hash as tx_hash
            ,w.l2Sender as sender
            ,w.l1Receiver as receiver
            ,w.l2Token as bridged_token_address
            ,w.amount as bridged_token_amount_raw
            ,UINT256 '324' as source_chain_id
            ,UINT256 '1' as destination_chain_id
            ,'zkSync Era' as source_chain_name
            ,'Ethereum' as destination_chain_name
        FROM {{ source('zksync_era_zksync', 'L2ERC20Bridge_evt_WithdrawalInitiated') }} w
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('w.evt_block_time') }}
        {% endif %}

        ) a
)

SELECT
    'zksync' as blockchain
    ,'standard_bridge' as project
    ,'' as version
    ,tf.block_time
    ,tf.block_date
    ,tf.block_month
    ,tf.block_number
    ,tx_hash
    ,COALESCE(sender,CAST(NULL as VARBINARY)) as sender
    ,COALESCE(receiver, CAST(NULL as VARBINARY)) as receiver
    ,erc.symbol as token_symbol
    ,CAST(bridged_token_amount_raw as double)/ POWER(10,erc.decimals) as token_amount
    ,p.price*( CAST(bridged_token_amount_raw as double) / POWER(10,erc.decimals) ) as token_amount_usd
    ,bridged_token_amount_raw as token_amount_raw
    ,0 as fee_amount
    ,0 as fee_amount_usd
    ,0 as fee_amount_raw
    ,bridged_token_address as token_address
    ,CAST(NULL as VARBINARY) as fee_address
    ,source_chain_id
    ,destination_chain_id
    ,cid_source.chain_name as source_chain_name
    ,cid_dest.chain_name as destination_chain_name
    ,1 as is_native_bridge
    ,t."from" as tx_from
    ,t.to as tx_to
    ,tf.transfer_id
    ,tf.evt_index
    ,tf.trace_address
    ,bytearray_substring(t.data,1,4) as tx_method_id
FROM bridge_events tf

LEFT JOIN {{ source('zksync', 'transactions') }} t
        ON t.block_time = tf.block_time
        AND t.hash = tf.tx_hash
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
        {% endif %}
        
LEFT JOIN {{ ref('tokens_erc20') }} erc
    ON erc.blockchain = 'zksync'
    AND erc.contract_address = tf.bridged_token_address
    
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = DATE_TRUNC('minute',tf.block_time)
    AND p.blockchain = 'zksync'
    AND p.contract_address = tf.bridged_token_address
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
