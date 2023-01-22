{{ 
    config(
        alias ='tokens', 
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

SELECT transfer_from_address,
        transfer_to_address,
        contract_address,
        token_standard,
        token_type,
        token_id,
        transfer_type,
        CASE
                WHEN transfer_from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
                WHEN transfer_to_address = '0x0000000000000000000000000000000000000000' THEN 'burn'
                ELSE 'transfer'
                END 
        AS transfer_style,
        transfer_tx_type,
        value,
        tx_block_time,
        DATE_TRUNC('day',tx_block_time) AS tx_block_date,
        tx_block_number,
        tx_hash,
        tx_method_id,
        tx_from_address,
        tx_to_address,
        evt_index,
        trace_address,
        unique_transfer_id

FROM (
        -----------
        -- ERC20 --
        -----------
        SELECT

        r.`from` AS transfer_from_address,
        r.to AS transfer_to_address,
        r.contract_address AS contract_address,
        'erc20' AS token_standard,
        'fungible' AS token_type,
        NULL AS token_id, -- used by NFTs
        NULL AS transfer_type, -- used by NFTs
        -- is the transaction an erc20 transfer, or did this happen in an internal transaction?
        CASE WHEN substring(t.data, 1, 10) = '0xa9059cbb'
                THEN 'transfer transaction'
                ELSE 'internal transaction'
                END
        AS transfer_tx_type,
        r.value,
        r.evt_block_time AS tx_block_time,
        r.evt_block_number AS tx_block_number,
        r.evt_tx_hash AS tx_hash,

        substring(t.data, 1, 10) AS tx_method_id,
        t.`from` AS tx_from_address,
        t.to AS tx_to_address,

        r.evt_index,
        NULL AS trace_address,

        evt_tx_hash || '-' || CAST(evt_index AS VARCHAR(100))  as unique_transfer_id

        FROM {{ source('erc20_ethereum', 'evt_transfer') }} r

        inner join {{ source('optimism', 'transactions') }} as t 
                on r.evt_tx_hash = t.hash
                and r.evt_block_number = t.block_number

        -- exclude ETH placeholder token transfer since this is handled in ETH transfers
        where r.contract_address != lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.block_time >= date_trunc('day', now() - interval '1 week')
        and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        ----------
        -- ETH --
        ----------
        UNION ALL

        SELECT

        r.`from` AS transfer_from_address,
        r.to AS transfer_to_address,
        r.contract_address AS contract_address,
        'eth' AS token_standard,
        'fungible' AS token_type,
        NULL AS token_id, -- used by NFTs
        NULL AS transfer_type, -- used by NFTs
        -- is the transaction an eth transfer, or did this happen in an internal transaction?
        CASE
                WHEN gas_used = 21000 AND tx_method_id = '0x' THEN 'transfer transaction'
                WHEN tx_method_id = '0xd0e30db0' THEN 'eth wrap'
                WHEN tx_method_id = '0x2e1a7d4d' THEN 'eth unwrap'
                ELSE 'internal transaction' END
        AS transfer_tx_type,
        r.value,
        tx_block_time,
        tx_block_number,

        tx_method_id,
        t.`from` AS tx_from_address,
        t.to AS tx_to_address,

        NULL AS evt_index,
        r.trace_address,

        r.tx_hash || '-' || cast(r.trace_address as string) as unique_transfer_id

        FROM {{ ref('transfers_optimism_eth') }} t

        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        where 
                r.block_time >= date_trunc('day', now() - interval '1 week')
                and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        ----------
        -- NFT --
        ----------
        UNION ALL
        
        SELECT

        r.`from` AS transfer_from_address,
        r.to AS transfer_to_address,
        r.contract_address AS contract_address,
        token_standard,
        'nft' AS token_type,
        token_id,
        transfer_type,
        -- is the transaction an erc20 transfer, or did this happen in an internal transaction?
        CASE WHEN substring(t.data, 1, 10)
                        IN (    '0x42842e0e','0x23b872dd','0xb88d4fde','0xf3993d11' --erc721
                                ,'0xf242432a','0x2eb2c2d6' --erc1155
                        )
                THEN 'transfer transaction'
                ELSE 'internal transaction'
                END
        AS transfer_tx_type,
        amount as value,
        r.evt_block_time AS tx_block_time,
        r.evt_block_number AS tx_block_number,
        r.evt_tx_hash AS tx_hash,

        substring(t.data, 1, 10) AS tx_method_id,
        t.`from` AS tx_from_address,
        t.to AS tx_to_address,

        r.evt_index,
        NULL AS trace_address,

        evt_tx_hash || '-' || CAST(evt_index AS VARCHAR(100))  as unique_transfer_id

        FROM {{ ref('nft_optimism_transfers') }} r

        inner join {{ source('optimism', 'transactions') }} as t 
                on r.evt_tx_hash = t.hash
                and r.evt_block_number = t.block_number

        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        where r.block_time >= date_trunc('day', now() - interval '1 week')
        and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

) tfs