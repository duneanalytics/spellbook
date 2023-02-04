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
        -- mint, burn, or transfer
        CASE
                WHEN transfer_from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
                WHEN transfer_to_address = '0x0000000000000000000000000000000000000000' THEN 'burn'
                ELSE 'transfer'
                END 
        AS transfer_style,

        -- is the transfer part of the top-level transaction, or is it internal?
        CASE WHEN substring(t.data, 1, 10)
                        IN (    '0x42842e0e','0x23b872dd','0xb88d4fde','0xf3993d11' --erc721
                                ,'0xf242432a','0x2eb2c2d6' --erc1155
                                , '0xa9059cbb' --erc20
                        )
                        THEN 'transfer transaction'
                WHEN t.gas_used = 21000 AND substring(t.data, 1, 10) = '0x'
                        THEN 'transfer transaction' -- we specify gas_used because some arb bots will send null data as well.
                WHEN substring(t.data, 1, 10) = '0xd0e30db0'
                        THEN 'eth wrap'
                WHEN substring(t.data, 1, 10) = '0x2e1a7d4d'
                        THEN 'eth unwrap'
                ELSE 'internal transaction'
                END
        AS transfer_tx_type,

        cast(tfs.value as double) AS value,
        tx_block_time,
        DATE_TRUNC('day',tx_block_time) AS tx_block_date,
        tx_block_number,
        tx_hash,
        substring(t.data, 1, 10) AS tx_method_id,
        t.`from` AS tx_from_address,
        t.to AS tx_to_address,
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
        
        r.value,
        r.evt_block_time AS tx_block_time,
        r.evt_block_number AS tx_block_number,
        r.evt_tx_hash AS tx_hash,

        r.evt_index,
        NULL AS trace_address,

        r.evt_tx_hash || '-' || CAST(evt_index AS VARCHAR(100))  as unique_transfer_id

        FROM {{ source('erc20_optimism', 'evt_transfer') }} r

        -- exclude ETH placeholder token transfer since this is handled in ETH transfers
        where r.contract_address != lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.evt_block_time >= date_trunc('day', now() - interval '1 week')
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
        r.value,
        tx_block_time,
        tx_block_number,
        tx_hash,

        NULL AS evt_index,
        r.trace_address,

        r.unique_transfer_id

        FROM {{ ref('transfers_optimism_eth') }} r

        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        where r.tx_block_time >= date_trunc('day', now() - interval '1 week')
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
        amount as value,
        r.block_time AS tx_block_time,
        r.block_number AS tx_block_number,
        r.tx_hash AS tx_hash,

        r.evt_index,
        NULL AS trace_address,

        r.unique_transfer_id

        FROM {{ ref('nft_optimism_transfers') }} r

        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        where r.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

) tfs
inner join {{ source('optimism', 'transactions') }} as t 
                on tfs.tx_hash = t.hash
                and tfs.tx_block_number = t.block_number
                {% if is_incremental() %} -- this filter will only be applied on an incremental run 
                and t.block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
