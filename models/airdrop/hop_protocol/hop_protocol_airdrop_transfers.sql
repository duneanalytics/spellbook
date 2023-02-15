{{ config(
    alias = 'hop_airdrop_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number']
    )
}}


{% set airdrop_start_date = '2022-06-08' %}

SELECT
'ethereum' AS blockchain,
'HOP Airdrop' AS airdrop_name,
'hop_protocol' AS airdrop_project,

tfer.claimant AS recipient_address,

r.contract_address AS airdrop_token_address,
r.symbol AS airdrop_token_symbol,
DATE_TRUNC('day',tfer.evt_block_time) AS transfer_block_date,
tfer.evt_block_time AS transfer_block_time,
tfer.evt_block_number AS transfer_block_number,
tfer.evt_tx_hash AS transfer_tx_hash,
tfer.evt_index AS transfer_evt_index,

cast(tfer.amount as double) / cast(POWER(10,r.decimals) as double) AS airdrop_token_amount,
cast(tfer.amount as double) AS airdrop_token_amount_raw,

tx.`from` AS tx_from_address,
tx.to AS tx_to_address,
substring(tx.data,1,10) AS tx_method_id

FROM {{ source('hop_protocol_ethereum', 'HOPToken_evt_Claim') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = lower('0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc') --HOP Token
        AND r.contract_address = tfer.contract_address
INNER JOIN {{ source('ethereum','transactions') }} tx
        ON tx.block_number = tfer.evt_block_number
        AND tx.block_time = tfer.evt_block_time
        AND tx.hash = tfer.evt_tx_hash

    WHERE evt_block_time >=
         cast('{{airdrop_start_date}}' as date)

        {% if is_incremental() %}
        AND tfer.evt_block_time >= date_trunc('day', now() - interval '1' week)
        AND tx.block_time >= date_trunc('day', now() - interval '1' week)
        {% endif %}

