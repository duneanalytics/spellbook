{{ config(
    alias = 'optimism_op_airdrop2_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number']
    )
}}

{% set airdrop_start_date = '2023-02-09' %}
{% set airdrop_end_date = '2023-02-11' %}

SELECT
'Optimism Airdrop #2' AS airdrop_name,

tfer.account AS recipient_address,

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

FROM {{ source('erc20_optimism', 'evt_transfer') }} tfer
INNER JOIN {{ ref('tokens_optimism_erc20') }} tk
        ON r.contract_address = '0x4200000000000000000000000000000000000042' --OP Token
INNER JOIN {{ source('optimism','transactions') }} tx
        ON tx.block_number = tfer.evt_block_number
        AND tx.block_time = tfer.evt_block_time
        AND tx.hash = tfer.evt_tx_hash
        AND tx.to = '0xbe9a9b1b07f027130e56d8569d1aea5dd5a86013'
        AND tx.block_time BETWEEN
            cast('{{airdrop_start_date}}' as date)
            AND cast('{{airdrop_end_date}}' as date)

    WHERE tfer.evt_block_time BETWEEN
         cast('{{airdrop_start_date}}' as date)
         AND cast('{{airdrop_end_date}}' as date)

        {% if is_incremental() %}
        AND tfer.evt_block_time >= date_trunc('day', now() - interval '1' week)
        AND tx.block_time >= date_trunc('day', now() - interval '1' week)
        {% endif %}

