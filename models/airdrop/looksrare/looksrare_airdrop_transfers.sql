{{ config(
    alias = 'looksrare_airdrop_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number']
    )
}}


{% set airdrop_start_date = '2022-01-09' %}
{% set airdrop_end_date = '2022-03-19' %}

SELECT
'ethereum' AS blockchain,
'LOOKS Airdrop' AS airdrop_name,
'looksrare' AS airdrop_project,

tfer.user AS recipient_address,

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

FROM {{ source('looksrare_ethereum', 'LooksRareAirdrop_evt_AirdropRewardsClaim') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = lower('0xa35dce3e0e6ceb67a30b8d7f4aee721c949b5970') --LOOKS Token
INNER JOIN {{ source('ethereum','transactions') }} tx
        ON tx.block_number = tfer.evt_block_number
        AND tx.block_time = tfer.evt_block_time
        AND tx.hash = tfer.evt_tx_hash

    WHERE evt_block_time BETWEEN
         cast('{{airdrop_start_date}}' as date)
         AND cast('{{airdrop_end_date}}' as date)

        {% if is_incremental() %}
        AND tfer.evt_block_time >= date_trunc('day', now() - interval '1' week)
        AND tx.block_time >= date_trunc('day', now() - interval '1' week)
        {% endif %}

