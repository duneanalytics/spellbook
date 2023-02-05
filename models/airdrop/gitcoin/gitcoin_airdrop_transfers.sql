{{ config(
    alias = 'gitcoin_airdrop_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number']
    )
}}

{% set airdrop_start_date = '2021-05-24' %}
{% set airdrop_end_date = '2021-06-24' %}

SELECT
'ethereum' AS blockchain,
'Gitcoin Airdrop' AS airdrop_name,
'Gticoin' AS airdrop_project,

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

FROM {{ source('gitcoin_ethereum', 'TokenDistributor_evt_Claimed') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = '0xde30da39c46104798bb5aa3fe8b9e0e1f348163f' --GTC Token
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

