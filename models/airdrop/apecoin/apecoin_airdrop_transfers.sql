{{ config(
    alias = 'apecoin_airdrop_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number']
    )
}}

-- https://dune.com/queries/511739 h/t hildobby
{% set airdrop_start_date = '2022-03-16' %}
{% set airdrop_end_date = '2022-03-19' %}

SELECT
'ethereum' AS blockchain,
'APE Airdrop' AS airdrop_name,
'Apecoin' AS airdrop_project,

tfer. account AS recipient_address,

r.contract_address AS airdrop_token_address,
r.symbol AS airdrop_token_symbol,
DATE_TRUNC('day',tfer.evt_block_time) AS transfer_block_date,
tfer.evt_block_time AS transfer_block_time,
tfer.evt_block_number AS transfer_block_number,
tfer.evt_tx_hash AS transfer_tx_hash,
tfer.evt_index AS transfer_evt_index,

cast(10094 as double) AS airdrop_token_amount,
cast(10094*POWER(10,decimals) as double) AS airdrop_token_amount_raw,

tx.`from` AS tx_from_address,
tx.to AS tx_to_address,
substring(tx.data,1,10) AS tx_method_id

FROM {{ source('apecoin_ethereum', 'AirdropGrapesToken_evt_AlphaClaimed') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = '0xba30e5f9bb24caa003e9f2f0497ad287fdf95623' --APE Token
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

UNION ALL

SELECT
'ethereum' AS blockchain,
'APE Airdrop' AS airdrop_name,
'Apecoin' AS airdrop_project,

tfer. account AS recipient_address,

r.contract_address AS airdrop_token_address,
r.symbol AS airdrop_token_symbol,
DATE_TRUNC('day',tfer.evt_block_time) AS transfer_block_date,
tfer.evt_block_time AS transfer_block_time,
tfer.evt_block_number AS transfer_block_number,
tfer.evt_tx_hash AS transfer_tx_hash,
tfer.evt_index AS transfer_evt_index,

cast(2042 as double) AS airdrop_token_amount,
cast(2042*POWER(10,decimals) as double) AS airdrop_token_amount_raw,

tx.`from` AS tx_from_address,
tx.to AS tx_to_address,
substring(tx.data,1,10) AS tx_method_id

FROM {{ source('apecoin_ethereum', 'AirdropGrapesToken_evt_BetaClaimed') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = '0xba30e5f9bb24caa003e9f2f0497ad287fdf95623' --APE Token
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

UNION ALL

SELECT
'ethereum' AS blockchain,
'APE Airdrop' AS airdrop_name,
'Apecoin' AS airdrop_project,

tfer. account AS recipient_address,

r.contract_address AS airdrop_token_address,
r.symbol AS airdrop_token_symbol,
DATE_TRUNC('day',tfer.evt_block_time) AS transfer_block_date,
tfer.evt_block_time AS transfer_block_time,
tfer.evt_block_number AS transfer_block_number,
tfer.evt_tx_hash AS transfer_tx_hash,
tfer.evt_index AS transfer_evt_index,

cast(856 as double) AS airdrop_token_amount,
cast(856*POWER(10,decimals) as double) AS airdrop_token_amount_raw,

tx.`from` AS tx_from_address,
tx.to AS tx_to_address,
substring(tx.data,1,10) AS tx_method_id

FROM {{ source('apecoin_ethereum', 'AirdropGrapesToken_evt_GammaClaimed') }} tfer
INNER JOIN {{ ref('tokens_ethereum_erc20') }} r
        ON r.contract_address = '0xba30e5f9bb24caa003e9f2f0497ad287fdf95623' --APE Token
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

