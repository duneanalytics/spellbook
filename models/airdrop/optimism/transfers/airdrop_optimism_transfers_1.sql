{{ config(
    schema = 'airdrop_optimism',
    alias = 'airdrop_1_transfers',
    partition_by = ['transfer_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_tx_hash', 'transfer_evt_index', 'transfer_block_number'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ovm_optimism",
                                \'["msilb7"]\') }}'
    )
}}

SELECT
'Airdrop #1' AS airdrop_name,

tfer."from" AS distributor_address,
tfer.to AS recipient_address,

tfer.contract_address AS airdrop_token_address,
r.symbol AS airdrop_token_symbol,
DATE_TRUNC('day',tfer.evt_block_time) AS transfer_block_date,
tfer.evt_block_time AS transfer_block_time,
tfer.evt_block_number AS transfer_block_number,
tfer.evt_tx_hash AS transfer_tx_hash,
tfer.evt_index AS transfer_evt_index,

cast(tfer.value as double) / cast(POWER(10,r.decimals) as double) AS airdrop_token_amount,
cast(tfer.value as double) AS airdrop_token_amount_raw,

tx."from" AS tx_from_address,
tx.to AS tx_to_address,
substring(tx.data,1,10) AS tx_method_id

FROM {{ source('erc20_optimism', 'evt_transfer') } tfer
INNER JOIN {{ ref('tokens_optimism_erc20') }} r
        ON r.contract_address = tfer.contract_address
INNER JOIN {{ source('optimism','transactions') }} tx
        ON tx.block_number = tfer.evt_block_number
        AND tx.block_time = tfer.evt_block_time
        AND tx.hash = tfer.evt_tx_hash

    WHERE "from" = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de' --claim distributor contract
      AND tfer.contract_address = '0x4200000000000000000000000000000000000042' --OP Token
      AND evt_block_time BETWEEN
         cast('2022-05-31' as date) -- launched May 31, 2022
         AND cast('2023-06-01' as date) -- 1 year claim period

        {% if is_incremental() %}
        AND tfer.evt_block_time >= date_trunc('day', now() - interval '1' week)
        AND tx.block_time >= date_trunc('day', now() - interval '1' week)
        {% endif %}

