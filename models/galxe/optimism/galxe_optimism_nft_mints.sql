{{ config(
    alias = 'nft_mints',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'nft_contract_address', 'tokenId', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "galxe",
                                \'["msilb7"]\') }}'
    )
}}
--SELECT MIN(block_time) FROM optimism.transactions where to = 0x2e42f214467f647Fe687Fd9a2bf3BAdDFA737465
{% set project_start_date = '2022-07-17' %}
{% set spacestation = '0x2e42f214467f647fe687fd9a2bf3baddfa737465' %}

SELECT
    cast( DATE_TRUNC('month',block_time) as date) AS block_month,
    block_time,
    block_number,
    t."from" as tx_from,
    t.to as tx_to,
    t.hash AS tx_hash,
    bytearray_substring(t.data,1,4) AS tx_method_id,
    tfer.to AS token_transfer_to,
    tfer.contract_address AS nft_contract_address,
    tfer.tokenId,
    tfer.evt_index

FROM
    {{source('optimism','transactions')}} t
INNER JOIN {{source('erc721_optimism','evt_transfer')}} tfer 
    ON t.hash = tfer.evt_tx_hash
    AND t.block_number = tfer.evt_block_number
    AND tfer."from" = 0x0000000000000000000000000000000000000000 --mint
    AND tfer.evt_block_time >= cast( '{{project_start_date}}' as timestamp)
    {% if is_incremental() %}
    AND tfer.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

WHERE success = true
    AND t.to = {{spacestation}}
AND block_time >= timestamp '{{project_start_date}}'
{% if is_incremental() %}
AND block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
