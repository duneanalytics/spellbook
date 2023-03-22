{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "stealcam",
                                \'["hildobby"]\') }}')
}}


{% set project_start_date = '2023-03-10' %}

SELECT 'arbitrum' AS blockchain
, 'Stealcam' AS project
, 'v1' AS version
, sc.evt_block_time AS block_time
, date_trunc('day', sc.evt_block_time) AS block_date
, sc.evt_block_number AS block_number
, 'Single Item Trade' AS trade_type
, 'Buy' AS trade_category
, CASE WHEN sc.value=0 THEN 'Mint' ELSE 'Trade' END AS evt_type
, sc.from AS seller
, sc.to AS buyer
, sc.contract_address AS nft_contract_address
, 'Stealcam' AS collection
, sc.id AS token_id
, 'erc721' AS token_standard
, 1 AS number_of_items
, '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' AS currency_contract
, 'ETH' AS currency_symbol
, sc.value AS amount_raw
, sc.value/POWER(10, 18) AS amount_original
, pu.price*sc.value/POWER(10, 18) AS amount_usd
, sc.contract_address AS project_contract_address
, NULL AS aggregator_name
, NULL AS aggregator_address
, sc.evt_tx_hash AS tx_hash
, at.from AS tx_from
, at.to AS tx_to
, COALESCE(sc.value-(roy.value+not_fee.value), 0) AS platform_fee_amount_raw
, COALESCE((sc.value-(roy.value+not_fee.value))/POWER(10, 18), 0) AS platform_fee_amount
, COALESCE(pu.price*(sc.value-(roy.value+not_fee.value))/POWER(10, 18), 0) AS platform_fee_amount_usd
, COALESCE(100*(sc.value-(roy.value+not_fee.value))/sc.value, 0) AS platform_fee_percentage
, CASE WHEN sc.value-(roy.value+not_fee.value) > 0 THEN 'ETH' ELSE NULL END AS royalty_fee_currency_symbol
, COALESCE(roy.value, 0) AS royalty_fee_amount_raw
, COALESCE(roy.value/POWER(10, 18), 0) AS royalty_fee_amount
, COALESCE(roy.value/POWER(10, 18)/pu.price, 0) AS royalty_fee_amount_usd
, COALESCE(100*roy.value/sc.value, 0) AS royalty_fee_percentage
, m._creator AS royalty_fee_receive_address
, 'arbitrum-stealcam-' || sc.evt_tx_hash || '-' || sc.evt_index AS unique_trade_id
FROM {{ source('stealcam_arbitrum', 'Stealcam_evt_Stolen') }} sc
INNER JOIN {{ source('arbitrum', 'transactions') }} at ON at.block_number=sc.evt_block_number
    AND at.hash=sc.evt_tx_hash
    {% if is_incremental() %}
    AND at.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND at.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN prices.usd_forward_fill pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND pu.minute=date_trunc('minute', sc.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute >= '{{project_start_date}}'
    {% endif %}
INNER JOIN {{ source('stealcam_arbitrum', 'Stealcam_call_mint') }} m ON m.call_success
    AND m.id=sc.id
    {% if is_incremental() %}
    AND m.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND m.call_block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('arbitrum', 'traces') }} roy ON roy.block_number=sc.evt_block_number
    AND roy.tx_hash=sc.evt_tx_hash
    AND roy.from=sc.contract_address
    AND roy.to=m._creator
    {% if is_incremental() %}
    AND roy.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND roy.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('arbitrum', 'traces') }} not_fee ON not_fee.block_number=sc.evt_block_number
    AND not_fee.tx_hash=sc.evt_tx_hash
    AND not_fee.from=sc.contract_address
    AND not_fee.to=sc.from
    {% if is_incremental() %}
    AND not_fee.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND not_fee.block_time >= '{{project_start_date}}'
    {% endif %}
{% if is_incremental() %}
WHERE sc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
WHERE sc.evt_block_time >= '{{project_start_date}}'
{% endif %}