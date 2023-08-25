{{ config(
    schema = 'blur_v2_ethereum',
    tags = ['dunesql'],
    alias = alias('base_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set blur_v2_start_date = '2023-07-05' %}

WITH blur_v2_trades AS (
    SELECT evt_tx_hash AS tx_hash
    , bytearray_to_uint256(bytearray_substring(cast(collectionPriceSide as varbinary),2,11)) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , NULL AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , bytearray_to_bigint(bytearray_substring(cast(collectionPriceSide as varbinary),1,1)) AS order_type
    , bytearray_substring(cast(collectionPriceSide as varbinary),13,20) AS nft_contract_address
    , orderHash AS order_hash
    , bytearray_to_uint256(bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),1,11)) AS nft_token_id
    , bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),13,20) AS trader
    , CAST(0 AS double) AS fee
    , NULL AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721Packed') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{blur_v2_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , bytearray_to_uint256(bytearray_substring(cast(collectionPriceSide as varbinary),2,11)) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , 'maker' AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , bytearray_to_bigint(bytearray_substring(cast(collectionPriceSide as varbinary),1,1)) AS order_type
    , bytearray_substring(cast(collectionPriceSide as varbinary),13,20) AS nft_contract_address
    , orderHash AS order_hash
    , bytearray_to_uint256(bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),1,11)) AS nft_token_id
    , bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),13,20) AS trader
    , CAST(bitwise_right_shift(makerFeeRecipientRate, 160) AS double)/10000 AS fee
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(makerFeeRecipientRate AS varbinary)) AS varchar), 40))) AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721MakerFeePacked') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{blur_v2_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , bytearray_to_uint256(bytearray_substring(cast(collectionPriceSide as varbinary),2,11)) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , 'taker' AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , bytearray_to_bigint(bytearray_substring(cast(collectionPriceSide as varbinary),1,1)) AS order_type
    , bytearray_substring(cast(collectionPriceSide as varbinary),13,20) AS nft_contract_address
    , orderHash AS order_hash
    , bytearray_to_uint256(bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),1,11)) AS nft_token_id
    , bytearray_substring(cast(tokenIdListingIndexTrader as varbinary),13,20) AS trader
    , CAST(bitwise_right_shift(takerFeeRecipientRate, 160) AS double)/10000 AS fee
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(takerFeeRecipientRate AS varbinary)) AS varchar), 40))) AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721TakerFeePacked') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{blur_v2_start_date}}'
    {% endif %}
    )

SELECT CAST(date_trunc('day', bt.block_time) AS date) AS block_date
, bt.block_time
, bt.block_number
, bt.tx_hash
, bt.evt_index AS sub_tx_trade_id
, CASE WHEN bt.order_type = 1 THEN 'Sell' ELSE 'Buy' END AS trade_category
, 'secondary' AS trade_type
, CASE WHEN bt.order_type = 1 THEN bt.trader ELSE txs."from" END AS buyer
, CASE WHEN bt.order_type = 0 THEN bt.trader ELSE txs."from" END AS seller
, bt.nft_contract_address
, bt.nft_token_id AS nft_token_id
, CAST(1 AS UINT256) AS nft_amount
, bt.price_raw
, CASE WHEN bt.order_type = 0 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE 0x0000000000a39bb272e79075ade125fd351887ac END AS currency_contract
, bt.project_contract_address
, CAST(0 AS uint256) AS platform_fee_amount_raw
, CAST(NULL AS varbinary) AS platform_fee_address
, CAST(ROUND(bt.price_raw * bt.fee) AS UINT256) AS royalty_fee_amount_raw
, bt.royalty_fee_address
FROM blur_v2_trades bt
INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=bt.block_number
    AND txs.hash=bt.tx_hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND txs.block_time >= TIMESTAMP '{{blur_v2_start_date}}'
    {% endif %}