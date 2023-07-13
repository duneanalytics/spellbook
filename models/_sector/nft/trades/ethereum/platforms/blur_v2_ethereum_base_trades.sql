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

{% set blur_v2_start_date = "cast('2023-07-05' as timestamp)" %}

WITH blur_v2_trades AS (
    SELECT evt_tx_hash AS tx_hash
    , ROUND(CAST((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256))) AS BIGINT), 8) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , NULL AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , CAST(bitwise_right_shift(collectionPriceSide, 248) AS BIGINT) AS order_type
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(collectionPriceSide AS varbinary)) AS varchar), 40))) AS nft_contract_address
    , orderHash AS order_hash
    , bitwise_right_shift(tokenIdListingIndexTrader, 168) AS nft_token_id
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(tokenIdListingIndexTrader AS varbinary)) AS varchar), 40))) AS trader
    , CAST(0 AS double) AS fee
    , NULL AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721Packed') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= {{blur_v2_start_date}}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , ROUND(CAST((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256))) AS BIGINT), 8) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , 'maker' AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , CAST(bitwise_right_shift(collectionPriceSide, 248) AS BIGINT) AS order_type
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(collectionPriceSide AS varbinary)) AS varchar), 40))) AS nft_contract_address
    , orderHash AS order_hash
    , bitwise_right_shift(tokenIdListingIndexTrader, 168) AS nft_token_id
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(tokenIdListingIndexTrader AS varbinary)) AS varchar), 40))) AS trader
    , CAST(bitwise_right_shift(makerFeeRecipientRate, 160) AS double)/10000 AS fee
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(makerFeeRecipientRate AS varbinary)) AS varchar), 40))) AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721MakerFeePacked') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= {{blur_v2_start_date}}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , ROUND(CAST((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256))) AS BIGINT), 8) AS price_raw
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , 'taker' AS fee_side
    , evt_index
    , contract_address AS project_contract_address
    , CAST(bitwise_right_shift(collectionPriceSide, 248) AS BIGINT) AS order_type
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(collectionPriceSide AS varbinary)) AS varchar), 40))) AS nft_contract_address
    , orderHash AS order_hash
    , bitwise_right_shift(tokenIdListingIndexTrader, 168) AS nft_token_id
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(tokenIdListingIndexTrader AS varbinary)) AS varchar), 40))) AS trader
    , CAST(bitwise_right_shift(takerFeeRecipientRate, 160) AS double)/10000 AS fee
    , from_hex('0x' || LOWER("RIGHT"(CAST(to_hex(CAST(takerFeeRecipientRate AS varbinary)) AS varchar), 40))) AS royalty_fee_address
    FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721TakerFeePacked') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= {{blur_v2_start_date}}
    {% endif %}
    )

SELECT date_trunc('day', block_time) AS block_date
, bt.block_number
, bt.tx_hash
, bt.evt_index AS sub_tx_trade_id
, CASE WHEN txs."from" = bt.trader THEN 'Sell' ELSE 'Buy' END AS trade_category
, 'secondary' AS trade_type
, CASE WHEN txs."from" = bt.trader THEN bt.trader ELSE txs."from" END AS buyer
, CASE WHEN txs."from" = bt.trader THEN txs."from" ELSE bt.trader END AS seller
, bt.nft_contract_address
, bt.nft_token_id
, 1 AS nft_amount
, bt.price_raw
, CASE WHEN bt.order_type = 0 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE 0x0000000000a39bb272e79075ade125fd351887ac END AS currency_contract
, bt.project_contract_address
, NULL AS platform_fee_amount_raw
, NULL AS platform_fee_address
, bt.price_raw * bt.fee AS royalty_fee_amount_raw
, bt.royalty_fee_address
FROM blur_v2_trades bt
INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=bt.block_number
    AND txs.hash=bt.tx_hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc("day", now() - interval '7' day)
    {% else %}
    AND txs.block_time >= {{blur_v2_start_date}}
    {% endif %}