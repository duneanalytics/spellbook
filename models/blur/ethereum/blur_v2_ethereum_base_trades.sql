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
    , ROUND((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256)))/POWER(10, 18), 8) AS price
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
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{blur_v2_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , ROUND((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256)))/POWER(10, 18), 8) AS price
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
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{blur_v2_start_date}}'
    {% endif %}
    
    UNION ALL
    
    SELECT evt_tx_hash AS tx_hash
    , ROUND((bitwise_right_shift(collectionPriceSide, 160) - (bitwise_right_shift(collectionPriceSide, 248) * CAST(power(2, 88) AS UINT256)))/POWER(10, 18), 8) AS price
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
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{blur_v2_start_date}}'
    {% endif %}
    )

SELECT 'blur' AS project
, 'v2' AS project_version
, bt.block_number
, bt.order_type
, bt.tx_hash
, bt.evt_index AS sub_tx_trade_id
, CASE WHEN txs."from" = bt.trader THEN 'Sell' ELSE 'Buy' END AS trade_category
, 'secondary' AS trade_type
, CASE WHEN txs."from" = bt.trader THEN bt.trader ELSE txs."from" END AS buyer
, CASE WHEN txs."from" = bt.trader THEN txs."from" ELSE bt.trader END AS seller
, bt.nft_contract_address
, bt.nft_token_id
, 1 AS nft_amount
, bt.price * POWER(10, 18) AS price_raw
, CASE WHEN bt.order_type = 0 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE 0x0000000000a39bb272e79075ade125fd351887ac END AS currency_contract
, bt.project_contract_address
, NULL AS platform_fee_amount_raw
, NULL AS platform_fee_address
, bt.price * POWER(10, 18) * bt.fee AS royalty_fee_amount_raw
, bt.royalty_fee_address
FROM blur_v2_trades bt
INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=bt.block_number
    AND txs.hash=bt.tx_hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND txs.block_time >= '{{blur_v2_start_date}}'
    {% endif %}








WITH
regular_blur_sales as (
    SELECT
          date_trunc('day', bm.evt_block_time) AS block_date
        , bm.evt_block_time AS block_time
        , bm.evt_block_number AS block_number
        , get_json_object(bm.buy, '$.collection') AS nft_contract_address
        , get_json_object(bm.sell, '$.tokenId') AS nft_token_id
        , CAST(get_json_object(bm.buy, '$.amount') AS INT) AS nft_amount
        , get_json_object(bm.sell, '$.trader') AS seller
        , get_json_object(bm.buy, '$.trader') AS buyer
        , CASE WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x00000000006411739da1c40b106f8511de5d1fac', '0x0000000000dab4a563819e8fd93dba3b25bc3495') THEN 'Buy'
            WHEN get_json_object(bm.buy, '$.matchingPolicy') IN ('0x0000000000b92d5d043faf7cecf7e2ee6aaed232') THEN 'Sell'
            ELSE CAST(null as VARCHAR(1))
            END AS trade_category
        , 'secondary' AS trade_type
        , CAST(get_json_object(bm.buy, '$.price') AS DECIMAL(38,0)) AS price_raw
        , get_json_object(bm.buy, '$.paymentToken') AS currency_contract
        , bm.contract_address AS project_contract_address
        , bm.evt_tx_hash AS tx_hash
        , 0 AS platform_fee_amount_raw  -- Hardcoded 0% platform fee
        , CAST(COALESCE(get_json_object(bm.buy, '$.price')*get_json_object(get_json_object(bm.sell, '$.fees[0]'), '$.rate')/10000, 0) AS DECIMAL(38,0)) AS royalty_fee_amount_raw
        , get_json_object(bm.sell, '$.fees[0].recipient') AS royalty_fee_address
        , cast(NULL as varchar(1)) as platform_fee_address
        , bm.evt_index as sub_tx_trade_id
    FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
    {% if is_incremental() %}
    WHERE bm.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE bm.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)

, seaport_blur_sales as (
SELECT
      date_trunc('day', s.evt_block_time) AS block_date
    , s.evt_block_time AS block_time
    , s.evt_block_number AS block_number
    , get_json_object(s.offer[0], '$.token') AS nft_contract_address
    , get_json_object(s.offer[0], '$.identifier') AS nft_token_id
    , CAST(get_json_object(s.offer[0], '$.amount') AS DECIMAL(38,0)) AS nft_amount
    , s.offerer AS seller
    , s.recipient AS buyer
    , 'Buy' AS trade_category
    , 'secondary' AS trade_type
    , CAST(get_json_object(s.consideration[0], '$.amount')+get_json_object(s.consideration[1], '$.amount') AS DECIMAL(38,0)) AS price_raw
    , get_json_object(s.consideration[0], '$.token') AS currency_contract
    , s.contract_address AS project_contract_address
    , s.evt_tx_hash AS tx_hash
    , CAST(0 AS DECIMAL(38,0)) AS platform_fee_amount_raw -- Hardcoded 0% platform fee
    , LEAST(CAST(get_json_object(s.consideration[0], '$.amount') AS DECIMAL(38,0)), CAST(get_json_object(s.consideration[1], '$.amount') AS DECIMAL(38,0))) AS royalty_fee_amount_raw
    , CASE WHEN get_json_object(s.consideration[0], '$.recipient')!=s.recipient THEN get_json_object(s.consideration[0], '$.recipient')
        ELSE get_json_object(s.consideration[1], '$.recipient')
        END AS royalty_fee_address
    , cast(NULL as varchar(1)) as platform_fee_address
    , s.evt_index as sub_tx_trade_id
FROM {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} s
WHERE s.zone='0x0000000000d80cfcb8dfcd8b2c4fd9c813482938'
    {% if is_incremental() %}
    AND s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND s.evt_block_time >= '{{seaport_usage_start_date}}'
    {% endif %}
)

select *
from regular_blur_sales
union all
select *
from seaport_blur_sales

