{{ config(
    schema = 'blur_ethereum',
    tags = ['dunesql'],
    alias = alias('base_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


{% set project_start_date = "cast('2022-10-18' as timestamp)" %}
{% set seaport_usage_start_date = "cast('2023-01-25' as timestamp)" %}


WITH
regular_blur_sales as (
    SELECT
          cast(date_trunc('month', bm.evt_block_time) as date) AS block_date
        , bm.evt_block_time AS block_time
        , bm.evt_block_number AS block_number
        , from_hex(JSON_EXTRACT_SCALAR(bm.buy, '$.collection')) AS nft_contract_address
        , cast(JSON_EXTRACT_SCALAR(bm.sell, '$.tokenId') as uint256) AS nft_token_id
        , CAST(JSON_EXTRACT_SCALAR(bm.buy, '$.amount') AS uint256) AS nft_amount
        , from_hex(JSON_EXTRACT_SCALAR(bm.sell, '$.trader')) AS seller
        , from_hex(JSON_EXTRACT_SCALAR(bm.buy, '$.trader')) AS buyer
        , CASE WHEN from_hex(JSON_EXTRACT_SCALAR(bm.buy, '$.matchingPolicy')) IN (0x00000000006411739da1c40b106f8511de5d1fac, 0x0000000000dab4a563819e8fd93dba3b25bc3495) THEN 'Buy'
            WHEN from_hex(JSON_EXTRACT_SCALAR(bm.buy, '$.matchingPolicy')) IN (0x0000000000b92d5d043faf7cecf7e2ee6aaed232) THEN 'Sell'
            ELSE CAST(null as VARCHAR)
            END AS trade_category
        , 'secondary' AS trade_type
        , CAST(JSON_EXTRACT_SCALAR(bm.buy, '$.price') AS uint256) AS price_raw
        , from_hex(JSON_EXTRACT_SCALAR(bm.buy, '$.paymentToken')) AS currency_contract
        , bm.contract_address AS project_contract_address
        , bm.evt_tx_hash AS tx_hash
        , cast(0 as uint256) AS platform_fee_amount_raw  -- Hardcoded 0% platform fee
        , CAST(COALESCE(cast(JSON_EXTRACT_SCALAR(bm.buy, '$.price') as uint256)*cast(JSON_EXTRACT_SCALAR(cast(JSON_EXTRACT(bm.sell, '$.fees[0]') as varchar), '$.rate') as uint256)/10000, cast(0 as uint256)) AS uint256) AS royalty_fee_amount_raw
        , from_hex(JSON_EXTRACT_SCALAR(cast(json_extract(bm.sell, '$.fees[0]') as varchar), '$.recipient')) AS royalty_fee_address
        , cast(NULL as varbinary) as platform_fee_address
        , bm.evt_index as sub_tx_trade_id
    FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
    {% if is_incremental() %}
    WHERE bm.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE bm.evt_block_time >= {{project_start_date}}
    {% endif %}
)

, seaport_blur_sales as (
SELECT
      cast(date_trunc('month', s.evt_block_time) as date) AS block_date
    , s.evt_block_time AS block_time
    , s.evt_block_number AS block_number
    , from_hex(JSON_EXTRACT_SCALAR(s.offer[1], '$.token')) AS nft_contract_address
    , cast(JSON_EXTRACT_SCALAR(s.offer[1], '$.identifier') as uint256) AS nft_token_id
    , CAST(JSON_EXTRACT_SCALAR(s.offer[1], '$.amount') AS uint256) AS nft_amount
    , s.offerer AS seller
    , s.recipient AS buyer
    , 'Buy' AS trade_category
    , 'secondary' AS trade_type
    , CAST(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.amount') as uint256)+cast(JSON_EXTRACT_SCALAR(element_at(s.consideration,2), '$.amount') AS uint256) AS price_raw
    , from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.token')) AS currency_contract
    , s.contract_address AS project_contract_address
    , s.evt_tx_hash AS tx_hash
    , CAST(0 AS uint256) AS platform_fee_amount_raw -- Hardcoded 0% platform fee
    , LEAST(CAST(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.amount') AS uint256), CAST(JSON_EXTRACT_SCALAR(element_at(s.consideration,2), '$.amount') AS uint256)) AS royalty_fee_amount_raw
    , CASE WHEN from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.recipient'))!=s.recipient THEN from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.recipient'))
        ELSE from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,2), '$.recipient'))
        END AS royalty_fee_address
    , cast(NULL as varbinary) as platform_fee_address
    , s.evt_index as sub_tx_trade_id
FROM {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} s
WHERE s.zone=0x0000000000d80cfcb8dfcd8b2c4fd9c813482938
    {% if is_incremental() %}
    AND s.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND s.evt_block_time >= {{seaport_usage_start_date}}
    {% endif %}
)

select *
from regular_blur_sales
union all
select *
from seaport_blur_sales

