{{ config(
    schema = 'blur_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


{% set project_start_date = '2022-10-18' %}
{% set seaport_usage_start_date = '2023-01-25' %}


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

