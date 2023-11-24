{{ config(
    schema = 'blur_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


{% set project_start_date = "2022-10-18" %}


SELECT
      'ethereum' as blockchain
    , 'blur' as project
    , 'v1' as project_version
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
    , uint256 '0' AS platform_fee_amount_raw  -- Hardcoded 0% platform fee
    , CAST(COALESCE(cast(JSON_EXTRACT_SCALAR(bm.buy, '$.price') as uint256)*cast(JSON_EXTRACT_SCALAR(cast(JSON_EXTRACT(bm.sell, '$.fees[0]') as varchar), '$.rate') as uint256)/10000, uint256 '0') AS uint256) AS royalty_fee_amount_raw
    , from_hex(JSON_EXTRACT_SCALAR(cast(json_extract(bm.sell, '$.fees[0]') as varchar), '$.recipient')) AS royalty_fee_address
    , cast(NULL as varbinary) as platform_fee_address
    , bm.evt_index as sub_tx_trade_id
FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
{% if is_incremental() %}
WHERE {{incremental_predicate('bm.evt_block_time')}}
{% else %}
WHERE bm.evt_block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
