{{ config(
    schema = 'fantasy_blast',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

{% set project_start_date = '2024-05-01' %}

WITH trades AS (
    SELECT evt_block_time AS block_time
    , evt_block_date AS block_date
    , CAST(JSON_EXTRACT_SCALAR(sell, '$.tokenId') AS UINT256) AS nft_token_id
    , UINT256 '1' AS nft_amount
    , 'Buy' AS trade_category
    , from_hex(JSON_EXTRACT_SCALAR(sell, '$.trader')) AS seller
    , buyer
    , CAST(JSON_EXTRACT_SCALAR(sell, '$.price') AS UINT256) AS price_raw
    , from_hex(JSON_EXTRACT_SCALAR(sell, '$.paymentToken')) AS currency_contract
    , from_hex(JSON_EXTRACT_SCALAR(sell, '$.collection')) AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , evt_block_number AS block_number
    , evt_index AS sub_tx_trade_id
    , evt_tx_from AS tx_from
    , evt_tx_to AS tx_to
    FROM {{ source('fantasy_blast', 'Exchange_evt_Buy')}}
    {% if not is_incremental() %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_date AS block_date
    , CAST(JSON_EXTRACT_SCALAR(buyOrder, '$.tokenId') AS UINT256) AS nft_token_id
    , UINT256 '1' AS nft_amount
    , 'Sell' AS trade_category
    , seller
    , from_hex(JSON_EXTRACT_SCALAR(buyOrder, '$.trader')) AS buyer
    , CAST(JSON_EXTRACT_SCALAR(buyOrder, '$.price') AS UINT256) AS price_raw
    , from_hex(JSON_EXTRACT_SCALAR(buyOrder, '$.paymentToken')) AS currency_contract
    , from_hex(JSON_EXTRACT_SCALAR(buyOrder, '$.collection')) AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , evt_block_number AS block_number
    , evt_index AS sub_tx_trade_id
    , evt_tx_from AS tx_from
    , evt_tx_to AS tx_to
    FROM {{ source('fantasy_blast', 'Exchange_evt_Sell')}}
    {% if not is_incremental() %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    )

    , trades_final AS (
    SELECT 'blast' AS blockchain
    , 'fantasy' AS project
    , 'v1' AS project_version
    , block_time
    , block_date
    , date_trunc('month', block_time) AS block_month
    , nft_token_id
    , 'secondary' AS trade_type
    , nft_amount
    , trade_category
    , seller
    , buyer
    , price_raw
    , currency_contract
    , nft_contract_address
    , tx_hash
    , project_contract_address
    , block_number
    , sub_tx_trade_id
    --, tx_from
    --, tx_to
    , CAST(0.015*CAST(price_raw AS double) AS UINT256) AS platform_fee_amount_raw
    , CAST(0.015*CAST(price_raw AS double) AS UINT256) AS royalty_fee_amount_raw
    , CAST(NULL AS VARBINARY) AS royalty_fee_address
    , 0x8ab15fe88a00b03724ac91ee4ee1f998064f2e31 AS platform_fee_address
    FROM trades
    )

{{ add_nft_tx_data('trades_final', 'blast') }}