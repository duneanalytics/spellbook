{{ config(
    schema = 'dew_polygon',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set project_start_date = "cast('2023-03-22' as timestamp)" %}

with trade_detail as (
    SELECT
          f.evt_block_time AS block_time
        , f.evt_block_number AS block_number
        , bytearray_to_uint256(bytearray_substring(f.data, 32 + 1, 32)) AS  nft_amount
        , bytearray_substring(bytearray_substring(f.data, 32 * 2 + 1, 32), 13, 20) AS nft_contract_address
        , bytearray_to_uint256(bytearray_substring(f.data, 32 * 3 + 1, 32)) AS  nft_token_id
        , f.intent
        , f.maker AS seller
        , f.taker AS buyer
        , CASE WHEN f.intent = CAST(1 AS uint256) THEN 'Buy'
            ELSE 'Sell'
            END AS trade_category
        , 'secondary' AS trade_type
        , f.price AS price_raw
        , CASE WHEN f.currency = 0x0000000000000000000000000000000000000000 then 0x0000000000000000000000000000000000001010
            ELSE f.currency END AS currency_contract
        , f.contract_address AS project_contract_address
        , f.evt_tx_hash AS tx_hash
        , uint256 '0' AS platform_fee_amount_raw
        , try(cast(json_extract_scalar(fees[1],'$.percentage') as double)) AS royalty_fee_percentage
        , try(from_hex(json_extract_scalar(fees[1],'$.to'))) AS royalty_fee_address
        , cast(NULL as varbinary) as platform_fee_address
        , f.evt_index as sub_tx_trade_id
    FROM {{ source('dew_polygon','dew_market_evt_Fulfilled') }} f
    {% if is_incremental() %}
    WHERE f.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE f.evt_block_time >= {{project_start_date}}
    {% endif %}
)

SELECT 'polygon' as blockchain
    , 'dew' as project
    , 'v1' as project_version
    , d.block_time
    , d.block_number
    , d.nft_contract_address
    , d.nft_token_id
    , d.nft_amount
    , d.seller
    , d.buyer
    , d.trade_category
    , d.trade_type
    , d.price_raw
    , d.currency_contract
    , d.project_contract_address
    , d.tx_hash
    , d.platform_fee_amount_raw
    , coalesce(cast(cast(d.price_raw as double) * d.royalty_fee_percentage / 1e6 as uint256), uint256 '0') AS royalty_fee_amount_raw
    , d.royalty_fee_address
    , d.platform_fee_address
    , d.sub_tx_trade_id
    , tx."from" as tx_from
    , tx."to" as tx_to
    , bytearray_reverse(bytearray_substring(bytearray_reverse(tx.data),1,32))  as tx_data_marker
FROM trade_detail d
INNER JOIN {{source('polygon', 'transactions')}} tx
    ON d.block_number = tx.block_number
    AND d.tx_hash = tx.hash
    {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
        AND tx.block_time >= {{project_start_date}}
    {% endif %}
