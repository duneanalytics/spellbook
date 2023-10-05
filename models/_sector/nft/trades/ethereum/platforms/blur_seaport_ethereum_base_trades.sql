{{ config(
    schema = 'blur_seaport_ethereum',
    tags = ['dunesql'],
    alias = alias('base_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set seaport_usage_start_date = "cast('2023-01-25' as timestamp)" %}

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