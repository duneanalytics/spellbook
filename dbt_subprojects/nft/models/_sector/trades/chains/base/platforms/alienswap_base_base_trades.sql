{{ config(
    schema = 'alienswap_base',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set alienswap_usage_start_date = "2023-08-13" %}

WITH base_trades as (
SELECT
      'base' as blockchain
    , 'alienswap' as project
    , '1' as project_version
    , s.evt_block_time AS block_time
    , cast(date_trunc('day', s.evt_block_time) as date) as block_date
    , cast(date_trunc('month', s.evt_block_time) as date) as block_month
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
    , uint256 '0' AS platform_fee_amount_raw -- Hardcoded 0% platform fee
    , LEAST(CAST(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.amount') AS uint256), CAST(JSON_EXTRACT_SCALAR(element_at(s.consideration,2), '$.amount') AS uint256)) AS royalty_fee_amount_raw
    , CASE WHEN from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.recipient'))!=s.recipient THEN from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,1), '$.recipient'))
        ELSE from_hex(JSON_EXTRACT_SCALAR(element_at(s.consideration,2), '$.recipient'))
        END AS royalty_fee_address
    , cast(NULL as varbinary) as platform_fee_address
    , s.evt_index as sub_tx_trade_id
FROM {{ source('alienswap_base','Alienswap_evt_OrderFulfilled') }} s
    {% if is_incremental() %}
    WHERE {{incremental_predicate('s.evt_block_time')}}
    {% else %}
    WHERE s.evt_block_time >= timestamp '{{alienswap_usage_start_date}}'
    {% endif %}
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'base') }}
