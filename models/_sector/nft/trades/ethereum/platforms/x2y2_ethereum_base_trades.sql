{{ config(
    schema = 'x2y2_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{%- set project_start_date = "TIMESTAMP '2022-02-04'" %}
{%- set fee_management_addr = "0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd" %}


WITH src_evt_inventory as (
    SELECT
     evt_block_time as block_time
    ,evt_block_number as block_number
    ,evt_tx_hash as tx_hash
    ,contract_address as project_contract_address
    ,case when intent = uint256 '1' then taker else maker end as buyer
    ,case when intent = uint256 '1' then maker else taker end as seller
    ,from_hex(substring(JSON_EXTRACT_SCALAR(inv.item, '$.data'), 155, 40)) as nft_contract_address
    ,bytearray_to_uint256(from_hex(substring(JSON_EXTRACT_SCALAR(inv.item, '$.data'), 195,64))) as nft_token_id
    ,UINT256 '1' AS nft_amount
    ,case when intent = uint256 '1' then 'Buy' else 'Offer Accepted' end as trade_category
    ,'secondary' as trade_type
    ,currency as currency_contract
    ,cast(JSON_EXTRACT_SCALAR(inv.item, '$.price') as UINT256) as price_raw
    ,from_hex(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.to')) as fees_0_to
    ,from_hex(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[1]'), '$.to')) as fees_1_to
    ,CASE WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.to') = '{{fee_management_addr}}'
     THEN cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.percentage') as DOUBLE)
     ELSE 0
     END as platform_fee_percentage
    ,CASE WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.to') = '{{fee_management_addr}}'
     THEN (COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[1]'), '$.percentage') as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[2]'), '$.percentage')as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[3]'), '$.percentage')as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[4]'), '$.percentage')as double), 0))
     ELSE ( COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.percentage')as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[1]'), '$.percentage')as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[2]'), '$.percentage')as double), 0)
        +COALESCE(cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[3]'), '$.percentage')as double), 0))
     END as royalty_fee_percentage
    ,{{fee_management_addr}} as platform_fee_address
    ,CASE WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.to')='{{fee_management_addr}}'
        THEN from_hex(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[1]'), '$.to'))
        ELSE from_hex(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(inv.detail, '$.fees[0]'), '$.to'))
        END AS royalty_fee_address
    ,evt_index as sub_tx_trade_id
    FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE evt_block_time >= {{project_start_date}}
    {% endif %}
)

-- results
SELECT
 'ethereum' as blockchain
, 'x2y2' as project
, 'ethereum' as project_version
, block_time
, block_number
, tx_hash
, project_contract_address
, buyer
, seller
, nft_contract_address
, nft_token_id
, nft_amount
, trade_type
, trade_category
, currency_contract
, price_raw
, CAST(COALESCE(price_raw*platform_fee_percentage/1e6, 0) as uint256) AS platform_fee_amount_raw
, CAST(COALESCE(price_raw*royalty_fee_percentage/1e6, 0) as uint256) AS royalty_fee_amount_raw
, platform_fee_address
, royalty_fee_address
, sub_tx_trade_id
FROM src_evt_inventory

