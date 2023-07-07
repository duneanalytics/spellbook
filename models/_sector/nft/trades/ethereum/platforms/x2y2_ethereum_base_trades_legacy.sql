{{ config(
    schema = 'x2y2_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{%- set project_start_date = '2022-02-04' %}
{%- set fee_management_addr = '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd' %}


WITH src_evt_inventory as (
    SELECT
     date_trunc('day',evt_block_time) as block_date
    ,evt_block_time as block_time
    ,evt_block_number as block_number
    ,evt_tx_hash as tx_hash
    ,contract_address as project_contract_address
    ,case when intent = 1 then taker else maker end as buyer
    ,case when intent = 1 then maker else taker end as seller
    ,'0x' || substring(get_json_object(inv.item, '$.data'), 155, 40) as nft_contract_address
    ,bytea2numeric_v3(substring(get_json_object(inv.item, '$.data'), 195,64)) as nft_token_id
    ,CAST(1 AS DECIMAL(38,0)) AS nft_amount
    ,case when intent = 1 then 'Buy' else 'Offer Accepted' end as trade_category
    ,'secondary' as trade_type
    ,currency as currency_contract
    ,get_json_object(inv.item, '$.price') as price_raw
    ,get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to') as fees_0_to
    ,get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to') as fees_1_to
    ,CASE WHEN get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to') ='{{fee_management_addr}}'
     THEN get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage')
     ELSE 0
     END as platform_fee_percentage
    ,CASE WHEN get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to') ='{{fee_management_addr}}'
     THEN (COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[4]'), '$.percentage'), 0))
     ELSE ( COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[2]'), '$.percentage'), 0)
        +COALESCE(get_json_object(get_json_object(inv.detail, '$.fees[3]'), '$.percentage'), 0))
     END as royalty_fee_percentage
    ,'{{fee_management_addr}}'  as platform_fee_address
    ,CASE WHEN get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to')='{{fee_management_addr}}'
        THEN get_json_object(get_json_object(inv.detail, '$.fees[1]'), '$.to')
        ELSE get_json_object(get_json_object(inv.detail, '$.fees[0]'), '$.to')
        END AS royalty_fee_address
    ,evt_index as sub_tx_trade_id
    FROM {{ source('x2y2_ethereum','X2Y2_r1_evt_EvInventory') }} inv
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
)

-- results
SELECT
  block_date
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
, CAST(COALESCE(price_raw*platform_fee_percentage/1e6, 0) as decimal(38)) AS platform_fee_amount_raw
, CAST(COALESCE(price_raw*royalty_fee_percentage/1e6, 0) as decimal(38)) AS royalty_fee_amount_raw
, platform_fee_address
, royalty_fee_address
, sub_tx_trade_id
FROM src_evt_inventory

