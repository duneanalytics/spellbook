{{ config(
    schema = 'opensea_v2_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash', 'sub_tx_trade_id']
    )
}}

{% set nft_start_date='2021-06-28' %}

WITH trades AS (
   select
      'Buy' AS trade_category,
      call_block_time AS block_time,
      call_block_number AS block_number,
      call_tx_hash AS tx_hash,
      a.contract_address,
      'secondary' AS trade_type,
      from_hex(json_extract_scalar(a.leftOrder,'$.makerAddress')) AS buyer,
      from_hex(json_extract_scalar(a.rightOrder,'$.makerAddress')) AS seller,
      from_hex('0x' || substring(json_extract_scalar(a.rightOrder,'$.makerAssetData'), 35, 40)) AS nft_contract_address,
      case when length(json_extract_scalar(a.rightOrder,'$.makerAssetData')) = 650 then bytearray_to_uint256(from_hex(substr(json_extract_scalar(a.rightOrder,'$.makerAssetData'),331,64)))
            else bytearray_to_uint256(from_hex(substr(json_extract_scalar(a.rightOrder,'$.makerAssetData'),75,64)))
      end AS token_id,
      least(cast(json_extract_scalar(output_matchedFillResults,'$.left.takerFeePaid') as uint256), cast(json_extract_scalar(output_matchedFillResults,'$.right.makerFeePaid') as uint256)) AS number_of_items,
      paymentTokenAddress AS currency_contract,
      least(
        coalesce(cast(json_extract_scalar(json_extract_scalar(output_matchedFillResults,'$.left'),'$.makerFeePaid') as uint256), cast(json_extract_scalar(json_extract_scalar(output_matchedFillResults,'$.right'),'$.takerFeePaid') as uint256))
        ,coalesce(cast(json_extract_scalar(json_extract_scalar(output_matchedFillResults,'$.right'),'$.takerFeePaid') as uint256), cast(json_extract_scalar(json_extract_scalar(output_matchedFillResults,'$.left'),'$.makerFeePaid') as uint256))
        ) AS amount_raw,
      2.5 AS platform_fee,
      from_hex(json_extract_scalar(element_at(feeData,1),'$.recipient')) AS fee_recipient,
      case when length(json_extract_scalar(element_at(feeData,2),'$.recipient')) > 0 then 2.5 else 0 end AS royalty_fee,
      a.call_trace_address
   from {{ source('opensea_polygon_v2_polygon','ZeroExFeeWrapper_call_matchOrders') }} a
   where 1=1
     and call_success
    {% if not is_incremental() %}
    AND a.call_block_time >= TIMESTAMP '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND a.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

trade_amount_detail as (
    SELECT e.evt_block_number,
        e.evt_tx_hash,
        e.value AS amount_raw,
        e.to AS receive_address,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.block_number
        AND e."from" = 0xf715beb51ec8f63317d66f491e37e7bb048fcc2d
        AND e.evt_tx_hash = t.tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= TIMESTAMP '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE e."from" = 0xf715beb51ec8f63317d66f491e37e7bb048fcc2d -- All fees are transferred to this address then split to other addresses
),

trade_amount_grouped as (
    SELECT evt_block_number,
        evt_tx_hash,
        sum(case when item_index = 1 then amount_raw else uint256 '0' end) AS fee_amount_raw_1,
        max(case when item_index = 1 then receive_address else null end) AS receive_address,
        sum(case when item_index = 2 then amount_raw else uint256 '0' end) AS fee_amount_raw_2,
        count(*) as row_count
    FROM trade_amount_detail
    GROUP BY 1, 2
),

trade_amount_summary as (
    SELECT evt_block_number,
        evt_tx_hash,
        -- Some tx has no royalty fee: https://polygonscan.com/tx/0x7a583aa2ac9aa7b25fdf969ddc7e3a860f4565e4e48e83c2d5d513355dd952a5
        (case when row_count = 2 then fee_amount_raw_2 else fee_amount_raw_1 end) AS platform_fee_amount_raw,
        (case when row_count = 2 then receive_address else null end) AS royalty_fee_receive_address,
        (case when row_count = 2 then fee_amount_raw_1 else cast(0 as uint256) end) AS royalty_fee_amount_raw
    FROM trade_amount_grouped
)

, base_trades as (
SELECT
  'polygon' AS blockchain,
  'opensea' AS project,
  'v2' AS project_version,
  a.tx_hash,
  a.block_time,
  cast(date_trunc('day', a.block_time) as date) as block_date,
  cast(date_trunc('month', a.block_time) as date) as block_month,
  a.block_number,
  a.amount_raw AS price_raw,
  a.currency_contract,
  a.token_id as nft_token_id,
  a.contract_address AS project_contract_address,
  a.trade_type,
  a.trade_category,
  coalesce(a.number_of_items, uint256 '1') AS nft_amount,
  a.buyer,
  a.seller,
  a.nft_contract_address,
  f.platform_fee_amount_raw AS platform_fee_amount_raw,
  f.royalty_fee_amount_raw AS royalty_fee_amount_raw,
  f.royalty_fee_receive_address as royalty_fee_address,
  0xf715beb51ec8f63317d66f491e37e7bb048fcc2d as platform_fee_address,
  row_number() over (partition by a.tx_hash order by a.call_trace_address) AS sub_tx_trade_id
FROM trades a
LEFT JOIN trade_amount_summary f ON a.block_number = f.evt_block_number AND a.tx_hash = f.evt_tx_hash
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'polygon') }}

