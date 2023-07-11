{{  config(
        alias='eth_flow_orders',
        tags = ['dunesql'],
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
    )
}}

-- PoC Query: https://dune.com/queries/2715069
with
eth_flow_orders as (
    select
        sender,
        cast(date_trunc('day', evt_block_time) as date) as block_date,
        case
          when event.contract_address = 0x40a50cf069e992aa4536211b23f286ef88752187 then 'prod'
          when event.contract_address = 0xd02de8da0b71e1b59489794f423fabba2adc4d93 then 'barn'
        end as environment,
        evt_block_time as block_time,
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        date_format(
            from_unixtime(bytearray_to_decimal(from_hex(substring(cast(data as varchar), 19, 8)))),
           '%Y-%m-%d %T'
        ) AS valid_to,
      substring(cast(data as varchar), 3, 16) as quote_id_hex,
      bytearray_to_decimal(from_hex(substring(cast(data as varchar), 3, 16))) as quote_id,
      JSON_EXTRACT_SCALAR(event."order", '$.sellAmount') as sell_amount,
      JSON_EXTRACT_SCALAR(event."order", '$.feeAmount') as fee_amount,
      JSON_EXTRACT_SCALAR(event."order", '$.buyAmount') as buy_amount,
      JSON_EXTRACT_SCALAR(event."order", '$.buyToken') as buy_token,
      JSON_EXTRACT_SCALAR(event."order", '$.receiver') as receiver,
      JSON_EXTRACT_SCALAR(event."order", '$.appData') as app_hash,
      -- OrderHash returned by createOrder with excluded fix values (owner = contract_address, validTo = max u32)
      -- https://github.com/cowprotocol/ethflowcontract/blob/9c74c8ba36ff9ff3e255172b02454f831c066865/src/CoWSwapEthFlow.sol#L81-L84
      concat(
        cast(output_orderHash as varchar),
        substring(cast(event.contract_address as varchar), 3, 40),
        'ffffffff'
      ) as order_uid
  from {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_evt_OrderPlacement') }} event
  inner join {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_call_createOrder') }} call
        on call_block_number = evt_block_number
        and call_tx_hash = evt_tx_hash
        and call_success = true
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

select * from eth_flow_orders
