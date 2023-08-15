{{
    config(
        tags=['dunesql'],
        schema = 'chainlink_polygon',
        alias = alias('ccip_onramps'),
        materialized = 'incremental',
        file_format = 'delta',
        unique_key = ['evt_index', 'evt_tx_hash', 'message'],
        post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["synthquest"]\') }}'
    )
}}

 {% set polygon_router = '0x3c3d92629a02a8d95d5cb9650fe49c3544f69b43' %}
 {% set test_date = "2022-06-01" %}

select 
'Polygon Mainnet' as origin
, 'Ethereum Mainnet' as destination
, cast(json_extract_scalar(message, '$.sourceChainSelector') as INT256) as sourceChainSelector
, cast(json_extract_scalar(message, '$.sequenceNumber') as INT256)  as sequenceNumber
, cast(json_extract_scalar(message, '$.feeTokenAmount') as INT256) as feeTokenAmount
, cast(json_extract_scalar(message, '$.sender') as VARCHAR) as sender
, cast(json_extract_scalar(message, '$.nonce') as INT256) as nonce
, cast(json_extract_scalar(message, '$.gasLimit') as INT256) as gasLimit
, cast(json_extract_scalar(message, '$.strict') as BOOLEAN) as strict
, cast(json_extract_scalar(message, '$.receiver') as VARCHAR) as receiver
, cast(json_extract_scalar(message, '$.data') as varchar) as data
-- model cannot handle JSON, convert to array for each unique tx
, array_agg(json_format(json_extract(message, '$.tokenAmounts'))) over (partition by cast(json_extract_scalar(message, '$.messageId') as VARCHAR), evt_tx_hash) as tokenAmounts
, cast(json_extract_scalar(message, '$.feeToken') as VARCHAR) as feeToken
, cast(json_extract_scalar(message, '$.messageId') as VARCHAR) as messageId
, '{{polygon_router}}' as router
, contract_address
, evt_tx_hash
, evt_block_time
, evt_index
, evt_block_number

FROM {{ source('chainlink_polygon', 'EVM2EVMOnRampETH_evt_CCIPSendRequested') }}
