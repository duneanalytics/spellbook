{{
    config(
        tags=['dunesql'],
        schema = 'chainlink_optimism',
        alias = alias('ccip_onramps'),
        materialized = 'incremental',
        file_format = 'delta',
        unique_key = ['evt_index', 'evt_tx_hash', 'message'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["synthquest"]\') }}'
    )
}}

 {% set op_router = '0x261c05167db67b2b619f9d312e0753f3721ad6e8' %}
 {% set test_date = "2022-06-01" %}

select 
'Optimism Mainnet' as origin
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
, json_extract(message, '$.tokenAmounts') as tokenAmounts
, cast(json_extract_scalar(message, '$.feeToken') as VARCHAR) as feeToken
, cast(json_extract_scalar(message, '$.messageId') as VARCHAR) as messageId
, '{{op_router}}' as router
, contract_address
, evt_tx_hash
, evt_block_time
, evt_index
, evt_block_number

FROM {{ source('chainlink_optimism', 'EVM2EVMOnRampETH_evt_CCIPSendRequested') }}
