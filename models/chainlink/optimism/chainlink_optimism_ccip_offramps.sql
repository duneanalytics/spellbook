{{
    config(
        schema = 'chainlink_optimism',
        alias = alias('ccip_offramps'),
        materialized = 'incremental',
        file_format = 'delta',
        unique_key = ['evt_index', 'evt_tx_hash', 'messageId'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["synthquest"]\') }}'
    )
}}

 {% set eth_router = '0xe561d5e02207fb5eb32cca20a699e0d8919a1476' %}
 {% set test_date = "2022-06-01" %}


select 
'Ethereum Mainnet' as origin
, 'Optimism Mainnet' as destination
, contract_address
, evt_tx_hash
, evt_index
, evt_block_time
, evt_index
, evt_block_number
, messageId
, returnData
, sequenceNumber
, state

FROM {{ source('chainlink_optimism', 'EVM2EVMOffRampETH_evt_ExecutionStateChanged') }}