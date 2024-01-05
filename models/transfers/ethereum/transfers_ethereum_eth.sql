{{ 
    config(
        alias = 'eth', 
        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

{{transfers_eth(
    blockchain='ethereum'
    , eth_placeholder_contract = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    , base_traces = source('ethereum','traces')
    , base_transactions = source('ethereum','transactions')
    , erc20_transfer = source('erc20_ethereum','evt_transfer')
)}}
