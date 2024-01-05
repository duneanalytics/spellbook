{{
    config(
        alias ='eth',
        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

{{transfers_eth(
    blockchain='base'
    , eth_placeholder_contract = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
    , base_traces = source('base','traces')
    , base_transactions = source('base','transactions')
    , erc20_transfer = source('erc20_base','evt_transfer')
)}}
