{{
    config(
        alias ='eth',
        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}
{{transfers_eth(
    blockchain='optimism'
    , eth_placeholder_contract = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
    , base_traces = source('optimism','traces')
    , base_transactions = source('optimism','transactions')
    , erc20_transfer = source('erc20_optimism','evt_transfer')
)}}
