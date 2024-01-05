{{
    config(
        alias ='eth',
        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["zora"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

{{transfers_eth(
    blockchain='zora'
    , eth_placeholder_contract = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
    , base_traces = source('zora','traces')
    , base_transactions = source('zora','transactions')
    , erc20_transfer = source('erc20_zora','evt_transfer')
)}}
