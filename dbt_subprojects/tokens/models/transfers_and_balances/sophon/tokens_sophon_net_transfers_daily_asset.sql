{% set blockchain = 'sophon' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily_asset'
        , materialized = 'table'
        , tags = ['static']
        , file_format = 'delta'
        )
}}


{{ evm_net_transfers_daily_asset(
        blockchain=blockchain,
        native_contract_address=var('ETH_ERC20_ADDRESS')
) 
}}