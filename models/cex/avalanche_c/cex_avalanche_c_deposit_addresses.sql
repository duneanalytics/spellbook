{{ config(
        tags = ['dunesql'],
        schema = 'cex_avalanche_c',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='avalanche_c'
        , transactions = source('avalanche_c','transactions')
        , traces = source('avalanche_c','traces')
        , erc20_transfers = source('erc20_avalanche_c','evt_Transfer')
        , cex_addresses = ref('cex_avalanche_c_addresses')
)}}
