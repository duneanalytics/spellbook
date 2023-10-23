{{ config(
        tags = ['dunesql'],
        schema = 'cex_ethereum',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='ethereum'
        , transactions = source('ethereum','transactions')
        , traces = source('ethereum','traces')
        , erc20_transfers = source('erc20_ethereum','evt_Transfer')
        , cex_addresses = ref('cex_ethereum_addresses')
)}}
