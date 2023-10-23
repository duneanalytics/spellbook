{{ config(
        tags = ['dunesql'],
        schema = 'cex_arbitrum',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='arbitrum'
        , transactions = source('arbitrum','transactions')
        , traces = source('arbitrum','traces')
        , erc20_transfers = source('erc20_arbitrum','evt_Transfer')
        , cex_addresses = ref('cex_arbitrum_addresses')
)}}
