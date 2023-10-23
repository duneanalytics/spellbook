{{ config(
        tags = ['dunesql'],
        schema = 'cex_optimism',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='optimism'
        , transactions = source('optimism','transactions')
        , traces = source('optimism','traces')
        , erc20_transfers = source('erc20_optimism','evt_Transfer')
        , cex_addresses = ref('cex_optimism_addresses')
)}}
