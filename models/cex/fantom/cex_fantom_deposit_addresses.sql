{{ config(
        tags = ['dunesql'],
        schema = 'cex_fantom',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='fantom'
        , transactions = source('fantom','transactions')
        , traces = source('fantom','traces')
        , erc20_transfers = source('erc20_fantom','evt_Transfer')
        , cex_addresses = ref('cex_fantom_addresses')
)}}
