{{ config(
        tags = ['dunesql'],
        schema = 'cex_polygon',
        alias = alias('deposit_addresses'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'deposit_address']
        )
}}

{{cex_deposit_addresses(
        blockchain='polygon'
        , transactions = source('polygon','transactions')
        , traces = source('polygon','traces')
        , erc20_transfers = source('erc20_polygon','evt_Transfer')
        , cex_addresses = ref('cex_polygon_addresses')
)}}
