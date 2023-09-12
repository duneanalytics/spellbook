{{ config(
        tags = ['dunesql'],
        schema = 'dex_base',
        alias = alias('sandwiches'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash', 'index'],
        pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
)
}}

{{dex_sandwiches(
        blockchain='base'
        , transactions = source('base','transactions')
)}}