{{ config(
        tags = ['dunesql'],
        schema = 'dex_ethereum',
        alias = alias('sandwiches'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['sandwiched_pool', 'frontrun_tx_hash', 'frontrun_taker', 'frontrun_index', 'currency_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{{dex_sandwiches(
        blockchain='ethereum'
        , transactions = source('ethereum','transactions')
)}}