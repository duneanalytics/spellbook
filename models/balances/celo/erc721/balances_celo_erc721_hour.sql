{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_hour'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    balances_erc721_hour(
        transfers_erc721_rolling_hour = ref('transfers_celo_erc721_rolling_hour'),
        balances_erc721_noncompliant = ref('balances_celo_erc721_noncompliant'),
        init_date = '2020-04-22'
    )
}}
