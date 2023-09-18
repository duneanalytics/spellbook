{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_day', 'wallet_address', 'token_address', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    balances_erc1155_day(
        transfers_erc1155_rolling_day = ref('transfers_celo_erc1155_rolling_day'),
        init_date = '2020-04-22'
    )
}}
