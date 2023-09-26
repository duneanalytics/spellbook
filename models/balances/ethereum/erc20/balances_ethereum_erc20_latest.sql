{{ config(
        tags=['dunesql'],
        alias = alias('erc20_latest'),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke","dot2dotseurat"]\') }}'
        )
}}

{{
    balances_fungible_latest(
        blockchain = 'ethereum',
        transfers_rolling_hour = ref('transfers_ethereum_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_ethereum_erc20_noncompliant'),
        rebase_tokens = ref('tokens_ethereum_rebase')
    )
}}
