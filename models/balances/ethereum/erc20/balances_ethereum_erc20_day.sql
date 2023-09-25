{{ config(
        schema = 'balances_ethereum',
        tags = ['dunesql'],
        alias = alias('erc20_day'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["soispoke","dot2dotseurat","Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_day(
        blockchain = 'ethereun',
        first_transaction_date = '2015-01-01',
        transfers_rolling_day = ref('transfers_ethereum_erc20_rolling_day'),
        balances_noncompliant = ref('balances_ethereum_erc20_noncompliant'),
        rebase_tokens = ref('tokens_ethereum_rebase')
    )
}}
