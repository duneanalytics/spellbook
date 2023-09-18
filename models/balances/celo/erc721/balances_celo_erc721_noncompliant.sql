{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_noncompliant'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats", "0xBoxer", "tomfutago"]\') }}'
    )
}}

{{
    balances_erc721_noncompliant(
        transfers_erc721_rolling_day = ref('transfers_celo_erc721_rolling_day')
    )
}}
