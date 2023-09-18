{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_latest'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    balances_erc721_latest(
        balances_erc721_hour = ref('balances_celo_erc721_hour')
    )
}}
