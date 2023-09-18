{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_latest'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

{{
    balances_erc1155_latest(
        balances_erc1155_hour = ref('balances_celo_erc1155_hour')
    )
}}
