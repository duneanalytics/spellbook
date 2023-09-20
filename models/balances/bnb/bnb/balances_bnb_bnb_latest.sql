{{ config(
        tags = ['dunesql'],
        schema = 'balances_bnb_bnb',
        alias = alias('bnb_latest'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_latest(
        blockchain = 'bnb',
        transfers_rolling_hour = ref('transfers_bnb_bnb_rolling_hour')
    )
}}
