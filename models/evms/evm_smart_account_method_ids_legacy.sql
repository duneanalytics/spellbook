
{{ config(
        schema = 'method_ids',
        tags = ['legacy','static'],
        alias = alias('evm_smart_account_method_ids', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum","polygon","gnosis","avalanche_c","fantom","goerli","bnb","base","celo"]\',
                                "sector",
                                "method_ids",
                                \'["msilb7"]\') }}'
        )
}}


SELECT 1