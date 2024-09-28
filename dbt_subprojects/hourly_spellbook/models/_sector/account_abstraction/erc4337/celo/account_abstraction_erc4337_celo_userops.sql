{{
    config(
        schema = 'account_abstraction_erc4337_celo',
        alias = 'userops',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['userop_hash', 'tx_hash'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                        "project",
                                        "erc4337",
                                        \'["0xbitfly", "hosuke", "tomfutago"]\') }}'
    )
}}

{% set erc4337_celo_models = [
    ref('account_abstraction_erc4337_celo_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'celo',
        base_models = erc4337_celo_models,
        wrapped_gas_address = '0x471EcE3750Da237f93B8E339c536989b8978a438',
        gas_symbol = 'CELO',
        deployed_date = '2023-08-01',
        transactions_model = source('celo', 'transactions'),
        prices_model = source('prices', 'usd')
    )
}}
