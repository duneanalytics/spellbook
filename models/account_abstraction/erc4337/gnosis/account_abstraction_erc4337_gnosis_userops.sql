{{ config(
    alias = 'userops',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke"]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('account_abstraction_erc4337_gnosis_v0_5_userops_basics')
    , ref('account_abstraction_erc4337_gnosis_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'gnosis',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0x44fa8e6f47987339850636f88629646662444217',
        gas_symbol = 'DAI',
        deployed_date = '2023-02-15',
        transactions_model = source('gnosis', 'transactions'),
        prices_model = source('prices','usd')
    )
}}