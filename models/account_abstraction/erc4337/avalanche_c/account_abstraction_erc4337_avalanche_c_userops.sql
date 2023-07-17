{{ config(
    alias = alias('userops'),
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke"]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('account_abstraction_erc4337_avalanche_c_v0_5_userops_basics')
    , ref('account_abstraction_erc4337_avalanche_c_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'avalanche_c',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        gas_symbol = 'AVAX',
        deployed_date = '2023-02-15',
        transactions_model = source('avalanche_c', 'transactions'),
        prices_model = source('prices','usd')
    )
}}