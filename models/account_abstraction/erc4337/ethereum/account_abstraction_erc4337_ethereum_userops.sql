{{ config(
    alias = 'userops',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke"]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('account_abstraction_erc4337_ethereum_v0_5_userops_basics')
    , ref('account_abstraction_erc4337_ethereum_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'ethereum',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        gas_symbol = 'ETH',
        deployed_date = '2023-02-15',
        transactions_model = source('ethereum', 'transactions'),
        prices_model = source('prices','usd')
    )
}}