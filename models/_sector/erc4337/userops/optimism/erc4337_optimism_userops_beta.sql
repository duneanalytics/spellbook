{{ config(
    schema ='erc4337_optimism',
    alias = 'userops_beta',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('erc4337_v0_5_optimism_userops_basics')
    , ref('erc4337_v0_6_optimism_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'optimism',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0x4200000000000000000000000000000000000006',
        gas_symbol = 'ETH',
        deployed_date = '2023-02-15',
        transactions_model = source('optimism', 'transactions'),
        prices_model = source('prices','usd')
    )
}}