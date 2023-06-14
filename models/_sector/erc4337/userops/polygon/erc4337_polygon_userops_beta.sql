{{ config(
    schema ='erc4337_polygon',
    alias = 'userops_beta',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('erc4337_v0_5_polygon_userops_basics')
    , ref('erc4337_v0_6_polygon_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'polygon',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        gas_symbol = 'ETH',
        deployed_date = '2023-02-15',
        transactions_model = source('polygon', 'transactions'),
        prices_model = source('prices','usd')
    )
}}