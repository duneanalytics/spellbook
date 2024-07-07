

{{ config(
    alias = 'userops',
    schema = 'account_abstraction_erc4337_zora',

    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["zora"]\',
                                    "project",
                                    "erc4337",
                                    \'["yourname"]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('account_abstraction_erc4337_zora_v0_5_userops_basics')
    , ref('account_abstraction_erc4337_zora_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'zora',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0x...',
        gas_symbol = 'ZORA',
        deployed_date = deployed_date,
        transactions_model = source('zora', 'transactions'),
        prices_model = source('prices','usd')
    )
}}
