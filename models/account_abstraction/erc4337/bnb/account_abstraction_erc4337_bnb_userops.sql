{{ config(
    alias = 'userops',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke"]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_bnb_models = [
    ref('account_abstraction_erc4337_bnb_v0_6_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'bnb',
        base_models = erc4337_bnb_models,
        wrapped_gas_address = '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
        gas_symbol = 'BNB',
        deployed_date = deployed_date,
        transactions_model = source('bnb', 'transactions'),
        prices_model = source('prices','usd')
    )
}}