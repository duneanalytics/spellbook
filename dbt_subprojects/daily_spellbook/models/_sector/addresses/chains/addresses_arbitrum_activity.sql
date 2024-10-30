{% set blockchain = 'arbitrum' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'activity',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
    )
}}

{{
    addresses_info(
        blockchain = blockchain
    )
}}
