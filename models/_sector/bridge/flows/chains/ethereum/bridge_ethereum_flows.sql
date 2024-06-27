{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'flows',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT *
FROM {{ref('bridge_ethereum_base_raw_flows')}}