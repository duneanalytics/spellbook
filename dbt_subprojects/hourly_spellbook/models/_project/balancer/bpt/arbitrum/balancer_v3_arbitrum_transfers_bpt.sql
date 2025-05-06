{% set blockchain = 'arbitrum' %}

{{
    config(
       schema = 'balancer_v3_arbitrum',
        alias = 'transfers_bpt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

{{ 
    balancer_v3_compatible_transfers_bpt_macro(
        blockchain = blockchain,
        version = '3',
        project_decoded_as = 'balancer_v3'
    )
}}