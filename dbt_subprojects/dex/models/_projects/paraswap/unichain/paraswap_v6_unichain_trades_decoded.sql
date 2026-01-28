{{ config(
    schema = 'paraswap_v6_unichain',
    alias = 'trades_decoded',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.blockTime')],
    unique_key = ['call_tx_hash', 'method', 'call_trace_address']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ paraswap_v6_trades_master('unichain', 'paraswap') }}
