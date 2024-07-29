{{ config(
    schema = 'paraswap_v6_base',
    alias = 'trades_decoded',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'method', 'call_trace_address']
    )
}}

{{ paraswap_v6_trades_master('base', 'paraswap') }}
