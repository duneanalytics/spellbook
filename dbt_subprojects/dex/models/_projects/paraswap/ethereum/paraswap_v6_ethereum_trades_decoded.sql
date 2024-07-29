{{ config(
    schema = 'paraswap_v6_ethereum',
    alias = 'trades_decoded',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.blockTime')],
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'txHash', 'method', 'callTraceAddress']
    )
}}

{{ paraswap_v6_trades_master('ethereum', 'paraswap') }}
