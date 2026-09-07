{{
    config(
        schema = 'zeroex_v2_robinhood',
        alias = 'settler_txs',
        materialized = 'incremental',
        partition_by = ['block_month'],
        unique_key = ['block_month', 'tx_hash', 'rn'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_settler_start_date = '2026-04-30' %}
{% set blockchain = 'robinhood' %}

-- Materialize the trace scan once so downstream 0x Settler models do not rescan Robinhood traces.
select
    settler_txs.*
    , cast(date_trunc('month', block_time) as date) as block_month
from (
    {{
        zeroex_settler_txs_cte(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
        )
    }}
) as settler_txs
