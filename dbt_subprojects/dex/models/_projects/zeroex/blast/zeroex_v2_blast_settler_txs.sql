{{
    config(
        schema = 'zeroex_v2_blast',
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

{% set zeroex_settler_start_date = '2024-07-15' %}
{% set blockchain = 'blast' %}

-- Shared staging model for 0x Settler transactions on Blast.
-- Materializing the settler-trace scan once breaks the CTE inlining that previously
-- re-scanned blast.traces ~14x per zeroex_v2_blast_trades build (the calldata-search
-- filter on input is non-pushable, so each inlined copy read the full traces window).
select
    settler_txs.*,
    cast(date_trunc('month', block_time) as date) as block_month
from (
    {{
        zeroex_settler_txs_cte(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
        )
    }}
) as settler_txs
