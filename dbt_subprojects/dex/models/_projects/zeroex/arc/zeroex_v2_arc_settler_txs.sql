{{
    config(
        schema = 'zeroex_v2_arc',
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

{% set zeroex_settler_start_date = '2026-09-07' %}
{% set blockchain = 'arc' %}

-- Materialize the trace scan once so downstream 0x Settler models do not rescan Arc traces.
-- The first observed Arc Settler registry event is on 7 September 2026.
  SELECT
          settler_txs.*
        , CAST(DATE_TRUNC('month', block_time) AS DATE) AS block_month
    FROM (
        {{
            zeroex_settler_txs_cte(
                blockchain = blockchain,
                start_date = zeroex_settler_start_date
            )
        }}
    ) settler_txs
