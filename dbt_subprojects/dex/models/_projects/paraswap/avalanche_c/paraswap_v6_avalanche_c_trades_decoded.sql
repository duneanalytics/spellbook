{{ config(
    schema = 'paraswap_v6_avalanche_c',
    alias = 'trades_decoded',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.blockTime')],
    unique_key = ['call_tx_hash', 'method']
    )
}}


--   - name: paraswap_v6_avalanche_c_trades_decoded
--     description: "Paraswap V6 trades decoded"
--     tests:
--       - dbt_utils.unique_combination_of_columns:
--           combination_of_columns:
--             - block_date
--             - blockchain
--             - project
--             - version
--             - call_tx_hash
--             - method
--             - call_trace_address  

{{ paraswap_v6_trades_master('avalanche_c', 'paraswap') }}