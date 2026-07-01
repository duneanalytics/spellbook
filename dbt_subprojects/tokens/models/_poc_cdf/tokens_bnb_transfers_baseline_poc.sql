{{ config(
    schema = 'tokens_bnb',
    alias = 'transfers_baseline_poc',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    merge_skip_unchanged = true,
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
) }}

-- POC baseline (CUR2-2963): the normal incremental-merge enrich over the SAME bounded
-- clone the CDF target reads, so the A/B (correctness + cost) is apples-to-apples.
-- Throwaway; delete the _poc_cdf folder when done.
{{
    transfers_enrich(
        base_transfers = ref('tokens_bnb_base_transfers_cdf_poc')
        , transfers_start_date = '2026-06-01'
        , blockchain = 'bnb'
    )
}}
