{{ config(
    schema = 'tokens_bnb',
    alias = 'transfers_cdf_poc',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delta_cdf',
    unique_key = ['block_date', 'unique_key'],
    on_schema_change = 'ignore',
    post_hook = "{% if not is_incremental() %}{{ cdf_advance_watermark(this, cdf_current_source_version(ref('tokens_bnb_base_transfers_cdf_poc'))) }}{% else %}select 1{% endif %}",
) }}

-- POC CDF target (CUR2-2963): transfers_enrich_cdf over the Delta change feed of the
-- bounded clone, applied via the delta_cdf strategy. Compare against
-- tokens_bnb_transfers_baseline_poc. The post_hook stamps the bootstrap watermark on
-- first build / --full-refresh (gated on `not is_incremental()`, NOT should_full_refresh,
-- so the very first creation also stamps); the incremental path stamps inside the
-- strategy macro. The `select 1` else-arm avoids dbt-trino erroring on an empty hook.
-- Throwaway; delete the _poc_cdf folder when done.
{{
    transfers_enrich_cdf(
        base_relation = ref('tokens_bnb_base_transfers_cdf_poc')
        , blockchain = 'bnb'
    )
}}
