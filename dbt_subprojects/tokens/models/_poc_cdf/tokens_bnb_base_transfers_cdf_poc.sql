{{ config(
    schema = 'tokens_bnb',
    alias = 'base_transfers_cdf_poc',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date', 'unique_key'],
    merge_skip_unchanged = true,
    change_data_feed_enabled = true,
) }}

-- POC clone of tokens_bnb_base_transfers for the Delta CDF strategy POC (CUR2-2963).
-- Reads the PROD base table directly (not ref(), which in dev resolves to an empty
-- personal-schema relation) and bounds to a recent window so a full-refresh (the CDF
-- bootstrap) is cheap. Prod spells live in the hive catalog (hive.<schema>.<alias>),
-- the same catalog the dev target writes to. This leaves the prod base spell untouched.
-- Throwaway: delete the _poc_cdf folder when the A/B is done. Both knobs are vars.
{% set poc_floor = "date '" ~ var('cdf_poc_floor', '2026-06-01') ~ "'" %}
{% set prod_base = var('cdf_poc_prod_base', 'hive.tokens_bnb.base_transfers') %}

select *
from {{ prod_base }}
where block_time >= {{ poc_floor }}
{% if is_incremental() -%}
and {{ incremental_predicate('block_time') }}
{%- endif %}
