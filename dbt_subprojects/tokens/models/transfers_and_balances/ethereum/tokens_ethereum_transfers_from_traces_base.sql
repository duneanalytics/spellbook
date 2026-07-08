{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces_base',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [
            incremental_month_predicate('DBT_INTERNAL_DEST.block_month'),
            incremental_predicate('DBT_INTERNAL_DEST.block_time'),
        ],
        unique_key = ['block_date', 'unique_key'],
    )
}}

-- CI-only scan bound (target=ci); prod/full-refresh unaffected.
{% if target.name == 'ci' -%}
select * from (
{%- endif %}

{{ transfers_from_traces_base_macro(blockchain=blockchain) }}

{% if target.name == 'ci' -%}
) as _ci_bounded
where block_time >= now() - interval '3' day
{%- endif %}