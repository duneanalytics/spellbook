{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        materialized = 'view',
        unique_key = ['block_month', 'id'],
    )
-}}

-- it's splitted to 2 operations (sides) and fetching from pre-materialized tables to prevent doubling full-scan of tables used

select * from {{ ref('oneinch_' + blockchain + '_project_swaps_base') }}
union all
select * from {{ ref('oneinch_' + blockchain + '_project_swaps_second_side') }}