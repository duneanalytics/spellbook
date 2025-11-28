{%- set blockchain = 'sonic' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_hash', 'second_side', 'call_trace_address', 'call_trade_id'],
    )
-}}

-- it's splitted to 2 operations (sides) and fetching from pre-materialized tables to prevent doubling full-scan of tables used

select * from {{ ref('oneinch_' + blockchain + '_project_swaps_base') }}
union all
select * from {{ ref('oneinch_' + blockchain + '_project_swaps_second_side') }}