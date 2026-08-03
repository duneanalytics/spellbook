{%- set blockchain = oneinch_polygon_cfg_macro() -%}
{%- set stream = oneinch_lo_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'lop_venue_settled_fills',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'block_date', 'execution_id'],
    )
-}}

{{-
    oneinch_lop_venue_settled_fills_macro(
        blockchain = blockchain,
        stream = stream
    )
-}}
