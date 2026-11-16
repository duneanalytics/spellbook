{%- set blockchain = oneinch_fantom_cfg_macro() -%}
{%- set stream = oneinch_lo_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'lop_venue_settled_fills',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
-}}

{{-
    oneinch_lop_venue_settled_fills_macro(
        blockchain = blockchain,
        stream = stream
    )
-}}
