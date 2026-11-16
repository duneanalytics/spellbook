{%- set blockchain = oneinch_fantom_cfg_macro() -%}
{%- set stream = oneinch_lo_executions_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = stream.name + '_executions',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
-}}

{{-
    oneinch_lo_executions_macro(
        blockchain = blockchain,
        stream = stream
    )
-}}