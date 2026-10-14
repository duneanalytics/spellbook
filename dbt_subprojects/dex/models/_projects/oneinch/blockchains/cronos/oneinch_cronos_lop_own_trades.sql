{%- set blockchain = oneinch_cronos_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'lop_own_trades',
        materialized = 'view',
    )
-}}

{{-
    oneinch_lop_own_trades_macro(
        blockchain = blockchain
    )
-}}
