{%- set blockchain = oneinch_fantom_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'lop_own_trades',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
    )
-}}

{{-
    oneinch_lop_own_trades_macro(
        blockchain = blockchain
    )
-}}
