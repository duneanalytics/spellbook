{{
    config(
        schema = 'oneinch',
        alias = 'lop_own_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'evt_index'],
    )
}}

{%- set stream = oneinch_lo_cfg_macro() -%}

-- cross-chain union of the per-chain LOP own-trades views; dex_<blockchain>_trades
-- consume the per-chain models directly (oneinch_lop_dex_trades_passthrough) so that
-- one chain's base-trades changes don't fan out to every chain's trades lineage

{% for blockchain in oneinch_blockchains_cfg_macro() if blockchain.exposed and stream.name in blockchain.exposed %}
select *
from {{ ref('oneinch_' + blockchain.name + '_lop_own_trades') }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
