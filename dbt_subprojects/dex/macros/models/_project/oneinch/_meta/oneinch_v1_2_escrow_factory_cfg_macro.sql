{# The v1.2 factory changes LO mode detection on the canonical chains. Keep this overlay
   on LO and blockchains metadata, so unrelated AR raw calls retain their existing build. #}
{% macro oneinch_v1_2_escrow_factory_cfg_macro(blockchain) %}
    {% set canonical_chains = ['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'optimism', 'base', 'linea', 'sonic', 'unichain'] %}
    {% if blockchain.name in canonical_chains %}
        {% set factory = '0x03a25b3215a0e5c15cf23ac4d2e5cf86c0ff7efa' %}
    {% elif blockchain.name == 'zksync' %}
        {% set factory = '0xd9085ac07da21bd6eb003a530a524ab054ca8652' %}
    {% else %}
        {{ return(blockchain) }}
    {% endif %}
    {{ return(dict(blockchain, escrow_factory_addresses=blockchain.escrow_factory_addresses + [factory])) }}
{% endmacro %}
