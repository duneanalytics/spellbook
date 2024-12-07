{{
    config(
        schema = 'bungee',
        alias = 'bridges',
        materialized = 'view'
    )
}}

{% set chains = [
    'ethereum', 'zkevm', 'scroll', 'blast', 'linea', 'mantle', 'optimism',
    'gnosis', 'arbitrum', 'zksync', 'base', 'bnb', 'polygon',
    'avalanche_c', 'fantom'
] %}

{% for chain in chains %}
    {{ bungee_SocketBridge(chain) }}
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}
