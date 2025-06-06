{% macro oneinch_project_swaps_exposed_blockchains_list() %}
   {{ return([
        'ethereum', 
        'optimism', 
        'arbitrum', 
        'avalanche_c', 
        'polygon', 
        'bnb', 
        'gnosis', 
        'fantom', 
        'base',
        'zksync',
        'linea',
        'sonic',
        'unichain',
    ]) }}
{% endmacro %}