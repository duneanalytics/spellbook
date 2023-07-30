{% macro all_evm_chains() %}
   {{ return(['ethereum', 'optimism', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'gnosis', 'fantom', 'base']) }}
{% endmacro %}