{% macro all_evm_chains() %}
   {{ return(array('ethereum', 'optimism', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'gnosis', 'fantom')) }}
{% endmacro %}
