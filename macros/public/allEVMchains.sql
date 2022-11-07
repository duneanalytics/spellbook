{% macro allEVMchains() %}
   {{ return(array('ethereum', 'optimism', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'gnosis')) }}
{% endmacro %}
