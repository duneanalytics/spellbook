{% macro uniswap_established_blockchains_list() %}
   {{ return([
       "arbitrum"
       , "avalanche_c"
       , "base"
       , "bnb"
       , "ethereum"
       , "optimism"
       , "polygon"
    ]) }}
{% endmacro %} 

{% macro uniswap_new_blockchains_list() %}
   {{ return([
       "blast"
       , "scroll"
       , "zksync"
       , "zora"
    ]) }}
{% endmacro %} 
