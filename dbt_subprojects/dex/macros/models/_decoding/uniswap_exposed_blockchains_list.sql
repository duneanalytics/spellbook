{% macro uniswap_old_blockchains_list() %}
   {{ return([
       "arbitrum"
       , "avalanche_c"
       , "base"
       , "bnb"
       , "ethereum"
       , "gnosis"
       , "polygon"
    ]) }}
{% endmacro %} 


{% macro uniswap_new_blockchains_list() %}
   {{ return([
        "blast"
       , "bob"
       , "celo"
       , "degen"
       , "fantom"
       , "kaia"
       , "linea"
       , "mantle"
       , "mode"
       , "nova"
       , "ronin"
       , "scroll"
       , "sei"
       , "worldchain"
       , "zkevm"
       , "zksync"
       , "zora"
    ]) }}
{% endmacro %} 


-- "optimism" has some problems with traces, need to fix later