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
        "berachain"
       , "blast"
       , "bob"
       , "boba"
       , "celo"
       , "degen"
       , "fantom"
       , "flare"
       , "kaia"
       , "linea"
       , "mantle"
       , "mode"
       , "nova"
       , "ronin"
       , "scroll"
       , "sei"
       , "sonic"
       , "sophon"
       , "unichain"
       , "viction"
       , "worldchain"
       , "zkevm"
       , "zksync"
       , "zora"
    ]) }}
{% endmacro %} 


-- "optimism" has some problems with traces, need to fix later