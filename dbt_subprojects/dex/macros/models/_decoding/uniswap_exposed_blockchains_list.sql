{% macro uniswap_exposed_blockchains_list() %}
   {{ return([
        "gnosis"
        , "optimism"
        , "zkevm"
        , "zksync"
        , "zora"
    ]) }}
{% endmacro %} 