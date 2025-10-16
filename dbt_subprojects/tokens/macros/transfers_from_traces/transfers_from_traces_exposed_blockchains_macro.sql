{% macro transfers_from_traces_exposed_blockchains_macro() %}

{% set exposed = [
    "arbitrum",
    "avalanche_c",
    "base",
    "bnb",
    "ethereum",
    "fantom",
    "gnosis",
    "linea",
    "optimism",
    "polygon",
    "sonic",
    "unichain",
    "zksync",
] %}

{{ return(exposed) }}

{% endmacro %}