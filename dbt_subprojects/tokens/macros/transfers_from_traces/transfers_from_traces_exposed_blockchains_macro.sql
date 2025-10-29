{% macro transfers_from_traces_exposed_blockchains_macro() %}

{% set exposed = [
    "abstract",
    "apechain",
    "arbitrum",
    "avalanche_c",
    "b3",
    "base",
    "berachain",
    "blast",
    "bnb",
    "bob",
    "boba",
    "celo",
    "corn",
    "degen",
    "ethereum",
    "fantom",
    "flare",
    "flow",
    "gnosis",
    "goerli",
    "hemi",
    "hyperevm",
    "ink",
    "kaia",
    "katana",
    "lens",
    "linea",
    "mantle",
    "mezo",
    "nova",
    "optimism",
    "polygon",
    "sonic",
    "unichain",
    "zksync",
] %}

{{ return(exposed) }}

{% endmacro %}