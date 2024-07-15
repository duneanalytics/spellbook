{{ config(
        schema = 'tokens'
        , alias = 'transfers'
        )
}}

{% set chains = [
     'ethereum'
    ,'optimism'
    ,'polygon'
    ,'bnb'
    ,'avalanche_c'
    ,'gnosis'
    ,'fantom'
    ,'arbitrum'
    ,'celo'
    ,'base'
    ,'zksync'
    ,'zora'
    ,'scroll'
    ,'zkevm'
    ,'linea'
    ,'mantle'
    ,'blast'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT *
    FROM {{ ref('tokens_'~chain~'_transfers') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
