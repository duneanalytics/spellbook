{{ config(
        schema = 'tokens'
        alias = 'transfers'
        )
}}

{% set chains = [
     'ethereum'
--     ,'polygon'
--     ,'bnb'
--     ,'avalanche_c'
--     ,'gnosis'
--     ,'fantom'
     ,'optimism'
--     ,'arbitrum'
--     ,'celo'
--     ,'base'
--     ,'goerli'
--     ,'zksync'
--     ,'zora'
--     ,'scroll'
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
