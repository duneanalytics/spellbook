{{ config(
        schema = 'tokens'
        , alias = 'transfers'
        , post_hook='{{ expose_spells(\'[
                                            "arbitrum"
                                            ,"avalanche_c"
                                            ,"base"
                                            ,"blast"
                                            ,"bnb"
                                            ,"celo"
                                            ,"ethereum"
                                            ,"fantom"
                                            ,"gnosis"
                                            ,"linea"
                                            ,"mantle"
                                            ,"optimism"
                                            ,"polygon"
                                            ,"scroll"
                                            ,"zkevm"
                                            ,"zksync"
                                            ,"zora"
                                        ]\',
                                        "sector",
                                        "tokens",
                                        \'["aalan3", "jeff-dude"]\') }}'
        )
}}

{% set chains = [
     'arbitrum'
    ,'avalanche_c'
    ,'base'
    ,'blast'
    ,'bnb'
    ,'celo'
    ,'ethereum'
    ,'fantom'
    ,'gnosis'
    ,'linea'
    ,'mantle'
    ,'optimism'
    ,'polygon'
    ,'scroll'
    ,'zkevm'
    ,'zksync'
    ,'zora'
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
