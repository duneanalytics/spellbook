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
                                            ,"sei"
                                            ,"zkevm"
                                            ,"zksync"
                                            ,"zora"
                                            ,"mode"
                                        ]\',
                                        "sector",
                                        "tokens",
                                        \'["aalan3", "jeff-dude", "0xBoxer", "hildobby"]\') }}'
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
    ,'sei'
    ,'zkevm'
    ,'zksync'
    ,'zora'
    ,'mode'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT
          unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , "to"
        , contract_address
        , symbol
        , amount_raw
        , amount
        , price_usd
        , amount_usd
    FROM {{ ref('tokens_'~chain~'_transfers') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
