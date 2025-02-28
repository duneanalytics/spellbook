{{ config(
        schema = 'tokens'
        , alias = 'transfers'
        , post_hook='{{ expose_spells(blockchains = \'[
                                            "abstract"
                                            ,"apechain"
                                            ,"arbitrum"
                                            ,"avalanche_c"
                                            ,"base"
                                            ,"berachain"
                                            ,"blast"
                                            ,"bnb"
                                            ,"boba"
                                            ,"celo"
                                            ,"corn"
                                            ,"ethereum"
                                            ,"fantom"
                                            ,"flare"
                                            ,"gnosis"
                                            ,"ink"
                                            ,"kaia"
                                            ,"linea"
                                            ,"mantle"
                                            ,"nova"
                                            ,"optimism"
                                            ,"polygon"
                                            ,"ronin"
                                            ,"scroll"
                                            ,"sei"
                                            ,"sonic"
                                            ,"sophon"
                                            ,"tron"
                                            ,"viction"
                                            ,"worldchain"
                                            ,"zkevm"
                                            ,"zksync"
                                            ,"zora"
                                        ]\',
                                        spell_type = "sector",
                                        spell_name = "tokens",
                                        contributors = \'["aalan3", "jeff-dude", "0xBoxer", "hildobby", "0xRob", "hosuke"]\') }}'
        )
}}

{% set chains = [
    'abstract'
    ,'apechain'
    ,'arbitrum'
    ,'avalanche_c'
    ,'base'
    ,'berachain'
    ,'blast'
    ,'bnb'
    ,'boba'
    ,'celo'
    ,'corn'
    ,'ethereum'
    ,'fantom'
    ,'flare'
    ,'gnosis'
    ,'ink'
    ,'kaia'
    ,'linea'
    ,'mantle'
    ,'nova'
    ,'optimism'
    ,'polygon'
    ,'ronin'
    ,'scroll'
    ,'sei'
    ,'sonic'
    ,'sophon'
    ,'tron'
    ,'viction'
    ,'worldchain'
    ,'zkevm'
    ,'zksync'
    ,'zora'
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
