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
    UNION ALL
    SELECT 
    concat(tx_id, '-', outer_instruction_index, '-', inner_instruction_index, '-', block_slot) as unique_key
    , 'solana' as blockchain
    , date_trunc('month', block_date) as block_month
    , block_date
    , block_time
    , block_slot as block_number
    , tx_id as tx_hash
    , cast(null as bigint) as evt_index
    , cast(null as array<bigint>) as trace_address
    , token_version as token_standard
    , tx_signer as tx_from
    , cast(null as varchar) as tx_to -- not a concept in solana
    , index as tx_index -- need to look into this 
    , from_owner as "from"
    , to_owner as "to"
    , from_base58(token_mint_address) as contract_address
    , symbol
    , amount as amount_raw
    , amount as amount -- need to look into this
    , cast(null as double) as price_usd -- need to look into this
    , amount_usd
    FROM {{ source('tokens_solana', 'transfers') }}
)
