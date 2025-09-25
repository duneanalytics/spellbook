{% set chains = [
    'abstract'
    ,'apechain'
    ,'arbitrum'
    ,'avalanche_c'
    ,'b3'
    ,'base'
    ,'berachain'
    ,'bnb'
    ,'bob'
    ,'boba'
    ,'celo'
    ,'corn'
    ,'degen'
    ,'ethereum'
    ,'fantom'
    ,'flare'
    ,'flow'
    ,'gnosis'
    ,'hyperevm'
    ,'ink'
    ,'kaia'
    ,'katana'
    ,'linea'
    ,'mantle'
    ,'nova'
    ,'opbnb'
    ,'optimism'
    ,'peaq'
    ,'plume'
    ,'polygon'
    ,'ronin'
    ,'scroll'
    ,'sei'
    ,'shape'
    ,'somnia'
    ,'sonic'
    ,'sophon'
    ,'superseed'
    ,'taiko'
    ,'tron'
    ,'unichain'
    ,'viction'
    ,'worldchain'
    ,'zkevm'
    ,'zksync'
    ,'zora'
    ,'lens'
] %}

{{ config(
        schema = 'tokens'
        , alias = 'transfers'
        , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                        spell_type = "sector",
                                        spell_name = "tokens",
                                        contributors = \'["aalan3", "jeff-dude", "0xBoxer", "hildobby", "0xRob", "hosuke"]\') }}'
        )
}}

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
