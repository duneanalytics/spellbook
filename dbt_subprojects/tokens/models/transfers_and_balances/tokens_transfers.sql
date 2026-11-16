{% set chains = [
    'abstract'
    ,'apechain'
    ,'arbitrum'
    ,'arc'
    ,'avalanche_c'
    ,'b3'
    ,'base'
    ,'berachain'
    ,'blast'
    ,'bnb'
    ,'bob'
    ,'boba'
    ,'celo'
    ,'corn'
    ,'cronos'
    ,'degen'
    ,'ethereum'
    ,'fantom'
    ,'flare'
    ,'flow'
    ,'gnosis'
    ,'hemi'
    ,'henesys'
    ,'hyperevm'
    ,'ink'
    ,'kaia'
    ,'katana'
    ,'linea'
    ,'mantle'
    ,'megaeth'
    ,'mezo'
    ,'monad'
    ,'morph'
    ,'nova'
    ,'opbnb'
    ,'optimism'
    ,'peaq'
    ,'plasma'
    ,'plume'
    ,'polygon'
    ,'robinhood'
    ,'ronin'
    ,'rise'
    ,'scroll'
    ,'sei'
    ,'shape'
    ,'somnia'
    ,'sonic'
    ,'sophon'
    ,'story'
    ,'superseed'
    ,'tac'
    ,'taiko'
    ,'tempo'
    ,'tezos_evm'
    ,'tron'
    ,'unichain'
    ,'viction'
    ,'worldchain'
    ,'xlayer'
    ,'zkevm'
    ,'zksync'
    ,'zora'
    ,'lens'
] %}

{{ config(
        schema = 'tokens'
        , alias = 'transfers'
        , filtering_columns = ['block_month', 'blockchain']
        , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                        spell_type = "sector",
                                        spell_name = "tokens",
                                        contributors = \'["aalan3", "jeff-dude", "0xBoxer", "hildobby", "0xRob", "hosuke", "tomfutago"]\') }}'
        )
}}

{# Tron transfers are built in this subproject. Every other chain's table is written outside
   Spellbook, so it is read by source(). #}
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
        , _updated_at
    {% if chain == 'tron' -%}
    FROM {{ ref('tokens_tron_transfers') }}
    {% else -%}
    FROM {{ source('tokens_' ~ chain, 'transfers') }}
    {% endif -%}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
