{{ config(
        schema='evms',
        alias = 'erc721_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora", "scroll", "linea", "zkevm", "blast", "mantle", "ronin"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc721_transfers_models = [
     ('ethereum', source('erc721_ethereum', 'evt_transfer'))
     , ('polygon', source('erc721_polygon', 'evt_transfer'))
     , ('bnb', source('erc721_bnb', 'evt_transfer'))
     , ('avalanche_c', source('erc721_avalanche_c', 'evt_transfer'))
     , ('gnosis', source('erc721_gnosis', 'evt_transfer'))
     , ('fantom', source('erc721_fantom', 'evt_transfer'))
     , ('optimism', source('erc721_optimism', 'evt_Transfer'))
     , ('arbitrum', source('erc721_arbitrum', 'evt_transfer'))
     , ('celo', source('erc721_celo', 'evt_transfer'))
     , ('base', source('erc721_base', 'evt_transfer'))
     , ('zksync', source('erc721_zksync', 'evt_transfer'))
     , ('zora', source('erc721_zora', 'evt_transfer'))
     , ('scroll', source('erc721_scroll', 'evt_transfer'))
     , ('linea', source('erc721_linea', 'evt_transfer'))
     , ('zkevm', source('erc721_zkevm', 'evt_transfer'))
     , ('blast', source('erc721_blast', 'evt_transfer'))
     , ('mantle', source('erc721_mantle', 'evt_transfer'))
     , ('sei', source('erc721_sei', 'evt_transfer'))
     , ('ronin', source('erc721_ronin', 'evt_transfer'))
] %}

SELECT *
FROM (
        {% for erc721_transfers_model in erc721_transfers_models %}
        SELECT
        '{{ erc721_transfers_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , "from"
        , to
        , tokenId
        FROM {{ erc721_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );