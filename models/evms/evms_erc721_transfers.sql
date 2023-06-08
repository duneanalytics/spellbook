{{ config(
        alias ='erc721_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
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
     , ('optimism', source('erc721_optimism', 'evt_transfer'))
     , ('arbitrum', source('erc721_arbitrum', 'evt_transfer'))
] %}

SELECT *
FROM (
        {% for erc721_transfers_model in erc721_transfers_models %}
        SELECT
        '{{ erc721_transfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc721_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );