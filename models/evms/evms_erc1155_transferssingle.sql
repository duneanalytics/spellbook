{{ config(
        alias ='erc1155_transferssingle',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc1155_singletransfers_models = [
     ('ethereum', source('erc1155_ethereum', 'evt_transfersingle'))
     , ('polygon', source('erc1155_polygon', 'evt_transfersingle'))
     , ('bnb', source('erc1155_bnb', 'evt_transfersingle'))
     , ('avalanche_c', source('erc1155_avalanche_c', 'evt_transfersingle'))
     , ('gnosis', source('erc1155_gnosis', 'evt_transfersingle'))
     , ('fantom', source('erc1155_fantom', 'evt_transfersingle'))
     , ('optimism', source('erc1155_optimism', 'evt_transfersingle'))
     , ('arbitrum', source('erc1155_arbitrum', 'evt_transfersingle'))
] %}

SELECT *
FROM (
        {% for erc1155_singletransfers_model in erc1155_singletransfers_models %}
        SELECT
        '{{ erc1155_singletransfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc1155_singletransfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );