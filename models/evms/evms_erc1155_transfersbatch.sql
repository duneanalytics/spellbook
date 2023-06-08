{{ config(
        alias ='erc1155_transfersbatch',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc1155_batchtransfers_models = [
     ('ethereum', source('erc1155_ethereum', 'evt_transferbatch'))
     , ('polygon', source('erc1155_polygon', 'evt_transferbatch'))
     , ('bnb', source('erc1155_bnb', 'evt_transferbatch'))
     , ('avalanche_c', source('erc1155_avalanche_c', 'evt_transferbatch'))
     , ('gnosis', source('erc1155_gnosis', 'evt_transferbatch'))
     , ('fantom', source('erc1155_fantom', 'evt_transferbatch'))
     , ('optimism', source('erc1155_optimism', 'evt_transferbatch'))
     , ('arbitrum', source('erc1155_arbitrum', 'evt_transferbatch'))
] %}

SELECT *
FROM (
        {% for erc1155_batchtransfers_model in erc1155_batchtransfers_models %}
        SELECT
        '{{ erc1155_batchtransfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc1155_batchtransfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );