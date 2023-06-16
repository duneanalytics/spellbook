{{ config(
        alias ='erc20_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc20_transfers_models = [
     ('ethereum', source('erc20_ethereum', 'evt_transfer'))
     , ('polygon', source('erc20_polygon', 'evt_transfer'))
     , ('bnb', source('erc20_bnb', 'evt_transfer'))
     , ('avalanche_c', source('erc20_avalanche_c', 'evt_transfer'))
     , ('gnosis', source('erc20_gnosis', 'evt_transfer'))
     , ('fantom', source('erc20_fantom', 'evt_transfer'))
     , ('optimism', source('erc20_optimism', 'evt_transfer'))
     , ('arbitrum', source('erc20_arbitrum', 'evt_transfer'))
] %}

SELECT *
FROM (
        {% for erc20_transfers_model in erc20_transfers_models %}
        SELECT
        '{{ erc20_transfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc20_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );