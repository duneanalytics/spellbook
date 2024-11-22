{{ config(
        schema='evms',
        alias = 'erc1155_transferssingle',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora", "scroll", "linea", "zkevm", "blast", "mantle"]\',
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
     , ('optimism', source('erc1155_optimism', 'evt_TransferSingle'))
     , ('arbitrum', source('erc1155_arbitrum', 'evt_transfersingle'))
     , ('celo', source('erc1155_celo', 'evt_transfersingle'))
     , ('base', source('erc1155_base', 'evt_transfersingle'))
     , ('zksync', source('erc1155_zksync', 'evt_transfersingle'))
     , ('zora', source('erc1155_zora', 'evt_transfersingle'))
     , ('scroll', source('erc1155_scroll', 'evt_transfersingle'))
     , ('linea', source('erc1155_linea', 'evt_transfersingle'))
     , ('zkevm', source('erc1155_zkevm', 'evt_transfersingle'))
     , ('blast', source('erc1155_blast', 'evt_transfersingle'))
     , ('mantle', source('erc1155_mantle', 'evt_transfersingle'))
     , ('sei', source('erc1155_sei', 'evt_transfersingle'))
     , ('bob', source('erc1155_bob', 'evt_transfersingle'))
] %}

SELECT *
FROM (
        {% for erc1155_singletransfers_model in erc1155_singletransfers_models %}
        SELECT
        '{{ erc1155_singletransfers_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , operator
        , "from"
        , to
        , id
        , value
        FROM {{ erc1155_singletransfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );