{{ config(
        
        alias = 'erc1155_transfersbatch',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
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
     , ('celo', source('erc1155_celo', 'evt_transferbatch'))
     , ('base', source('erc1155_base', 'evt_transferbatch'))
     , ('goerli', source('erc1155_goerli', 'evt_transferbatch'))
     , ('zksync', source('erc1155_zksync', 'evt_transferbatch'))
     , ('zora', source('erc1155_zora', 'evt_transferbatch'))
     , ('scroll', source('erc1155_scroll', 'evt_transferbatch'))
] %}

SELECT *
FROM (
        {% for erc1155_batchtransfers_model in erc1155_batchtransfers_models %}
        SELECT
        '{{ erc1155_batchtransfers_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , operator
        , "from"
        , to
        , ids
        , "values"
        FROM {{ erc1155_batchtransfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );