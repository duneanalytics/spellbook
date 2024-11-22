{{ config(
        schema='evms',
        alias = 'erc20_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora", "scroll", "linea", "zkevm", "blast", "mantle"]\',
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
     , ('celo', source('erc20_celo', 'evt_transfer'))
     , ('base', source('erc20_base', 'evt_transfer'))
     , ('zksync', source('erc20_zksync', 'evt_transfer'))
     , ('zora', source('erc20_zora', 'evt_transfer'))
     , ('scroll', source('erc20_scroll', 'evt_transfer'))
     , ('linea', source('erc20_linea', 'evt_transfer'))
     , ('zkevm', source('erc20_zkevm', 'evt_transfer'))
     , ('blast', source('erc20_blast', 'evt_transfer'))
     , ('mantle', source('erc20_mantle', 'evt_transfer'))
     , ('sei', source('erc20_sei', 'evt_transfer'))
     , ('bob', source('erc20_bob', 'evt_transfer'))
] %}

SELECT *
FROM (
        {% for erc20_transfers_model in erc20_transfers_models %}
        SELECT
        '{{ erc20_transfers_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , "from"
        , to
        , value
        FROM {{ erc20_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );