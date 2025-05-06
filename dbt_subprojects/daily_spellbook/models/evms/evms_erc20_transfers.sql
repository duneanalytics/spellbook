{{ config(
        schema='evms',
        alias = 'erc20_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'[
                                        "abstract",
                                        "apechain",
                                        "arbitrum",
                                        "avalanche_c",
                                        "b3",
                                        "base",
                                        "berachain",
                                        "blast",
                                        "bnb",
                                        "boba",
                                        "celo",
                                        "ethereum",
                                        "fantom",
                                        "gnosis",
                                        "kaia",
                                        "linea",
                                        "mantle",
                                        "optimism",
                                        "polygon",
                                        "ronin",
                                        "scroll",
                                        "sonic",
                                        "zkevm",
                                        "zksync",
                                        "zora",
                                        "opbnb",
                                        "ink",
                                        "unichain"
                                        ]\',
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
     , ('ronin', source('erc20_ronin', 'evt_transfer'))
     , ('abstract', source('erc20_abstract', 'evt_transfer'))
     , ('boba', source('erc20_boba', 'evt_transfer'))
     , ('sonic', source('erc20_sonic', 'evt_transfer'))
     , ('kaia', source('erc20_kaia', 'evt_transfer'))
     , ('apechain', source('erc20_apechain', 'evt_transfer'))
     , ('berachain', source('erc20_berachain', 'evt_transfer'))
     , ('b3', source('erc20_b3', 'evt_transfer'))
     , ('opbnb', source('erc20_opbnb', 'evt_transfer'))
     , ('ink', source('erc20_ink', 'evt_transfer'))
     , ('lens', source('erc20_lens', 'evt_transfer'))
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
