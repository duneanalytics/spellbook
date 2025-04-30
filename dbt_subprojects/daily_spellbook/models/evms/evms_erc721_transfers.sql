{{ config(
        schema='evms',
        alias = 'erc721_transfers',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'[
                                        "abstract"
                                        , "apechain"
                                        , "arbitrum"
                                        , "avalanche_c"
                                        , "b3"
                                        , "base"
                                        , "berachain"
                                        , "blast"
                                        , "bnb"
                                        , "bob"
                                        , "boba"
                                        , "celo"
                                        , "corn"
                                        , "degen"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "gnosis"
                                        , "ink"
                                        , "kaia"
                                        , "lens"
                                        , "linea"
                                        , "mantle"
                                        , "nova"
                                        , "opbnb"
                                        , "optimism"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "shape"
                                        , "sonic"
                                        , "sophon"
                                        , "unichain"
                                        , "viction"
                                        , "worldchain"
                                        , "zkevm"
                                        , "zksync"
                                        , "zora"
                                        ]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "allelosi"]\') }}'
        )
}}

{% set erc721_transfers_models = [
     ('abstract', source('erc721_abstract', 'evt_transfer'))
     , ('apechain', source('erc721_apechain', 'evt_transfer'))
     , ('arbitrum', source('erc721_arbitrum', 'evt_transfer'))
     , ('avalanche_c', source('erc721_avalanche_c', 'evt_transfer'))
     , ('b3', source('erc721_b3', 'evt_transfer'))
     , ('base', source('erc721_base', 'evt_transfer'))
     , ('berachain', source('erc721_berachain', 'evt_transfer'))
     , ('blast', source('erc721_blast', 'evt_transfer'))
     , ('bnb', source('erc721_bnb', 'evt_transfer'))
     , ('bob', source('erc721_bob', 'evt_transfer'))
     , ('boba', source('erc721_boba', 'evt_transfer'))
     , ('celo', source('erc721_celo', 'evt_transfer'))
     , ('corn', source('erc721_corn', 'evt_transfer'))
     , ('degen', source('erc721_degen', 'evt_transfer'))
     , ('ethereum', source('erc721_ethereum', 'evt_transfer'))
     , ('fantom', source('erc721_fantom', 'evt_transfer'))
     , ('flare', source('erc721_flare', 'evt_transfer'))
     , ('gnosis', source('erc721_gnosis', 'evt_transfer'))
     , ('ink', source('erc721_ink', 'evt_transfer'))
     , ('kaia', source('erc721_kaia', 'evt_transfer'))
     , ('lens', source('erc721_lens', 'evt_transfer'))
     , ('linea', source('erc721_linea', 'evt_transfer'))
     , ('mantle', source('erc721_mantle', 'evt_transfer'))
     , ('nova', source('erc721_nova', 'evt_transfer'))
     , ('opbnb', source('erc721_opbnb', 'evt_transfer'))
     , ('optimism', source('erc721_optimism', 'evt_transfer'))
     , ('polygon', source('erc721_polygon', 'evt_transfer'))
     , ('ronin', source('erc721_ronin', 'evt_transfer'))
     , ('scroll', source('erc721_scroll', 'evt_transfer'))
     , ('sei', source('erc721_sei', 'evt_transfer'))
     , ('shape', source('erc721_shape', 'evt_transfer'))
     , ('sonic', source('erc721_sonic', 'evt_transfer'))
     , ('sophon', source('erc721_sophon', 'evt_transfer'))
     , ('unichain', source('erc721_unichain', 'evt_transfer'))
     , ('viction', source('erc721_viction', 'evt_transfer'))
     , ('worldchain', source('erc721_worldchain', 'evt_transfer'))
     , ('zkevm', source('erc721_zkevm', 'evt_transfer'))
     , ('zksync', source('erc721_zksync', 'evt_transfer'))
     , ('zora', source('erc721_zora', 'evt_transfer'))
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