{{ config(
        schema='evms',
        alias = 'erc20_transfers',
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

{% set erc20_transfers_models = [
     ('abstract', source('erc20_abstract', 'evt_transfer'))
     , ('apechain', source('erc20_apechain', 'evt_transfer'))
     , ('arbitrum', source('erc20_arbitrum', 'evt_transfer'))
     , ('avalanche_c', source('erc20_avalanche_c', 'evt_transfer'))
     , ('b3', source('erc20_b3', 'evt_transfer'))
     , ('base', source('erc20_base', 'evt_transfer'))
     , ('berachain', source('erc20_berachain', 'evt_transfer'))
     , ('blast', source('erc20_blast', 'evt_transfer'))
     , ('bnb', source('erc20_bnb', 'evt_transfer'))
     , ('bob', source('erc20_bob', 'evt_transfer'))
     , ('boba', source('erc20_boba', 'evt_transfer'))
     , ('celo', source('erc20_celo', 'evt_transfer'))
     , ('corn', source('erc20_corn', 'evt_transfer'))
     , ('degen', source('erc20_degen', 'evt_transfer'))
     , ('ethereum', source('erc20_ethereum', 'evt_transfer'))
     , ('fantom', source('erc20_fantom', 'evt_transfer'))
     , ('flare', source('erc20_flare', 'evt_transfer'))
     , ('gnosis', source('erc20_gnosis', 'evt_transfer'))
     , ('ink', source('erc20_ink', 'evt_transfer'))
     , ('kaia', source('erc20_kaia', 'evt_transfer'))
     , ('lens', source('erc20_lens', 'evt_transfer'))
     , ('linea', source('erc20_linea', 'evt_transfer'))
     , ('mantle', source('erc20_mantle', 'evt_transfer'))
     , ('nova', source('erc20_nova', 'evt_transfer'))
     , ('opbnb', source('erc20_opbnb', 'evt_transfer'))
     , ('optimism', source('erc20_optimism', 'evt_transfer'))
     , ('polygon', source('erc20_polygon', 'evt_transfer'))
     , ('ronin', source('erc20_ronin', 'evt_transfer'))
     , ('scroll', source('erc20_scroll', 'evt_transfer'))
     , ('sei', source('erc20_sei', 'evt_transfer'))
     , ('shape', source('erc20_shape', 'evt_transfer'))
     , ('sonic', source('erc20_sonic', 'evt_transfer'))
     , ('sophon', source('erc20_sophon', 'evt_transfer'))
     , ('unichain', source('erc20_unichain', 'evt_transfer'))
     , ('viction', source('erc20_viction', 'evt_transfer'))
     , ('worldchain', source('erc20_worldchain', 'evt_transfer'))
     , ('zkevm', source('erc20_zkevm', 'evt_transfer'))
     , ('zksync', source('erc20_zksync', 'evt_transfer'))
     , ('zora', source('erc20_zora', 'evt_transfer'))
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
