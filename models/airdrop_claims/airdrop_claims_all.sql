{{ config(
        alias ='all',
        post_hook='{{ expose_spells(\'["ethereum", "optimism"]\',
                                      "sector",
                                      "airdrop_claims",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('airdrop_claims_ethereum_ampleforth')
    , ref('airdrop_claims_ethereum_apecoin')
    , ref('airdrop_claims_ethereum_benddao')
    , ref('airdrop_claims_ethereum_blur_1')
    , ref('airdrop_claims_ethereum_cow_protocol')
    , ref('airdrop_claims_ethereum_dappradar')
    , ref('airdrop_claims_ethereum_dydx')
    , ref('airdrop_claims_ethereum_ens')
    , ref('airdrop_claims_ethereum_forta_network')
    , ref('airdrop_claims_ethereum_galxe')
    , ref('airdrop_claims_ethereum_gas_dao')
    , ref('airdrop_claims_ethereum_gitcoin')
    , ref('airdrop_claims_ethereum_hop_protocol')
    , ref('airdrop_claims_ethereum_looksrare')
    , ref('airdrop_claims_ethereum_oneinch')
    , ref('airdrop_claims_ethereum_paladin')
    , ref('airdrop_claims_ethereum_paraswap')
    , ref('airdrop_claims_ethereum_ribbon')
    , ref('airdrop_claims_ethereum_safe')
    , ref('airdrop_claims_ethereum_sudoswap')
    , ref('airdrop_claims_ethereum_tornado_cash')
    , ref('airdrop_claims_ethereum_uniswap')
    , ref('airdrop_claims_ethereum_x2y2')
    , ref('airdrop_claims_optimism_optimism_1')
    , ref('airdrop_claims_optimism_velodrome')
] %}


SELECT *
FROM (
    {% for airdrop_claims_model in airdrop_claims_models %}
    SELECT
    blockchain
    , block_time
    , block_number
    , project
    , airdrop_identifier
    , recipient
    , contract_address
    , tx_hash
    , quantity
    , token_address
    , token_symbol
    , evt_index
    FROM {{ airdrop_claims_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
