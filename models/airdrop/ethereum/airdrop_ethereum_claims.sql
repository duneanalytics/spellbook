{{ config(
        
        alias = 'claims',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "sector",
                                      "airdrop",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('ampleforth_ethereum_airdrop_claims')
    , ref('apecoin_ethereum_airdrop_claims')
    , ref('bend_dao_ethereum_airdrop_claims')
    , ref('blur_ethereum_airdrop_1_claims')
    , ref('cow_protocol_ethereum_airdrop_claims')
    , ref('dappradar_ethereum_airdrop_claims')
    , ref('dydx_ethereum_airdrop_claims')
    , ref('ens_ethereum_airdrop_claims')
    , ref('forta_network_ethereum_airdrop_claims')
    , ref('galxe_ethereum_airdrop_claims')
    , ref('gas_dao_ethereum_airdrop_claims')
    , ref('genie_ethereum_airdrop_claims')
    , ref('gitcoin_ethereum_airdrop_claims')
    , ref('hop_protocol_ethereum_airdrop_claims')
    , ref('looksrare_ethereum_airdrop_claims')
    , ref('oneinch_ethereum_airdrop_claims')
    , ref('paladin_ethereum_airdrop_claims')
    , ref('ribbon_ethereum_airdrop_claims')
    , ref('sudoswap_ethereum_airdrop_claims')
    , ref('tornado_cash_ethereum_airdrop_claims')
    , ref('uniswap_ethereum_airdrop_claims')
    , ref('x2y2_ethereum_airdrop_claims')
    , ref('pooltogether_ethereum_airdrop_claims')
    , ref('notional_ethereum_airdrop_claims')
    , ref('snowswap_ethereum_airdrop_claims')
    , ref('botto_ethereum_airdrop_claims')
    , ref('shapeshift_ethereum_airdrop_claims')
    , ref('tokenfy_ethereum_airdrop_claims')
    , ref('component_ethereum_airdrop_claims')
    , ref('forefront_ethereum_airdrop_claims')
    , ref('paraswap_ethereum_airdrop_claims') 
    , ref('safe_ethereum_airdrop_claims')
    , ref('gearbox_ethereum_airdrop_claims')
    , ref('thales_ethereum_airdrop_claims')
    , ref('value_defi_ethereum_airdrop_claims')
    , ref('alchemydao_ethereum_airdrop_claims')
    , ref('arkham_ethereum_airdrop_claims')
] %}

SELECT *
FROM (
    {% for airdrop_claims_model in airdrop_claims_models %}
    SELECT
    blockchain
    , block_time
    , block_number
    , project
    , airdrop_number
    , recipient
    , contract_address
    , tx_hash
    , amount_raw
    , amount_original
    , amount_usd
    , token_address
    , token_symbol
    , evt_index
    FROM {{ airdrop_claims_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
