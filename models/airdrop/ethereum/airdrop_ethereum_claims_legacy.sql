{{ config(
	tags=['legacy'],
	
        alias = alias('claims', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "sector",
                                      "airdrop",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('ampleforth_ethereum_airdrop_claims_legacy')
    , ref('apecoin_ethereum_airdrop_claims_legacy')
    , ref('bend_dao_ethereum_airdrop_claims_legacy')
    , ref('blur_ethereum_airdrop_1_claims_legacy')
    , ref('cow_protocol_ethereum_airdrop_claims_legacy')
    , ref('dappradar_ethereum_airdrop_claims_legacy')
    , ref('dydx_ethereum_airdrop_claims_legacy')
    , ref('ens_ethereum_airdrop_claims_legacy')
    , ref('forta_network_ethereum_airdrop_claims_legacy')
    , ref('galxe_ethereum_airdrop_claims_legacy')
    , ref('gas_dao_ethereum_airdrop_claims_legacy')
    , ref('genie_ethereum_airdrop_claims_legacy')
    , ref('gitcoin_ethereum_airdrop_claims_legacy')
    , ref('hop_protocol_ethereum_airdrop_claims_legacy')
    , ref('looksrare_ethereum_airdrop_claims_legacy')
    , ref('oneinch_ethereum_airdrop_claims_legacy')
    , ref('paladin_ethereum_airdrop_claims_legacy')
    , ref('paraswap_ethereum_airdrop_claims_legacy')
    , ref('ribbon_ethereum_airdrop_claims_legacy')
    , ref('safe_ethereum_airdrop_claims_legacy')
    , ref('sudoswap_ethereum_airdrop_claims_legacy')
    , ref('tornado_cash_ethereum_airdrop_claims_legacy')
    , ref('uniswap_ethereum_airdrop_claims_legacy')
    , ref('x2y2_ethereum_airdrop_claims_legacy')
    , ref('pooltogether_ethereum_airdrop_claims_legacy')
    , ref('gearbox_ethereum_airdrop_claims_legacy')
    , ref('notional_ethereum_airdrop_claims_legacy')
    , ref('snowswap_ethereum_airdrop_claims_legacy')
    , ref('botto_ethereum_airdrop_claims_legacy')
    , ref('thales_ethereum_airdrop_claims_legacy')
    , ref('shapeshift_ethereum_airdrop_claims_legacy')
    , ref('value_defi_ethereum_airdrop_claims_legacy')
    , ref('tokenfy_ethereum_airdrop_claims_legacy')
    , ref('component_ethereum_airdrop_claims_legacy')
    , ref('forefront_ethereum_airdrop_claims_legacy')
    , ref('alchemydao_ethereum_airdrop_claims_legacy')
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
