{{ config(
	tags=['legacy'],
	
        schema = 'account_abstraction_erc4337',
        alias = alias('userops', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","polygon","arbitrum","optimism","avalanche_c","gnosis"]\',
                                "project",
                                "erc4337",
                                \'["0xbitfly", "hosuke"]\') }}'
        )
}}

{% set erc4337_models = [
  ref('account_abstraction_erc4337_ethereum_userops_legacy')
, ref('account_abstraction_erc4337_polygon_userops_legacy')
, ref('account_abstraction_erc4337_arbitrum_userops_legacy')
, ref('account_abstraction_erc4337_optimism_userops_legacy')
, ref('account_abstraction_erc4337_avalanche_c_userops_legacy')
, ref('account_abstraction_erc4337_gnosis_userops_legacy')
] %}

SELECT *
FROM (
    {% for erc4337_model in erc4337_models %}
    SELECT
          blockchain
        , version
        , block_time
        , entrypoint_contract
        , tx_hash
        , sender
        , userop_hash
        , success
        , paymaster
        , op_fee
        , op_fee_usd
        , bundler
        , tx_to
        , gas_symbol
        , tx_fee
        , tx_fee_usd
        , beneficiary
    FROM {{ erc4337_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;