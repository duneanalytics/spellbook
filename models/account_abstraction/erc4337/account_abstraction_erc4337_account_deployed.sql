{{ config(
        schema = 'account_abstraction_erc4337',
        alias = 'account_deployed',
        post_hook='{{ expose_spells(\'["ethereum","polygon","arbitrum","optimism","avalanche_c","gnosis","celo"]\',
                                "project",
                                "erc4337",
                                \'["0xbitfly"]\') }}'
        )
}}

{% set erc4337_models = [
  ref('account_abstraction_erc4337_ethereum_account_deployed')
, ref('account_abstraction_erc4337_polygon_account_deployed')
, ref('account_abstraction_erc4337_arbitrum_account_deployed')
, ref('account_abstraction_erc4337_optimism_account_deployed')
, ref('account_abstraction_erc4337_avalanche_c_account_deployed')
, ref('account_abstraction_erc4337_gnosis_account_deployed')
, ref('account_abstraction_erc4337_base_account_deployed')
, ref('account_abstraction_erc4337_bnb_account_deployed')
, ref('account_abstraction_erc4337_celo_account_deployed')
] %}

SELECT *
FROM (
    {% for erc4337_model in erc4337_models %}
    SELECT
          blockchain
        , version
        , block_time
        , block_month
        , userop_hash
        , entrypoint_contract
        , tx_hash
        , sender
        , paymaster
        , factory
    FROM {{ erc4337_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;