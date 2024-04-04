{{ config(
	    schema = 'yield_yak',
        alias = 'balances',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c"]\',
                                "project",
                                "yield_yak",
                                \'["angus_1"]\') }}'
        )
}}

{% set yield_yak_models = [
ref('yield_yak_avalanche_c_balances')
,ref('yield_yak_arbitrum_balances')
] %}


SELECT *
FROM (
    {% for balances_model in yield_yak_models %}
    SELECT
        blockchain
        , contract_address
        , from_time
        , to_time
        , deposit_token_balance
    FROM {{ balances_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
