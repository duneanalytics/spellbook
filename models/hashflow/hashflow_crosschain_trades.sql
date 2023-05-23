{{
    config(
        alias="cross_chain_trades"
        ,post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb"]\',
                        "project",
                        "hashflow",
                        \'["BroderickBonelli"]\') }}'
    )
}}

{% set cross_chain_models = 
    [
        ref('hashflow_avalanche_c_crosschain_trades')
        ,ref('hashflow_ethereum_crosschain_trades')
        ,ref('hashflow_bnb_crosschain_trades')
    ]
%}

{% for ref in cross_chain_models %}
SELECT 
    *
FROM {{ ref }}
{% if not loop.last %}
    UNION ALL
{% endif %}
{% endfor %}
