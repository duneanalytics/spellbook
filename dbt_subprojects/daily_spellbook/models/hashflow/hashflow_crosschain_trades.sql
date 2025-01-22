{{
    config(
        
        alias = 'crosschain_trades'
        ,post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "optimism"]\',
                        "project",
                        "hashflow",
                        \'["BroderickBonelli", "ARDev097"]\') }}'
    )
}}

{% set cross_chain_models = 
    [
        ref('hashflow_avalanche_c_crosschain_trades')
        ,ref('hashflow_ethereum_crosschain_trades')
        ,ref('hashflow_bnb_crosschain_trades')
        ,ref('hashflow_optimism_crosschain_trades')
    ]
%}

{% for ref in cross_chain_models %}
SELECT 
    block_date
    ,block_time
    ,token_bought_symbol
    ,token_sold_symbol
    ,token_bought_amount
    ,token_sold_amount
    ,token_bought_amount_raw
    ,token_sold_amount_raw
    ,amount_usd
    ,token_bought_address
    ,token_sold_address
    ,trader
    ,tx_hash
    ,source_chain
    ,destination_chain
FROM {{ ref }}
{% if not loop.last %}
    UNION ALL
{% endif %}
{% endfor %}
