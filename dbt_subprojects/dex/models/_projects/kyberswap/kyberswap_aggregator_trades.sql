{{ config
(
    
    schema = 'kyberswap_aggregator',
    alias = 'trades',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["arbitrum", "base"]\',
                            "project",
                            "kyberswap",
                            \'["nhd98z", "Henrystats"]\') }}'
    )
}}

{% set models = [
    ref('kyberswap_aggregator_arbitrum_trades')
    ,ref('kyberswap_aggregator_avalanche_c_trades')
    ,ref('kyberswap_aggregator_ethereum_trades')
    ,ref('kyberswap_aggregator_optimism_trades')
    ,ref('kyberswap_aggregator_bnb_trades')
    ,ref('kyberswap_aggregator_polygon_trades')
    ,ref('kyberswap_aggregator_base_trades')
] %}


SELECT *
FROM (
    {% for aggregator_dex_model in models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_month,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_pair,
        token_bought_amount,
        token_sold_amount,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        evt_index,
        trace_address
    FROM {{ aggregator_dex_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
