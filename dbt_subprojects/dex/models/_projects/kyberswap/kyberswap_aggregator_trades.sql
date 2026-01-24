{{ config
(
    
    schema = 'kyberswap_aggregator',
    alias = 'trades',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "berachain", "bnb", "ethereum", "linea", "mantle", "optimism", "polygon", "ronin", "sonic", "unichain"]\',
                            "project",
                            "kyberswap",
                            \'["nhd98z", "Henrystats", "phu_ngo"]\') }}'
    )
}}

{% set models = [
    ref('kyberswap_aggregator_arbitrum_trades')
    ,ref('kyberswap_aggregator_avalanche_c_trades')
    ,ref('kyberswap_aggregator_base_trades')
    ,ref('kyberswap_aggregator_berachain_trades')
    ,ref('kyberswap_aggregator_bnb_trades')
    ,ref('kyberswap_aggregator_ethereum_trades')
    ,ref('kyberswap_aggregator_linea_trades')
    ,ref('kyberswap_aggregator_mantle_trades')
    ,ref('kyberswap_aggregator_optimism_trades')
    ,ref('kyberswap_aggregator_polygon_trades')
    ,ref('kyberswap_aggregator_ronin_trades')
    ,ref('kyberswap_aggregator_sonic_trades')
    ,ref('kyberswap_aggregator_unichain_trades')
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
