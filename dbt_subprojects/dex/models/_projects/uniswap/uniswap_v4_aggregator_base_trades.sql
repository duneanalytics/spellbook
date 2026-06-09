{{ config(
        schema = 'uniswap_v4',
        alias = 'aggregator_base_trades'
        )
}}

{% set uniswap_v4_aggregator_models = [
ref('uniswap_v4_arbitrum_aggregator_base_trades')
, ref('uniswap_v4_avalanche_c_aggregator_base_trades')
, ref('uniswap_v4_base_aggregator_base_trades')
, ref('uniswap_v4_blast_aggregator_base_trades')
, ref('uniswap_v4_bnb_aggregator_base_trades')
, ref('uniswap_v4_celo_aggregator_base_trades')
, ref('uniswap_v4_ethereum_aggregator_base_trades')
, ref('uniswap_v4_ink_aggregator_base_trades')
, ref('uniswap_v4_monad_aggregator_base_trades')
, ref('uniswap_v4_optimism_aggregator_base_trades')
, ref('uniswap_v4_polygon_aggregator_base_trades')
, ref('uniswap_v4_tempo_aggregator_base_trades')
, ref('uniswap_v4_unichain_aggregator_base_trades')
, ref('uniswap_v4_worldchain_aggregator_base_trades')
, ref('uniswap_v4_zora_aggregator_base_trades')
] %}

SELECT *
FROM (
    {% for agg_model in uniswap_v4_aggregator_models %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_date,
        block_time,
        token_bought_amount_raw,
        token_sold_amount_raw,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address, --ensure field is explicitly cast as array<bigint> in base models
        evt_index
    FROM {{ agg_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
