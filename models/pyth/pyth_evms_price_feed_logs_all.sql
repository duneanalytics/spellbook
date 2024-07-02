{{ config(
        schema = 'pyth',
        alias = 'price_evms_price_feed_logs_all',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","celo","ethereum","gnosis","linea","mantle","optimism","polygon","scroll","zkevm","zksync"]\',
                                "project",
                                "pyth",
                                \'["synthquest"]\') }}'
        )
}}

{% set pyth_transaction_models = [
 ref('pyth_arbitrum_price_feed_logs'),
 ref('pyth_avalanche_c_price_feed_logs'),
 ref('pyth_base_price_feed_logs'),
 ref('pyth_blast_price_feed_logs'),
 ref('pyth_bnb_price_feed_logs'),
 ref('pyth_celo_price_feed_logs'),
 ref('pyth_ethereum_price_feed_logs'),
 ref('pyth_fantom_price_feed_logs'),
 ref('pyth_gnosis_price_feed_logs'),
 ref('pyth_linea_price_feed_logs'),
 ref('pyth_mantle_price_feed_logs'),
 ref('pyth_optimism_price_feed_logs'),
 ref('pyth_polygon_price_feed_logs'),
 ref('pyth_scroll_price_feed_logs'),
 ref('pyth_zkevm_price_feed_logs'),
 ref('pyth_zksync_price_feed_logs')
] %}


SELECT *
FROM (
    {% for transfer_model in pyth_transaction_models %}
    SELECT
          chain_type
        , chain
        , identifier
        , category
        , token1
        , token2
        , block_time
        , block_number
        , publish_time
        , price
        , conf
        , tx_hash
        , price_id
        , index
        , tx_index
        , tx_to
        , tx_from
        , namespace
    FROM {{ transfer_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
