{{ config(
        tags = ['prod_exclude'],
        schema='prices_v2',
        alias = 'minute_sparse',
        file_format = 'delta',
        materialized = 'incremental',
        partition_by = ['date'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'timestamp'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
        )
}}

-- this model feeds into sqlmesh which performs a forward fill and aggregates upto higher timeframes

{% set prices_models = [
    ref('prices_v2_dex_minute')
    ,ref('prices_v2_coinpaprika_minute')
] %}


SELECT *
FROM
(
    {% for model in prices_models %}
    SELECT
        blockchain
        , contract_address
        , timestamp
        , price
        , volume    -- can be null
        , source    -- dex.trades/coinpaprika/..
        , date      -- partition
    FROM {{ model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('timestamp') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
