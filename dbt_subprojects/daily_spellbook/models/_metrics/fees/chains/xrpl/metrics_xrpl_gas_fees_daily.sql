{{
    config(
        schema = 'metrics_xrpl',
        alias = 'gas_fees_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH fees AS (
    SELECT
        blockchain
        , block_date
        , SUM(tx_fee_usd) AS gas_fees_usd
    FROM
        {{ ref('gas_xrpl_fees') }}
    WHERE blockchain = 'xrpl'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
    {% endif %}
    GROUP BY
        blockchain
        , block_date
)

SELECT
    blockchain
    , block_date
    , gas_fees_usd
FROM fees