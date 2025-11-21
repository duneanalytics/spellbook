{{
    config(
        schema = 'metrics_xrpl',
        alias = 'transactions_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

SELECT
    blockchain
    , block_date
    , approx_distinct(tx_hash) AS tx_count
FROM
    {{ ref('tokens_xrpl_transfers') }}
WHERE
    amount_usd >= 1  -- $1 filter for significant transactions
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
    {% endif %}
GROUP BY
    blockchain
    , block_date