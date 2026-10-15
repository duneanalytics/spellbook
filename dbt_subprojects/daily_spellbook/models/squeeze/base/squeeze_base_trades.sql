{{ config(
    alias = 'trades',
    schema = 'squeeze_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'blockchain', 'tx_hash', 'evt_index']
) }}

{% set project_start_date = '2025-01-01' %}
{% set blockchain = 'base' %}

-- Attribution table: Uniswap (etc.) volume on Squeeze-launched tokens.
-- Double-counted vs dex.trades project volume — intentional launchpad lens.

WITH deployments AS (
    SELECT * FROM {{ ref('squeeze_base_deployments') }}
)

SELECT
    t.block_time
    , date_trunc('day', t.block_time) AS block_date
    , date_trunc('month', t.block_time) AS block_month
    , t.blockchain
    , 'squeeze' AS project
    , t.project AS dex_project
    , t.version AS dex_version
    , t.token_bought_amount
    , t.token_sold_amount
    , t.token_bought_symbol
    , t.token_sold_symbol
    , t.token_bought_address
    , t.token_sold_address
    , t.amount_usd
    , t.taker AS user
    , t.tx_hash
    , t.evt_index
FROM {{ source('dex', 'trades') }} t
INNER JOIN deployments d
    ON t.blockchain = d.blockchain
   AND (
        t.token_bought_address = d.token
        OR t.token_sold_address = d.token
   )
WHERE t.blockchain = '{{ blockchain }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_time') }}
  {% else %}
  AND t.block_time >= TIMESTAMP '{{ project_start_date }}'
  {% endif %}
