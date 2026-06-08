{{
    config(
        schema = 'kuru_monad',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{# Kuru is a CLOB: every fill (Router-routed, KuruFlow-routed, direct limit
   order, or margin) ultimately emits OrderBook.Trade on a per-market proxy.
   Market metadata lives in Router.MarketRegistered. The `price` field is
   always scaled by 10^18 regardless of the market's pricePrecision (which is
   UI tick metadata) — verified by cross-checking Router↔OB matched txs. #}

WITH markets AS (
    SELECT market, baseAsset, quoteAsset, sizePrecision
    FROM (
        SELECT
            market,
            baseAsset,
            quoteAsset,
            sizePrecision,
            ROW_NUMBER() OVER (PARTITION BY market ORDER BY evt_block_number DESC) AS rn
        FROM {{ source('kuru_monad', 'router_evt_marketregistered') }}
    ) t
    WHERE rn = 1
),

trades AS (
    SELECT
        t.evt_block_time AS block_time,
        t.evt_block_number AS block_number,
        t.evt_tx_hash AS tx_hash,
        t.evt_index,
        t.contract_address AS project_contract_address,
        t.takerAddress AS taker,
        t.makerAddress AS maker,
        t.isBuy,
        t.filledSize,
        t.price,
        m.baseAsset,
        m.quoteAsset,
        m.sizePrecision
    FROM {{ source('kuru_monad', 'orderbook_evt_trade') }} t
    INNER JOIN markets m
        ON m.market = t.contract_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),

with_decimals AS (
    SELECT
        tr.*,
        COALESCE(eb.decimals, 18) AS base_decimals,
        COALESCE(eq.decimals, 18) AS quote_decimals
    FROM trades tr
    LEFT JOIN {{ source('tokens', 'erc20') }} eb
        ON eb.contract_address = tr.baseAsset
        AND eb.blockchain = 'monad'
    LEFT JOIN {{ source('tokens', 'erc20') }} eq
        ON eq.contract_address = tr.quoteAsset
        AND eq.blockchain = 'monad'
)

SELECT
    'monad' AS blockchain,
    'kuru' AS project,
    '1' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day',   block_time) AS date) AS block_date,
    block_time,
    block_number,
    CAST(
        CASE
            WHEN isBuy
                THEN filledSize * power(10, base_decimals) / sizePrecision
            ELSE filledSize * price * power(10, quote_decimals) / (sizePrecision * power(10, 18))
        END AS UINT256
    ) AS token_bought_amount_raw,
    CAST(
        CASE
            WHEN isBuy
                THEN filledSize * price * power(10, quote_decimals) / (sizePrecision * power(10, 18))
            ELSE filledSize * power(10, base_decimals) / sizePrecision
        END AS UINT256
    ) AS token_sold_amount_raw,
    CASE WHEN isBuy THEN baseAsset  ELSE quoteAsset END AS token_bought_address,
    CASE WHEN isBuy THEN quoteAsset ELSE baseAsset  END AS token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM with_decimals
