{{
  config(
    schema = 'mento_v2_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

/*
  Mento V2 trades on Celo
  
  This model excludes 1:1 same-currency collateral mint/redeem operations which are
  not price-discovery swaps. These pairs allow minting/redeeming Mento stablecoins
  using external collateral at a fixed 1:1 rate.
  
  EXCLUDED PAIRS (1:1 collateral operations, no price discovery):
  - cUSD ↔ USDC: USD stablecoin mint/redeem via Circle USDC
  - cUSD ↔ axlUSDC: USD stablecoin mint/redeem via Axelar bridged USDC
  - cEUR ↔ axlEUROC: EUR stablecoin mint/redeem via Axelar bridged EUROC
  
  KEPT PAIRS (real FX swaps with price discovery):
  - cUSD ↔ cEUR: USD/EUR exchange rate
  - cUSD ↔ cREAL: USD/BRL exchange rate
  - cUSD ↔ CELO: USD/CELO exchange rate
  - cEUR ↔ CELO: EUR/CELO exchange rate
  - Any other cross-currency pairs

  Token addresses:
  - cUSD: 0x765de816845861e75a25fca122bb6898b8b1282a
  - cEUR: 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73
  - USDC: 0xceba9300f2b948710d2653dd7b07f33a8b32118c
  - axlUSDC: 0xeb466342c4d449bc9f53a865d5cb90586f405215
  - axlEUROC: 0x061cc5a2c863e0c1cb404006d559db18a34c762d
*/

SELECT
    'celo' AS blockchain,
    'mento' AS project,
    '2' AS version,
    CAST(date_trunc('month', t.evt_block_time) AS date) AS block_month,
    CAST(date_trunc('day', t.evt_block_time) AS date) AS block_date,
    t.evt_block_time AS block_time,
    t.evt_block_number AS block_number,
    t.amountOut AS token_bought_amount_raw,
    t.amountIn AS token_sold_amount_raw,
    t.tokenOut AS token_bought_address,
    t.tokenIn AS token_sold_address,
    t.trader AS taker,
    CAST(NULL AS varbinary) AS maker,
    t.contract_address AS project_contract_address,
    t.evt_tx_hash AS tx_hash,
    t.evt_index
FROM {{ source('mento_celo', 'Broker_evt_Swap') }} t
WHERE 1=1
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}
  -- Exclude 1:1 same-currency mint/redeem operations (no price discovery)
  AND NOT (
      -- cUSD <-> USDC
      (t.tokenIn = 0x765de816845861e75a25fca122bb6898b8b1282a AND t.tokenOut = 0xceba9300f2b948710d2653dd7b07f33a8b32118c)
      OR (t.tokenIn = 0xceba9300f2b948710d2653dd7b07f33a8b32118c AND t.tokenOut = 0x765de816845861e75a25fca122bb6898b8b1282a)
      -- cUSD <-> axlUSDC
      OR (t.tokenIn = 0x765de816845861e75a25fca122bb6898b8b1282a AND t.tokenOut = 0xeb466342c4d449bc9f53a865d5cb90586f405215)
      OR (t.tokenIn = 0xeb466342c4d449bc9f53a865d5cb90586f405215 AND t.tokenOut = 0x765de816845861e75a25fca122bb6898b8b1282a)
      -- cEUR <-> axlEUROC
      OR (t.tokenIn = 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73 AND t.tokenOut = 0x061cc5a2c863e0c1cb404006d559db18a34c762d)
      OR (t.tokenIn = 0x061cc5a2c863e0c1cb404006d559db18a34c762d AND t.tokenOut = 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73)
  )
