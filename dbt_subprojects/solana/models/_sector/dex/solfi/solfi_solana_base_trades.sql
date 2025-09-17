{{
  config(
    schema = 'solfi_solana',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'surrogate_key'],
    pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-09-01' %}

-- 1) One row per SolFi CPI (swap call)
WITH solfi_calls AS (
  SELECT
      s.call_block_time                  AS block_time,
      s.call_block_slot                  AS block_slot,
      s.call_tx_id                       AS tx_id,
      s.call_tx_index                    AS tx_index,
      s.call_outer_instruction_index     AS outer_ix,
      s.call_inner_instruction_index     AS solfi_ix,
      s.call_is_inner                    AS is_inner_swap,
      s.call_outer_executing_account     AS outer_exec,
      s.call_tx_signer                   AS trader_id,
      -- accounts we will use to hard‑filter transfers
      s.account_user,
      s.account_pair,
      s.account_poolTokenAccountA,
      s.account_poolTokenAccountB,
      s.account_userTokenAccountA,
      s.account_userTokenAccountB
  FROM {{ source('solfi_solana', 'solfi_call_swap') }} s
  WHERE 1=1
    {% if is_incremental() %}
      AND {{ incremental_predicate('s.call_block_time') }}
    {% else %}
      AND s.call_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
),

-- 2) Boundaries of each SolFi CPI frame (next SolFi inner ix in same (tx, outer_ix))
frames AS (
  SELECT
      c.*,
      LEAD(c.solfi_ix) OVER (
        PARTITION BY c.tx_id, c.block_slot, c.outer_ix
        ORDER BY c.solfi_ix
      ) AS next_solfi_ix
  FROM solfi_calls c
),

-- 3) Prefilter transfers to only those inside the SolFi frame and touching SolFi accounts
frame_transfers AS (
  SELECT
    f.tx_id, f.block_slot, f.block_time, f.tx_index,
    f.outer_ix, f.solfi_ix, f.next_solfi_ix,
    f.trader_id, f.account_user,
    f.account_poolTokenAccountA, f.account_poolTokenAccountB,
    f.account_userTokenAccountA, f.account_userTokenAccountB,
    t.inner_instruction_index AS transfer_ix,
    t.token_mint_address,
    t.amount,
    t.from_owner, t.to_owner,
    t.from_token_account, t.to_token_account
  FROM frames f
  JOIN {{ source('tokens_solana', 'transfers') }} t
    ON t.tx_id = f.tx_id
   AND t.block_slot = f.block_slot
   AND t.outer_instruction_index = f.outer_ix
   AND t.inner_instruction_index > f.solfi_ix
   AND (f.next_solfi_ix IS NULL OR t.inner_instruction_index < f.next_solfi_ix)
   AND t.token_version = 'spl_token'                               -- only SPL token flows
   -- hard prefilter: transfer must touch SolFi's 4 accounts (user A/B or pool A/B)
   AND (
         t.from_token_account IN (f.account_userTokenAccountA, f.account_userTokenAccountB, f.account_poolTokenAccountA, f.account_poolTokenAccountB)
      OR t.to_token_account   IN (f.account_userTokenAccountA, f.account_userTokenAccountB, f.account_poolTokenAccountA, f.account_poolTokenAccountB)
   )
  WHERE 1=1
    {% if is_incremental() %}
      AND {{ incremental_predicate('t.block_time') }}
    {% else %}
      AND t.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
),

-- 4) Net flow to the "user set" for each mint inside the frame
per_mint_net AS (
  SELECT
    tx_id, block_slot, block_time, tx_index, outer_ix, solfi_ix,
    trader_id, account_user,
    token_mint_address,
    SUM(CASE WHEN to_owner   IN (trader_id, account_user) THEN amount ELSE 0 END) AS to_user_raw,
    SUM(CASE WHEN from_owner IN (trader_id, account_user) THEN amount ELSE 0 END) AS from_user_raw
  FROM frame_transfers
  GROUP BY 1,2,3,4,5,6,7,8,9
),

ranked AS (
  SELECT
    *,
    (to_user_raw - from_user_raw) AS net_to_user_raw,
    ROW_NUMBER() OVER (
      PARTITION BY tx_id, block_slot, outer_ix, solfi_ix
      ORDER BY (to_user_raw - from_user_raw) DESC, token_mint_address
    ) AS r_buy,
    ROW_NUMBER() OVER (
      PARTITION BY tx_id, block_slot, outer_ix, solfi_ix
      ORDER BY (to_user_raw - from_user_raw) ASC, token_mint_address
    ) AS r_sell
  FROM per_mint_net
),

legs AS (
  SELECT
    tx_id, block_slot, block_time, tx_index, outer_ix, solfi_ix,
    trader_id, account_user,
    MAX(CASE WHEN r_buy  = 1 AND net_to_user_raw > 0 THEN token_mint_address END) AS token_bought_mint_address,
    MAX(CASE WHEN r_buy  = 1 AND net_to_user_raw > 0 THEN  net_to_user_raw       END) AS token_bought_amount_raw,
    MAX(CASE WHEN r_sell = 1 AND net_to_user_raw < 0 THEN token_mint_address END) AS token_sold_mint_address,
    MAX(CASE WHEN r_sell = 1 AND net_to_user_raw < 0 THEN -net_to_user_raw      END) AS token_sold_amount_raw
  FROM ranked
  GROUP BY 1,2,3,4,5,6,7,8
),

-- 5) Vault heuristics:
--    bought vault = earliest (by transfer_ix) non-user sender of bought mint
--    sold   vault = earliest (by transfer_ix) non-user receiver of sold mint
bought_vault AS (
  SELECT
    ft.tx_id, ft.block_slot, ft.outer_ix, ft.solfi_ix,
    ft.from_token_account AS token_bought_vault,
    ROW_NUMBER() OVER (
      PARTITION BY ft.tx_id, ft.block_slot, ft.outer_ix, ft.solfi_ix
      ORDER BY ft.transfer_ix
    ) AS rn
  FROM frame_transfers ft
  JOIN legs l
    ON  l.tx_id      = ft.tx_id
    AND l.block_slot = ft.block_slot
    AND l.outer_ix   = ft.outer_ix
    AND l.solfi_ix   = ft.solfi_ix
  WHERE ft.token_mint_address = l.token_bought_mint_address
    AND ft.from_owner NOT IN (l.trader_id, l.account_user)
),
sold_vault AS (
  SELECT
    ft.tx_id, ft.block_slot, ft.outer_ix, ft.solfi_ix,
    ft.to_token_account AS token_sold_vault,
    ROW_NUMBER() OVER (
      PARTITION BY ft.tx_id, ft.block_slot, ft.outer_ix, ft.solfi_ix
      ORDER BY ft.transfer_ix
    ) AS rn
  FROM frame_transfers ft
  JOIN legs l
    ON  l.tx_id      = ft.tx_id
    AND l.block_slot = ft.block_slot
    AND l.outer_ix   = ft.outer_ix
    AND l.solfi_ix   = ft.solfi_ix
  WHERE ft.token_mint_address = l.token_sold_mint_address
    AND ft.to_owner NOT IN (l.trader_id, l.account_user)
),

-- 6) Attach call metadata and finalize
trades_base AS (
  SELECT
    l.block_time,
    'solfi'  AS project,
    1        AS version,
    'solana' AS blockchain,

    CASE WHEN c.is_inner_swap = FALSE THEN 'direct' ELSE c.outer_exec END AS trade_source,

    l.token_bought_mint_address,
    l.token_bought_amount_raw,
    l.token_sold_mint_address,
    l.token_sold_amount_raw,

    CAST(NULL AS DOUBLE) AS fee_tier,

    c.account_pair AS pool_id,
    'SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe' AS project_main_id,

    l.trader_id,
    l.tx_id,
    l.outer_ix AS outer_instruction_index,
    l.solfi_ix AS inner_instruction_index,
    l.tx_index,
    l.block_slot,

    (SELECT b.token_bought_vault FROM bought_vault b
      WHERE b.tx_id = l.tx_id AND b.block_slot = l.block_slot
        AND b.outer_ix = l.outer_ix AND b.solfi_ix = l.solfi_ix AND b.rn = 1) AS token_bought_vault,

    (SELECT s.token_sold_vault FROM sold_vault s
      WHERE s.tx_id = l.tx_id AND s.block_slot = l.block_slot
        AND s.outer_ix = l.outer_ix AND s.solfi_ix = l.solfi_ix AND s.rn = 1) AS token_sold_vault

  FROM legs l
  JOIN frames c
    ON  c.tx_id      = l.tx_id
    AND c.block_slot = l.block_slot
    AND c.outer_ix   = l.outer_ix
    AND c.solfi_ix   = l.solfi_ix
)

SELECT
    tb.blockchain,
    tb.project,
    tb.version,
    CAST(DATE_TRUNC('month', tb.block_time) AS DATE) AS block_month,
    tb.block_time,
    tb.block_slot,
    tb.trade_source,
    tb.token_bought_amount_raw,
    tb.token_sold_amount_raw,
    tb.fee_tier,
    tb.token_sold_mint_address,
    tb.token_bought_mint_address,
    tb.token_sold_vault,
    tb.token_bought_vault,
    tb.pool_id            AS project_program_id,
    tb.project_main_id,
    tb.trader_id,
    tb.tx_id,
    tb.outer_instruction_index,
    COALESCE(tb.inner_instruction_index, 0) AS inner_instruction_index,
    tb.tx_index,
    {{ dbt_utils.generate_surrogate_key([
        'tx_id',
        'tx_index',
        'outer_instruction_index',
        'inner_instruction_index'
    ]) }} AS surrogate_key
FROM trades_base tb
