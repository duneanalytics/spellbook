{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , on_schema_change = 'append_new_columns'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2024-06-05' %}

WITH fee_tiers_defaults AS (
    SELECT
          account_feeTier AS fee_tier
        , defaultfeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializeFeeTier', 'whirlpool_call_initialize_fee_tier', [["account_feeTier","account_fee_tier"],["defaultfeeRate","default_fee_rate"]]) }}) decoded_initialize_fee_tier

    UNION ALL

    SELECT
          account_feeTier AS fee_tier
        , defaultfeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_setDefaultFeeRate', 'whirlpool_call_set_default_fee_rate', [["account_feeTier","account_fee_tier"],["defaultFeeRate","default_fee_rate"]]) }}) decoded_set_default_fee_rate
)

-- Adaptive-fee pools (`initializePoolWithAdaptiveFee`) approximate fee_rate via
-- the tier's defaultBaseFeeRate; realized fee varies per swap with volatility.
-- Caveat: `initializeAdaptiveFeeTier` decoder coverage is partial, so most
-- adaptive pools resolve to fee_rate = NULL (trades still flow; fee_tier/fee_usd null).
, adaptive_fee_tiers AS (
    SELECT
          account_adaptiveFeeTier AS adaptive_fee_tier
        , defaultBaseFeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializeAdaptiveFeeTier', 'whirlpool_call_initialize_adaptive_fee_tier', [["account_adaptiveFeeTier","account_adaptive_fee_tier"],["defaultBaseFeeRate","default_base_fee_rate"]]) }}) decoded_initialize_adaptive_fee_tier

    UNION ALL

    SELECT
          account_adaptiveFeeTier AS adaptive_fee_tier
        , defaultBaseFeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_setDefaultBaseFeeRate', 'whirlpool_call_set_default_base_fee_rate', [["account_adaptiveFeeTier","account_adaptive_fee_tier"],["defaultBaseFeeRate","default_base_fee_rate"]]) }}) decoded_set_default_base_fee_rate
)

, fee_updates AS (
    SELECT whirlpool_id, update_time, fee_rate
    FROM (
        SELECT
              fi.account_whirlpool AS whirlpool_id
            , fi.call_block_time AS update_time
            , ftd.fee_rate
            , row_number() OVER (PARTITION BY fi.account_whirlpool ORDER BY ftd.fee_time DESC) AS recent_update
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePool', 'whirlpool_call_initialize_pool', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_feeTier","account_fee_tier"]]) }}) fi
        LEFT JOIN fee_tiers_defaults ftd
            ON ftd.fee_tier = fi.account_feeTier
            AND ftd.fee_time <= fi.call_block_time
    )
    WHERE recent_update = 1

    UNION ALL

    SELECT whirlpool_id, update_time, fee_rate
    FROM (
        SELECT
              fi.account_whirlpool AS whirlpool_id
            , fi.call_block_time AS update_time
            , ftd.fee_rate
            , row_number() OVER (PARTITION BY fi.account_whirlpool ORDER BY ftd.fee_time DESC) AS recent_update
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePoolV2', 'whirlpool_call_initialize_pool_v2', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_feeTier","account_fee_tier"]]) }}) fi
        LEFT JOIN fee_tiers_defaults ftd
            ON ftd.fee_tier = fi.account_feeTier
            AND ftd.fee_time <= fi.call_block_time
    )
    WHERE recent_update = 1

    UNION ALL

    SELECT
          account_whirlpool AS whirlpool_id
        , call_block_time AS update_time
        , feeRate AS fee_rate
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_setFeeRate', 'whirlpool_call_set_fee_rate', [["account_whirlpool","account_whirlpool"],["feeRate","fee_rate"]]) }}) decoded_set_fee_rate

    UNION ALL

    SELECT whirlpool_id, update_time, fee_rate
    FROM (
        SELECT
              ip.account_whirlpool AS whirlpool_id
            , ip.call_block_time AS update_time
            , aft.fee_rate
            , row_number() OVER (PARTITION BY ip.account_whirlpool ORDER BY aft.fee_time DESC) AS recent_update
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePoolWithAdaptiveFee', 'whirlpool_call_initialize_pool_with_adaptive_fee', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_adaptiveFeeTier","account_adaptive_fee_tier"]]) }}) ip
        LEFT JOIN adaptive_fee_tiers aft
            ON aft.adaptive_fee_tier = ip.account_adaptiveFeeTier
            AND aft.fee_time <= ip.call_block_time
    )
    WHERE recent_update = 1
)

, whirlpools AS (
    SELECT
          ip.account_whirlpool AS whirlpool_id
        , ip.account_tokenMintA AS tokenA
        , ip.account_tokenVaultA AS tokenAVault
        , ip.account_tokenMintB AS tokenB
        , ip.account_tokenVaultB AS tokenBVault
        , fu.update_time
        , fu.fee_rate
    FROM (
        SELECT
              account_tokenMintA
            , account_tokenMintB
            , account_tokenVaultA
            , account_tokenVaultB
            , account_whirlpool
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePool', 'whirlpool_call_initialize_pool', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_feeTier","account_fee_tier"]]) }}) decoded_initialize_pool

        UNION ALL

        SELECT
              account_tokenMintA
            , account_tokenMintB
            , account_tokenVaultA
            , account_tokenVaultB
            , account_whirlpool
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePoolV2', 'whirlpool_call_initialize_pool_v2', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_feeTier","account_fee_tier"]]) }}) decoded_initialize_pool_v2

        UNION ALL

        SELECT
              account_tokenMintA
            , account_tokenMintB
            , account_tokenVaultA
            , account_tokenVaultB
            , account_whirlpool
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_initializePoolWithAdaptiveFee', 'whirlpool_call_initialize_pool_with_adaptive_fee', [["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"],["account_whirlpool","account_whirlpool"],["account_adaptiveFeeTier","account_adaptive_fee_tier"]]) }}) decoded_initialize_pool_with_adaptive_fee
    ) ip
    LEFT JOIN fee_updates fu
        ON fu.whirlpool_id = ip.account_whirlpool
)

, two_hop AS (
    {{ orca_whirlpool_two_hop_swaps() }}
)

, decoded_swaps AS (
    -- Both decoded naming conventions; current snake-case rows win on overlap.
    SELECT
          account_whirlpool
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_is_inner
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , account_tokenMintA AS swap_tokenA
        , account_tokenVaultA AS swap_tokenAVault
        , account_tokenMintB AS swap_tokenB
        , account_tokenVaultB AS swap_tokenBVault
        , CAST(NULL AS INTEGER) AS input_transfer_index
        , CAST(NULL AS INTEGER) AS output_transfer_index
    FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_swapV2', 'whirlpool_call_swap_v2', [["account_whirlpool","account_whirlpool"],["account_tokenMintA","account_token_mint_a"],["account_tokenMintB","account_token_mint_b"],["account_tokenVaultA","account_token_vault_a"],["account_tokenVaultB","account_token_vault_b"]], bounded=true) }}) decoded_swap_v2

    UNION ALL

    SELECT * FROM two_hop
)

SELECT
      block_slot
    , block_month
    , block_date
    , block_time
    , inner_instruction_index
    , outer_instruction_index
    , outer_executing_account
    , tx_id
    , tx_signer
    , tx_index
    , whirlpool_id
    , tokenA
    , tokenAVault
    , tokenB
    , tokenBVault
    , fee_rate
    , has_memo
    , surrogate_key
    , input_transfer_index
    , output_transfer_index
FROM (
    SELECT
          sp.call_block_slot AS block_slot
        , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
        , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
        , sp.call_block_time AS block_time
        , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
        , sp.call_outer_instruction_index AS outer_instruction_index
        , sp.call_outer_executing_account AS outer_executing_account
        , sp.call_tx_id AS tx_id
        , sp.call_tx_signer AS tx_signer
        , sp.call_tx_index AS tx_index
        , sp.account_whirlpool AS whirlpool_id
        , COALESCE(sp.swap_tokenA, wp.tokenA) AS tokenA
        , COALESCE(sp.swap_tokenAVault, wp.tokenAVault) AS tokenAVault
        , COALESCE(sp.swap_tokenB, wp.tokenB) AS tokenB
        , COALESCE(sp.swap_tokenBVault, wp.tokenBVault) AS tokenBVault
        , sp.input_transfer_index
        , sp.output_transfer_index
        , wp.fee_rate
        , CASE WHEN memo.tx_id IS NOT NULL THEN true ELSE false END AS has_memo
        , {{ solana_instruction_key(
              'sp.call_block_slot'
            , 'sp.call_tx_index'
            , 'sp.call_outer_instruction_index'
            , 'COALESCE(sp.call_inner_instruction_index, 0)'
          ) }} AS surrogate_key
        , row_number() OVER (
            PARTITION BY sp.call_tx_id, sp.call_outer_instruction_index, COALESCE(sp.call_inner_instruction_index, 0), sp.call_tx_index
            ORDER BY wp.update_time DESC
          ) AS fee_rank
    FROM decoded_swaps sp
    -- Decoded SwapV2 carries its own pool/mint/vault metadata. Decoded initialization
    -- is optional fee enrichment, not a requirement for retaining these trades.
    LEFT JOIN whirlpools wp
        ON sp.account_whirlpool = wp.whirlpool_id
        AND sp.call_block_time >= wp.update_time
    LEFT JOIN {{ source('solana', 'instruction_calls') }} memo
        ON memo.tx_id = sp.call_tx_id
        AND memo.block_slot = sp.call_block_slot
        AND memo.outer_instruction_index = sp.call_outer_instruction_index
        AND ((sp.call_is_inner = false AND memo.inner_instruction_index = 1)
            OR (sp.call_is_inner = true AND memo.inner_instruction_index = sp.call_inner_instruction_index + 1))
        AND memo.executing_account = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr'
        AND memo.executing_account_prefix = 'Me'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('memo.block_time') }}
        {% else -%}
        AND memo.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
    -- Decoded account fields keep swaps independent of pool initialization coverage.
    WHERE sp.swap_tokenA IS NOT NULL OR wp.whirlpool_id IS NOT NULL
)
WHERE fee_rank = 1
