{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2024-06-05' %}

-- TEMP CI ONLY: use the rolling incremental window for initial builds too.
-- Revert this comment and the "or true" guards before merge (see PR #9303).

WITH fee_tiers_defaults AS (
    SELECT
          account_feeTier AS fee_tier
        , defaultfeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM {{ source('whirlpool_solana', 'whirlpool_call_initializeFeeTier') }}

    UNION ALL

    SELECT
          account_feeTier AS fee_tier
        , defaultfeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM {{ source('whirlpool_solana', 'whirlpool_call_setDefaultFeeRate') }}
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
    FROM {{ source('whirlpool_solana', 'whirlpool_call_initializeAdaptiveFeeTier') }}

    UNION ALL

    SELECT
          account_adaptiveFeeTier AS adaptive_fee_tier
        , defaultBaseFeeRate AS fee_rate
        , call_block_time AS fee_time
    FROM {{ source('whirlpool_solana', 'whirlpool_call_setDefaultBaseFeeRate') }}
)

, fee_updates AS (
    SELECT whirlpool_id, update_time, fee_rate
    FROM (
        SELECT
              fi.account_whirlpool AS whirlpool_id
            , fi.call_block_time AS update_time
            , ftd.fee_rate
            , row_number() OVER (PARTITION BY fi.account_whirlpool ORDER BY ftd.fee_time DESC) AS recent_update
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }} fi
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
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolV2') }} fi
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
    FROM {{ source('whirlpool_solana', 'whirlpool_call_setFeeRate') }}

    UNION ALL

    SELECT whirlpool_id, update_time, fee_rate
    FROM (
        SELECT
              ip.account_whirlpool AS whirlpool_id
            , ip.call_block_time AS update_time
            , aft.fee_rate
            , row_number() OVER (PARTITION BY ip.account_whirlpool ORDER BY aft.fee_time DESC) AS recent_update
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolWithAdaptiveFee') }} ip
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
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }}

        UNION ALL

        SELECT
              account_tokenMintA
            , account_tokenMintB
            , account_tokenVaultA
            , account_tokenVaultB
            , account_whirlpool
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolV2') }}

        UNION ALL

        SELECT
              account_tokenMintA
            , account_tokenMintB
            , account_tokenVaultA
            , account_tokenVaultB
            , account_whirlpool
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolWithAdaptiveFee') }}
    ) ip
    LEFT JOIN fee_updates fu
        ON fu.whirlpool_id = ip.account_whirlpool
)

, two_hop AS (
    SELECT
          account_whirlpoolOne AS account_whirlpool
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_is_inner
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
    FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwapV2') }}
    WHERE 1=1
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}

    UNION ALL

    SELECT
          account_whirlpoolTwo AS account_whirlpool
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) + 2 AS call_inner_instruction_index
        , true AS call_is_inner
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
    FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwapV2') }}
    WHERE 1=1
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

, raw_swaps AS (
    -- Read SwapV2 directly: successful calls with remaining accounts can be
    -- absent from the decoded source. The Whirlpool account is always fifth.
    SELECT
          account_arguments[5] AS account_whirlpool
        , outer_instruction_index AS call_outer_instruction_index
        , inner_instruction_index AS call_inner_instruction_index
        , is_inner AS call_is_inner
        , tx_signer AS call_tx_signer
        , tx_id AS call_tx_id
        , tx_index AS call_tx_index
        , block_time AS call_block_time
        , block_slot AS call_block_slot
        , outer_executing_account AS call_outer_executing_account
        , account_arguments[6] AS raw_tokenA
        , account_arguments[9] AS raw_tokenAVault
        , account_arguments[7] AS raw_tokenB
        , account_arguments[11] AS raw_tokenBVault
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE 1=1
        AND executing_account_prefix = 'wh'
        AND executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND tx_success = true
        AND bytearray_substring(data, 1, 8) = 0x2b04ed0b1ac91e62
        AND cardinality(account_arguments) >= 15
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('block_time') }}
        {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}

    UNION ALL

    SELECT
          two_hop.*
        , CAST(NULL AS VARCHAR) AS raw_tokenA
        , CAST(NULL AS VARCHAR) AS raw_tokenAVault
        , CAST(NULL AS VARCHAR) AS raw_tokenB
        , CAST(NULL AS VARCHAR) AS raw_tokenBVault
    FROM two_hop
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
        , COALESCE(sp.raw_tokenA, wp.tokenA) AS tokenA
        , COALESCE(sp.raw_tokenAVault, wp.tokenAVault) AS tokenAVault
        , COALESCE(sp.raw_tokenB, wp.tokenB) AS tokenB
        , COALESCE(sp.raw_tokenBVault, wp.tokenBVault) AS tokenBVault
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
    FROM raw_swaps sp
    -- SwapV2 carries its own pool/mint/vault metadata. Decoded initialization
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
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('memo.block_time') }}
        {% else -%}
        AND memo.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
    -- Preserve the existing decoded two-hop behavior when pool metadata is absent.
    WHERE sp.raw_tokenA IS NOT NULL OR wp.whirlpool_id IS NOT NULL
)
WHERE fee_rank = 1
