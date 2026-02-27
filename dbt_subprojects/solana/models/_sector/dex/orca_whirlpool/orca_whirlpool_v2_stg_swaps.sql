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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND call_block_time < TIMESTAMP '2024-06-12'
        {% endif %}

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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND call_block_time < TIMESTAMP '2024-06-12'
        {% endif %}
)

, all_swaps AS (
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
    FROM {{ source('whirlpool_solana', 'whirlpool_call_swapV2') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND call_block_time < TIMESTAMP '2024-06-12'
        {% endif %}

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
        , wp.whirlpool_id
        , wp.tokenA
        , wp.tokenAVault
        , wp.tokenB
        , wp.tokenBVault
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
    FROM all_swaps sp
    INNER JOIN whirlpools wp
        ON sp.account_whirlpool = wp.whirlpool_id
        AND sp.call_block_time >= wp.update_time
    LEFT JOIN {{ source('solana', 'instruction_calls') }} memo
        ON memo.tx_id = sp.call_tx_id
        AND memo.block_slot = sp.call_block_slot
        AND memo.outer_instruction_index = sp.call_outer_instruction_index
        AND ((sp.call_is_inner = false AND memo.inner_instruction_index = 1)
            OR (sp.call_is_inner = true AND memo.inner_instruction_index = sp.call_inner_instruction_index + 1))
        AND memo.executing_account = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr'
        {% if is_incremental() %}
        AND {{ incremental_predicate('memo.block_time') }}
        {% else %}
        AND memo.block_time >= TIMESTAMP '{{ project_start_date }}'
        AND memo.block_time < TIMESTAMP '2024-06-12'
        {% endif %}
)
WHERE fee_rank = 1
