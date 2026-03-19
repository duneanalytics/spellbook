{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
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
        {% if is_incremental() -%}
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
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

, raw_swaps AS (
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
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}

    UNION ALL

    SELECT * FROM two_hop
)

, swaps AS (
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
        FROM raw_swaps sp
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
            {% if is_incremental() -%}
            AND {{ incremental_predicate('memo.block_time') }}
            {% else -%}
            AND memo.block_time >= TIMESTAMP '{{ project_start_date }}'
            {% endif -%}
    )
    WHERE fee_rank = 1
)

, transfers AS (
    SELECT
          tf.tx_id
        , tf.block_date
        , tf.block_slot
        , tf.outer_instruction_index
        , tf.inner_instruction_index
        , tf.amount
        , tf.token_mint_address
    FROM {{ ref('orca_whirlpool_v2_token_transfers') }} tf
    WHERE 1=1
    {% if is_incremental() -%}
        AND {{ incremental_predicate('tf.block_date') }}
    {% else -%}
        AND tf.block_date >= DATE '{{ project_start_date }}'
    {% endif -%}
)

, swap_transfers AS (
    SELECT
          sp.block_time
        , sp.block_slot
        , sp.block_month
        , sp.surrogate_key
        , CASE
            WHEN sp.outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'direct'
            ELSE sp.outer_executing_account
          END AS trade_source
        , trs_2.amount AS token_bought_amount_raw
        , trs_1.amount AS token_sold_amount_raw
        , trs_1.token_mint_address AS sold_token_mint
        , sp.whirlpool_id
        , sp.tokenA
        , sp.tokenAVault
        , sp.tokenB
        , sp.tokenBVault
        , sp.fee_rate
        , sp.tx_signer AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , row_number() OVER (
            PARTITION BY sp.tx_id, sp.outer_instruction_index, sp.inner_instruction_index
            ORDER BY trs_2.inner_instruction_index ASC
          ) AS first_transfer_out
    FROM swaps sp
    INNER JOIN transfers trs_1
        ON trs_1.tx_id = sp.tx_id
        AND trs_1.block_date = sp.block_date
        AND trs_1.block_slot = sp.block_slot
        AND trs_1.outer_instruction_index = sp.outer_instruction_index
        AND trs_1.inner_instruction_index = CASE WHEN sp.has_memo THEN sp.inner_instruction_index + 2 ELSE sp.inner_instruction_index + 1 END
    INNER JOIN transfers trs_2
        ON trs_2.tx_id = sp.tx_id
        AND trs_2.block_date = sp.block_date
        AND trs_2.block_slot = sp.block_slot
        AND trs_2.outer_instruction_index = sp.outer_instruction_index
        AND trs_2.inner_instruction_index >= CASE WHEN sp.has_memo THEN sp.inner_instruction_index + 3 ELSE sp.inner_instruction_index + 2 END
        AND trs_2.token_mint_address = CASE WHEN trs_1.token_mint_address = sp.tokenA THEN sp.tokenB ELSE sp.tokenA END
)

SELECT
      'solana' AS blockchain
    , 'whirlpool' AS project
    , 2 AS version
    , tb.block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , CAST(tb.fee_rate AS DOUBLE) / 1000000 AS fee_tier
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenA ELSE tb.tokenB END AS token_sold_mint_address
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenB ELSE tb.tokenA END AS token_bought_mint_address
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenAVault ELSE tb.tokenBVault END AS token_sold_vault
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenBVault ELSE tb.tokenAVault END AS token_bought_vault
    , tb.whirlpool_id AS project_program_id
    , 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
    , 1 AS recent_update
FROM swap_transfers tb
WHERE tb.first_transfer_out = 1
