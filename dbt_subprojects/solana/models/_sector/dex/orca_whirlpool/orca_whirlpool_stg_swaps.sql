{{
  config(
    schema = 'orca_whirlpool'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2022-03-10' %}

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
    SELECT
          whirlpool_id
        , update_time
        , fee_rate
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

    SELECT
          whirlpool_id
        , update_time
        , fee_rate
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
        , fu.update_time
        , fu.fee_rate
    FROM (
        SELECT account_whirlpool
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }}
        WHERE account_whirlpoolsConfig = '2LecshUwdy9xi7meFgHtFJQNSKk4KdTrcpvaB56dP2NQ'

        UNION ALL

        SELECT account_whirlpool
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolV2') }}
        WHERE account_whirlpoolsConfig = '2LecshUwdy9xi7meFgHtFJQNSKk4KdTrcpvaB56dP2NQ'
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
    FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwap') }}

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
    FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwap') }}
)

, all_swaps AS (
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
        , wp.whirlpool_id AS pool_id
        , wp.fee_rate
        , wp.update_time
        , {{ solana_instruction_key(
              'sp.call_block_slot'
            , 'sp.call_tx_index'
            , 'sp.call_outer_instruction_index'
            , 'COALESCE(sp.call_inner_instruction_index, 0)'
          ) }} AS surrogate_key
    FROM (
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
        FROM {{ source('whirlpool_solana', 'whirlpool_call_swap') }}
        WHERE 1=1
            {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
            {% else %}
            AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
            {% endif %}

        UNION ALL

        SELECT * FROM two_hop
        WHERE 1=1
            {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
            {% else %}
            AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
            {% endif %}
    ) sp
    INNER JOIN whirlpools wp
        ON sp.account_whirlpool = wp.whirlpool_id
        AND sp.call_block_time >= wp.update_time
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
    , pool_id
    , fee_rate
    , surrogate_key
FROM (
    SELECT *
        , row_number() OVER (
            PARTITION BY tx_id, outer_instruction_index, inner_instruction_index, tx_index
            ORDER BY update_time DESC
          ) AS recent_update
    FROM all_swaps
)
WHERE recent_update = 1
