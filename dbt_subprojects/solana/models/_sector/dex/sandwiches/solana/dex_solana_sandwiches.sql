{% set blockchain = 'solana' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_id', 'outer_instruction_index', 'inner_instruction_index']
        )
}}

WITH dex AS (
    SELECT
        *
    FROM
        {{ ref('dex_solana_trades') }}
    WHERE
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
)
, indexed_sandwich_trades AS (
    -- Checking that each frontrun trade has a matching backrun and at least one victim in between
    SELECT DISTINCT
        front.block_month
        , front.block_time
        , t.tx_id_unwrapped AS tx_id
        , front.project_program_id
    FROM dex as front
    INNER JOIN dex as back
        ON front.block_month=back.block_month
        AND front.block_time=back.block_time
        AND front.project_program_id=back.project_program_id
        AND front.trader_id=back.trader_id
        AND front.tx_index + 1 < back.tx_index
        AND front.token_sold_mint_address=back.token_bought_mint_address
        AND front.token_bought_mint_address=back.token_sold_mint_address
        {% if is_incremental() %}
        AND {{ incremental_predicate('back.block_time') }}
        {% endif %}
    INNER JOIN dex as victim
        ON front.block_month=victim.block_month
        AND front.block_time=victim.block_time
        AND front.project_program_id=victim.project_program_id
        AND victim.trader_id BETWEEN front.trader_id AND back.trader_id
        AND front.token_bought_mint_address=victim.token_bought_mint_address
        AND front.token_sold_mint_address=victim.token_sold_mint_address
        {% if is_incremental() %}
        AND {{ incremental_predicate('victim.block_time') }}
        {% endif %}
    CROSS JOIN UNNEST(ARRAY[front.tx_id, back.tx_id]) AS t(tx_id_unwrapped)
    WHERE 
        1 = 1
        {% if is_incremental() %}
        AND {{ incremental_predicate('front.block_time') }}
        {% endif %}
)

-- Joining back with dex.trades to get the rest of the data & adding block_number and tx_index to the mix
SELECT
    dt.blockchain
    , dt.project
    , dt.version
    , dt.block_time
    , dt.block_month
    , dt.block_slot
    , dt.token_sold_mint_address
    , dt.token_bought_mint_address
    , dt.token_sold_vault
    , dt.token_bought_vault
    , dt.token_sold_symbol
    , dt.token_bought_symbol
    , dt.trader_id
    , dt.tx_id
    , dt.token_pair
    , dt.tx_index
    , dt.token_sold_amount_raw
    , dt.token_bought_amount_raw
    , dt.token_sold_amount
    , dt.token_bought_amount
    , dt.amount_usd
    , dt.fee_tier
    , dt.fee_usd
    , dt.project_program_id
    , dt.outer_instruction_index
    , dt.inner_instruction_index
    , dt.trade_source
FROM dex as dt
INNER JOIN indexed_sandwich_trades as s
    ON dt.block_month=s.block_month
    AND dt.block_time=s.block_time
    AND dt.tx_id=s.tx_id
    AND dt.project_program_id=s.project_program_id
WHERE
    1 = 1
    {% if is_incremental() %}
    AND {{ incremental_predicate('dt.block_time') }}
    {% endif %}