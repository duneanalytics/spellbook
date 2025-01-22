{% set blockchain = 'solana' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'sandwiched',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_id', 'outer_instruction_index', 'inner_instruction_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

WITH sandwiches AS (
    SELECT
        *
    FROM
        {{ ref('dex_solana_sandwiches') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
),
sandwich_bounds AS (
    SELECT
        front.block_month
        , front.block_time
        , front.tx_id AS min_tx_id
        , back.tx_id AS max_tx_id
        , front.project_program_id    
        , front.token_bought_mint_address
        , front.token_sold_mint_address
    FROM sandwiches as front
    INNER JOIN sandwiches as back
        ON front.block_month=back.block_month
        AND front.block_time=back.block_time
        AND front.trader_id=back.trader_id
        AND front.project_program_id=back.project_program_id
        AND front.token_sold_mint_address=back.token_bought_mint_address
        AND front.token_bought_mint_address=back.token_sold_mint_address
        AND front.tx_index+1 < back.tx_index
), dex AS (
    SELECT
        *
    FROM
        {{ ref('dex_solana_trades') }}
    WHERE
        blockchain='{{blockchain}}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
)
SELECT DISTINCT
    dt.blockchain
    , dt.project
    , dt.version
    , dt.block_time
    , dt.block_slot
    , dt.block_month
    , dt.token_sold_mint_address
    , dt.token_bought_mint_address
    , dt.token_sold_vault
    , dt.token_bought_vault
    , dt.token_sold_symbol
    , dt.token_bought_symbol
    , dt.trader_id
    , dt.tx_id
    , dt.token_pair
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
    , dt.tx_index
FROM dex as dt
INNER JOIN sandwich_bounds as sb
    ON sb.block_month=dt.block_month
    AND sb.block_time=dt.block_time
    AND sb.project_program_id=dt.project_program_id
    AND sb.token_bought_mint_address=dt.token_bought_mint_address
    AND sb.token_sold_mint_address=dt.token_sold_mint_address
    AND dt.tx_id BETWEEN sb.min_tx_id AND sb.max_tx_id