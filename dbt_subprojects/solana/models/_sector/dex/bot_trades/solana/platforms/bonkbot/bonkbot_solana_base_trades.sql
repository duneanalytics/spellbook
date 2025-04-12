{{ config(
    materialized = 'table',
    schema = 'bonkbot_solana',
    alias = 'base_trades',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set project_start_date = '2025-04-10' %} -- TODO: replace with actual start date (2023-08-17)
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

SELECT
    trades.block_time,
    CAST(date_trunc('day', trades.block_time) AS date) AS block_date,
    CAST(date_trunc('month', trades.block_time) AS date) AS block_month,
    'solana' AS blockchain,
    amount_usd,
    -- TODO: find a more generic solution for this
    IF(
        token_sold_mint_address = '{{wsol_token}}',
        'Buy',
        'Sell'
    ) AS type,
    token_bought_amount,
    token_bought_symbol,
    token_bought_mint_address AS token_bought_address,
    token_sold_amount,
    token_sold_symbol,
    token_sold_mint_address AS token_sold_address,
    -- add fee columns
    fee_payments.amount_usd AS fee_amount_usd,
    fee_payments.amount AS fee_token_amount,
    fee_payments.token_symbol AS fee_token_symbol,
    fee_payments.token_address AS fee_token_address,
    project,
    trades.version,
    token_pair,
    project_program_id AS project_contract_address,
    trader_id AS user,
    trades.tx_id,
    tx_index,
    outer_instruction_index,
    inner_instruction_index
FROM
    {{ ref('dex_solana_trades') }} AS trades
    -- join with fee payment
    JOIN {{ ref('bonkbot_solana_fee_payments_usd') }} AS fee_payments ON (
        trades.tx_id = fee_payments.tx_id
        AND fee_payments.block_time = trades.block_time
        AND fee_payments.index = 1 -- only get the first fee payment per tx
        AND trades.trader_id != fee_payments.fee_receiver
    )
WHERE
    trades.trader_id != '{{fee_receiver}}' -- Exclude trades signed by FeeWallet
    -- TODO: find a efficient solution for this AND transactions.signer != '{{fee_receiver}}' -- Exclude trades signed by FeeWallet
    -- TODO: to filtering for signer in 2nd stage/cte
    {% if is_incremental() %}
    AND {{ incremental_predicate('trades.block_time') }}
    AND {{ incremental_predicate('fee_payments.block_time') }}
    {% else %}
    AND trades.block_time >= TIMESTAMP '{{project_start_date}}'
    AND fee_payments.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %} 
