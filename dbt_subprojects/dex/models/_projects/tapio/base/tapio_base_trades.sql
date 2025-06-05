{{ config(
    schema = 'tapio_base',
    alias = 'trades'
) }}

SELECT
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_symbol,
    token_sold_symbol,
    token_pair,
    token_bought_amount,
    token_sold_amount,
    token_bought_amount_raw,
    token_sold_amount_raw,
    amount_usd,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    pool_id,
    swap_fee,
    project_contract_address,
    pool_symbol,
    pool_type,
    tx_hash,
    tx_from,
    tx_to,
    evt_index
FROM {{ ref('tapio_base_trades') }}