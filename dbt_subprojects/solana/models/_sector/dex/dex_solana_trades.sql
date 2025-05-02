{{
  config(
        schema = 'dex_solana',
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "dex_solana",
                                    \'["ilemi","0xRob","jeff-dude"]\') }}')
}}

with base_trades as (
    SELECT
        *
    FROM
        {{ ref('dex_solana_base_trades')}}
    {% if is_incremental() %}
    WHERE
        {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT bt.blockchain
      , bt.project
      , bt.version
      , bt.block_month
      , bt.block_time
      , bt.block_slot
      , bt.trade_source
      , token_bought.symbol as token_bought_symbol
      , token_sold.symbol as token_sold_symbol
      , case when lower(token_bought.symbol) > lower(token_sold.symbol)
            then concat(token_bought.symbol, '-', token_sold.symbol)
            else concat(token_sold.symbol, '-', token_bought.symbol)
        end as token_pair
      , bt.token_bought_amount_raw / pow(10,coalesce(token_bought.decimals, 9)) as token_bought_amount
      , bt.token_sold_amount_raw / pow(10,coalesce(token_sold.decimals, 9)) as token_sold_amount
      , bt.token_bought_amount_raw
      , bt.token_sold_amount_raw
      , COALESCE(
            -- if bought token is trusted, prefer that price, else default to sold token then bought token.
            case when tt_bought.contract_address is not null then
                bt.token_bought_amount_raw / pow(10,coalesce(token_bought.decimals, 9)) * p_bought.price
                else null end
               , bt.token_sold_amount_raw / pow(10,coalesce(token_sold.decimals, 9)) * p_sold.price
               , bt.token_bought_amount_raw / pow(10,coalesce(token_bought.decimals, 9)) * p_bought.price)
            as amount_usd
      , bt.fee_tier
      , bt.fee_tier *
        COALESCE(
            -- if bought token is trusted, prefer that price, else default to sold token then bought token.
            case when tt_bought.contract_address is not null then
                bt.token_bought_amount_raw / pow(10,coalesce(token_bought.decimals, 9)) * p_bought.price
                else null end
               , bt.token_sold_amount_raw / pow(10,coalesce(token_sold.decimals, 9)) * p_sold.price
               , bt.token_bought_amount_raw / pow(10,coalesce(token_bought.decimals, 9)) * p_bought.price)
            as fee_usd
      , bt.token_bought_mint_address
      , bt.token_sold_mint_address
      , bt.token_bought_vault
      , bt.token_sold_vault
      , bt.project_program_id
      , bt.project_main_id
      , bt.trader_id
      , bt.tx_id
      , bt.outer_instruction_index
      , bt.inner_instruction_index
      , bt.tx_index
FROM
    base_trades bt
LEFT JOIN
    {{ ref('tokens_solana_fungible') }} token_bought
    ON token_bought.token_mint_address = bt.token_bought_mint_address
LEFT JOIN 
    {{ ref('tokens_solana_fungible') }} token_sold 
    ON token_sold.token_mint_address = bt.token_sold_mint_address
LEFT JOIN 
    {{ source('prices', 'usd') }} p_bought
    ON p_bought.blockchain = 'solana'
    AND date_trunc('minute', bt.block_time) = p_bought.minute
    AND bt.token_bought_mint_address = toBase58(p_bought.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% endif %}
LEFT JOIN 
    {{ source('prices', 'usd') }} p_sold
    ON p_sold.blockchain = 'solana'
    AND date_trunc('minute', bt.block_time) = p_sold.minute
    AND bt.token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% endif %}
-- if bought token is trusted, prefer that price, else default to sold token then bought token.
LEFT JOIN 
    {{ source('prices','trusted_tokens') }} tt_bought
    ON bt.token_bought_mint_address = toBase58(tt_bought.contract_address)
    AND bt.blockchain = tt_bought.blockchain