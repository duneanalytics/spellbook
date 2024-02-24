 {{
  config(
        schema = 'meteora_v2_solana',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
        )
}}

{% set project_start_date = '2023-11-01' %} --grabbed program deployed at time (account created at).

  WITH
    all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , 'meteora' as project
            , 2 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            , case
                when lower(dec_1.symbol) > lower(dec_2.symbol) then concat(dec_1.symbol, '-', dec_2.symbol)
                else concat(dec_2.symbol, '-', dec_1.symbol)
              end as token_pair
            , COALESCE(dec_2.symbol, dec_2.token_mint_address) as token_bought_symbol 
            -- -- token bought is always the second instruction (transfer) in the inner instructions
            , trs_2.amount as token_bought_amount_raw
            , trs_2.amount/pow(10,COALESCE(dec_2.decimals,9)) as token_bought_amount
            , COALESCE(dec_1.symbol, dec_1.token_mint_address) as token_sold_symbol 
            , trs_1.amount as token_sold_amount_raw
            , trs_1.amount/pow(10,COALESCE(dec_1.decimals,9)) as token_sold_amount
            , account_lbPair as pool_id --p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , COALESCE(trs_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(trs_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , trs_2.from_token_account as token_bought_vault
            , trs_1.to_token_account as token_sold_vault
        FROM {{ source('meteora_solana','lb_clmm_call_swap') }}  sp
        INNER JOIN {{ ref('tokens_solana_transfers') }} trs_1 
            ON trs_1.tx_id = sp.call_tx_id 
            AND trs_1.block_time = sp.call_block_time
            AND trs_1.outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_1.inner_instruction_index = COALESCE(sp.call_inner_instruction_index,0) + 1
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_1.block_time')}}
            {% else %}
            AND trs_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ ref('tokens_solana_transfers') }} trs_2 
            ON trs_2.tx_id = sp.call_tx_id 
            AND trs_2.block_time = sp.call_block_time
            AND trs_2.outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_2.inner_instruction_index = COALESCE(sp.call_inner_instruction_index,0) + 2
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.block_time')}}
            {% else %}
            AND trs_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        LEFT JOIN {{ ref('tokens_solana_fungible') }} dec_1 ON dec_1.token_mint_address = trs_1.token_mint_address
        LEFT JOIN {{ ref('tokens_solana_fungible') }} dec_2 ON dec_2.token_mint_address = trs_2.token_mint_address
        WHERE 1=1
    )
    
SELECT
    tb.blockchain
    , tb.project 
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.token_pair
    , tb.trade_source
    , tb.token_bought_symbol
    , tb.token_bought_amount
    , tb.token_bought_amount_raw
    , tb.token_sold_symbol
    , tb.token_sold_amount
    , tb.token_sold_amount_raw
    , COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price) as amount_usd
    , cast(null as double) as fee_tier
    , cast(null as double) as fee_usd
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_bought.minute 
    AND token_bought_mint_address = toBase58(p_bought.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_sold.minute 
    AND token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}