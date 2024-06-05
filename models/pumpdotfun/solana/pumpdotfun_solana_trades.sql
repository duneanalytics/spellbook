 {{
  config(
        schema = 'pumpdotfun_solana',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "pumpdotfun_solana",
                                    \'["ilemi"]\') }}')
}}


{% set project_start_date = '2024-01-14' %} --grabbed program deployed at time (account created at)

with 
    bonding_curves as (
        SELECT
            account_arguments[1] as token_mint_address
            , account_arguments[3] as bonding_curve 
            , account_arguments[4] as bonding_curve_vault
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
            AND bytearray_substring(data,1,8) = 0x181ec828051c0777 --Create https://solscan.io/tx/2Vfq4gS9nq2jvpmZSxVXJ3uHGeheENXetwUus6KnhBzFu23Brqbt5EoNiTLds6jr72yZYGJ9YbMDG1BYKMRe3hSQ 
            and tx_success = true
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        -- and block_time >= now() - interval '7' day
        {% endif %}
    )
    
    , swaps as (
        SELECT
            to_base58(bytearray_substring(data,1+16,32)) as token_mint_address
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+16+32,8))) as sol_amount
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+16+32+8,8))) as token_amount
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+16+32+8+8,1))) as is_buy
            , to_base58(bytearray_substring(data,1+16+32+8+8+1,32)) as user
            , bytearray_to_int256(bytearray_reverse(bytearray_substring(data,1+16+32+8+8+1+32,8))) as trade_timestamp
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+16+32+8+8+1+32+8,8))) as sol_reserves
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+16+32+8+8+1+32+8+8,8))) as token_reserves
            , data
            , tx_id
            , tx_index
            , block_time
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , outer_executing_account
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        AND bytearray_substring(data,1,16) = 0xe445a52e51cb9a1dbddb7fd34ee661ee --SwapEvent 
        and tx_success = true
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        -- and block_time >= now() - interval '7' day
        {% endif %}
        -- AND tx_id = '5782i58ZHCKSANgTi3WXL5vjFAWEVJuPMGAr6gjx2qqcRP6LLNRH2g6pb61tk44H7PC9ohFNCUKQVUAMd5vYeUns'
        -- and block_slot = 269850397
        --buy https://solscan.io/tx/5782i58ZHCKSANgTi3WXL5vjFAWEVJuPMGAr6gjx2qqcRP6LLNRH2g6pb61tk44H7PC9ohFNCUKQVUAMd5vYeUns
        --sell https://solscan.io/tx/4thHCu9SX166TP2cnjgwJ7mDSSMn5MBe8xLsTYzRLJmE7jPgwQatXh4ehh3At4xvVcgUefFzzsYVBaVwYyS1bA6v
    )
    
    , trades_base as (
        SELECT 
            sp.block_time
            , 'pumpdotfun' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' then 'direct'
                else sp.outer_executing_account
                end as trade_source
            ,case
                when lower(tk.symbol) > lower(tk_sol.symbol) then concat(tk_sol.symbol, '-', tk.symbol)
                else concat(tk.symbol, '-', tk_sol.symbol)
            end as token_pair
            --bought
            , case when is_buy = 1 then COALESCE(tk.symbol, sp.token_mint_address)
                else COALESCE(tk_sol.symbol, 'So11111111111111111111111111111111111111112') 
                end as token_bought_symbol 
            , case when is_buy = 1 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_bought_mint_address
            , case when is_buy = 1 then token_amount
                else sol_amount
                end as token_bought_amount_raw
            , case when is_buy = 1 then token_amount/pow(10,tk.decimals)
                else sol_amount/pow(10,tk_sol.decimals) 
                end as token_bought_amount
            --sold
            , case when is_buy = 0 then COALESCE(tk.symbol, sp.token_mint_address)
                else COALESCE(tk_sol.symbol, 'So11111111111111111111111111111111111111112') 
                end as token_sold_symbol 
            , case when is_buy = 0 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_sold_mint_address
            , case when is_buy = 0 then token_amount
                else sol_amount
                end as token_sold_amount_raw
            , case when is_buy = 0 then token_amount/pow(10,tk.decimals)
                else sol_amount/pow(10,tk_sol.decimals) 
                end as token_sold_amount
            , sp.sol_amount*0.01 as sol_fee_raw
            , sp.sol_amount/pow(10,tk_sol.decimals)*0.01 as sol_fee
            , cast(bc.bonding_curve as varchar) as pool_id
            , '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' as project_main_id
            , sp.sol_reserves as sol_reserves_raw
            , sp.sol_reserves/pow(10,tk_sol.decimals) as sol_reserves
            , sp.token_reserves as token_reserves_raw
            , sp.token_reserves/pow(10,tk.decimals) as token_reserves
            , sp.user as trader_id
            , sp.tx_id
            , sp.outer_instruction_index
            , sp.inner_instruction_index
            , sp.tx_index
            , sp.block_slot
            , cast(case when is_buy = 1 then bc.bonding_curve --sol is just held on the curve account
                else bonding_curve_vault
                end as varchar) as token_bought_vault
            , cast(case when is_buy = 0 then bc.bonding_curve --sol is just held on the curve account
                else bonding_curve_vault
                end as varchar) as token_sold_vault
        FROM swaps sp
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tk ON tk.token_mint_address = sp.token_mint_address
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tk_sol ON tk_sol.token_mint_address = 'So11111111111111111111111111111111111111112'
        LEFT JOIN bonding_curves bc ON bc.token_mint_address = sp.token_mint_address
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
    , sol_fee_raw
    , sol_fee
    , (case when tb.token_bought_mint_address = 'So11111111111111111111111111111111111111112' then p_bought.price
        else p_sold.price 
        end) * sol_fee as fee_usd --fees are only in sol
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.sol_reserves_raw
    , tb.sol_reserves
    , tb.token_reserves_raw
    , tb.token_reserves
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM trades_base tb
LEFT JOIN {{ ref('prices_usd_forward_fill') }} p_bought ON p_bought.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_bought.minute 
    AND token_bought_mint_address = toBase58(p_bought.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('prices_usd_forward_fill') }} p_sold ON p_sold.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_sold.minute 
    AND token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}