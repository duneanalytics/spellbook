 {{
  config(
        
        schema = 'raydium_v3',
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
                                    "raydium",
                                    \'["ilemi"]\') }}')
}}


{% set project_start_date = '2022-08-17' %} --grabbed program deployed at time (account created at)

  WITH 
    pools as (
        -- come back for fees some other day after we can tie fee account
        SELECT 
            tkA.symbol as tokenA_symbol
            , tkA.decimals as tokenA_decimals
            , ip.account_tokenMint0 as tokenA
            , ip.account_tokenVault0 as tokenAVault
            , tkB.symbol as tokenB_symbol
            , tkB.decimals as tokenB_decimals
            , ip.account_tokenMint1 as tokenB
            , ip.account_tokenVault1 as tokenBVault
            , ip.account_ammConfig as fee_tier
            , ip.account_poolState as pool_id
            , ip.call_tx_id as init_tx
            , ip.call_block_time as init_time
            , row_number() over (partition by ip.account_poolState order by ip.call_block_time desc) as recent_init
        FROM {{ source('raydium_clmm_solana','amm_v3_call_createPool') }} ip
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tkA ON tkA.token_mint_address = ip.account_tokenMint0
            AND tkA.token_version = 'spl_token'
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tkB ON tkB.token_mint_address = ip.account_tokenMint1
            AND tkB.token_version = 'spl_token'
    )
    
    , all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , 'raydium' as project
            , 3 as version
            , 'solana' as blockchain
            , call_block_slot as block_slot
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            ,case
                when lower(tokenA_symbol) > lower(tokenB_symbol) then concat(tokenB_symbol, '-', tokenA_symbol)
                else concat(tokenA_symbol, '-', tokenB_symbol)
            end as token_pair
            , case when tr_1.token_mint_address = p.tokenA then COALESCE(tokenB_symbol, tokenB) 
                else COALESCE(tokenA_symbol, tokenA)
                end as token_bought_symbol 
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_2.amount/pow(10,COALESCE(case when tr_1.token_mint_address = p.tokenA then p.tokenB_decimals else tokenA_decimals end,9)) as token_bought_amount
            , case when tr_1.token_mint_address = p.tokenA  then COALESCE(tokenA_symbol, tokenA)
                else COALESCE(tokenB_symbol, tokenB)
                end as token_sold_symbol
            , tr_1.amount as token_sold_amount_raw
            , tr_1.amount/pow(10,COALESCE(case when tr_1.token_mint_address = p.tokenA then p.tokenA_decimals else tokenB_decimals end,9)) as token_sold_amount
            , p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , case when tr_1.token_mint_address = p.tokenA then p.tokenB
                else p.tokenA
                end as token_bought_mint_address
            , case when tr_1.token_mint_address = p.tokenA then p.tokenA 
                else p.tokenB
                end as token_sold_mint_address
            , case when tr_1.token_mint_address = p.tokenA then p.tokenBVault
                else p.tokenAVault
                end as token_bought_vault
            , case when tr_1.token_mint_address = p.tokenA then p.tokenAVault 
                else p.tokenBVault
                end as token_sold_vault
        FROM (
            SELECT 
                account_poolState , call_is_inner, call_outer_instruction_index, call_inner_instruction_index, call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer, call_tx_index
            FROM {{ source('raydium_clmm_solana', 'amm_v3_call_swap') }} 
            UNION ALL 
            SELECT 
                account_poolState , call_is_inner, call_outer_instruction_index, call_inner_instruction_index, call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer, call_tx_index
            FROM {{ source('raydium_clmm_solana', 'amm_v3_call_swapV2') }}
        ) sp
        INNER JOIN pools p
            ON sp.account_poolState = p.pool_id --account 2
            and p.recent_init = 1 --for some reason, some pools get created twice.
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_1 
            ON tr_1.tx_id = sp.call_tx_id 
            AND tr_1.outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_1.inner_instruction_index = 1) 
                OR (sp.call_is_inner = true AND tr_1.inner_instruction_index = sp.call_inner_instruction_index + 1))
            AND tr_1.token_version = 'fungible'
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_1.block_time')}}
            {% else %}
            AND tr_1.block_time >= TIMESTAMP '{{project_start_date}}'
            -- AND tr_1.block_time >= now() - interval '7' day
            {% endif %}
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_2 
            ON tr_2.tx_id = sp.call_tx_id 
            AND tr_2.outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_2.inner_instruction_index = 2)
                OR (sp.call_is_inner = true AND tr_2.inner_instruction_index = sp.call_inner_instruction_index + 2))
            AND tr_2.token_version = 'fungible'
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_2.block_time')}}
            {% else %}
            AND tr_2.block_time >= TIMESTAMP '{{project_start_date}}'
            -- AND tr_2.block_time >= now() - interval '7' day
            {% endif %}
        WHERE 1=1
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= TIMESTAMP '{{project_start_date}}'
            -- AND call_block_time >= now() - interval '7' day
            {% endif %}
    )
    
SELECT
    tb.blockchain
    , tb.project 
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.token_pair
    , tb.trade_source
    , tb.token_bought_symbol
    , tb.token_bought_amount
    , tb.token_bought_amount_raw
    , tb.token_sold_symbol
    , tb.token_sold_amount
    , tb.token_sold_amount_raw
    , case when p_sold.price is not null and p_bought.price is not null 
        then least(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price)
        else COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price)
        end as amount_usd
    , cast(null as double) as fee_tier
    , cast(null as double) as fee_usd
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' as project_main_id
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
    -- AND p_bought.minute >= now() - interval '7' day
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_sold.minute 
    AND token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    -- AND p_sold.minute >= now() - interval '7' day
    {% endif %}