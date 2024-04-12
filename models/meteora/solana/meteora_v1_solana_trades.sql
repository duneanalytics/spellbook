 {{
  config(
        schema = 'meteora_v1_solana',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
        )
}}

{% set project_start_date = '2021-03-21' %} --grabbed program deployed at time (account created at).

  WITH
    pools as (
        --technically not used below, but here for a dex_solana.pools spell later on.
        SELECT 
            tkA.symbol as tokenA_symbol
            , tkA.decimals as tokenA_decimals
            , ip.account_tokenAMint as tokenA
            , ip.account_aTokenVault as tokenAVault
            , tkB.symbol as tokenB_symbol
            , tkB.decimals as tokenB_decimals
            , ip.account_tokenAMint as tokenB
            , ip.account_bTokenVault as tokenBVault
            , cast(null as varchar) as fee_tier
            , ip.account_pool as pool_id
            , ip.call_tx_id as init_tx
            , ip.call_block_time as init_time
        FROM {{ source('meteora_pools_solana', 'amm_call_initialize') }} ip 
        LEFT JOIN {{ ref('tokens_solana_fungible') }}  tkA ON tkA.token_mint_address = ip.account_tokenAMint
        LEFT JOIN {{ ref('tokens_solana_fungible') }}  tkB ON tkB.token_mint_address = ip.account_tokenBMint
        --force
    )

    , all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , 'meteora' as project
            , 1 as version
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
            , account_pool as pool_id --p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , COALESCE(tk_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(tk_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , trs_2.account_source as token_bought_vault
            , trs_1.account_destination as token_sold_vault
        FROM (
            SELECT 
                sp.*
                , dp.call_inner_instruction_index as deposit_index
                , row_number() over (partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index order by dp.call_inner_instruction_index asc) as first_deposit
            FROM {{ source('meteora_pools_solana', 'amm_call_swap') }} sp
            LEFT JOIN {{ source('meteora_vault_solana', 'vault_call_deposit') }} dp ON sp.call_tx_id = dp.call_tx_id 
                AND sp.call_block_slot = dp.call_block_slot
                AND sp.call_outer_instruction_index = dp.call_outer_instruction_index 
                and COALESCE(sp.call_inner_instruction_index, 0) < dp.call_inner_instruction_index
                {% if is_incremental() %}
                AND {{incremental_predicate('dp.call_block_time')}}
                {% else %}
                AND dp.call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
            WHERE 1=1 
                {% if is_incremental() %}
                AND {{incremental_predicate('sp.call_block_time')}}
                {% else %}
                AND sp.call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
        ) sp
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} trs_1 
            ON trs_1.call_tx_id = sp.call_tx_id 
            AND trs_1.call_block_time = sp.call_block_time
            AND trs_1.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_1.call_inner_instruction_index = sp.deposit_index + 1
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_1.call_block_time')}}
            {% else %}
            AND trs_1.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} trs_2 
            ON trs_2.call_tx_id = sp.call_tx_id 
            AND trs_2.call_block_time = sp.call_block_time
            AND trs_2.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_2.call_inner_instruction_index = sp.deposit_index + 4
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.call_block_time')}}
            {% else %}
            AND trs_2.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        --we want to get what token was transfered out first as this is the sold token. THIS MUST BE THE DESTINATION account, the source account is commonly created/closed through swap legs.
        LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_1 ON tk_1.address = trs_1.account_destination
        LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_2 ON tk_2.address = trs_2.account_source
        LEFT JOIN {{ ref('tokens_solana_fungible') }} dec_1 ON dec_1.token_mint_address = tk_1.token_mint_address
        LEFT JOIN {{ ref('tokens_solana_fungible') }} dec_2 ON dec_2.token_mint_address = tk_2.token_mint_address
        WHERE 1=1
        and first_deposit = 1 --keep only the first deposit after swap invoke
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