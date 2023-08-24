 {{
  config(
        tags = ['dunesql'],
        schema = 'orca_whirlpool',
        alias = alias('trades'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "orca_whirlpool",
                                    \'["ilemi"]\') }}')
}}

{% set project_start_date = '2022-03-10' %} --grabbed min block time from whirlpool_solana.whirlpool_call_swap

with 
    whirlpools as (
    with 
        fee_updates as (
            SELECT 
                ip.account_whirlpool as whirlpool_id
                , ip.call_block_time as update_time
                , fee.defaultFeeRate as fee_tier --https://docs.orca.so/reference/trading-fees, should track protocol fees too. and rewards.
            FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }} ip
            LEFT JOIN {{ source('whirlpool_solana', 'whirlpool_call_initializeFeeTier') }} fee ON ip.account_feeTier = fee.account_feeTier
            
            UNION all
            
             SELECT 
                account_whirlpool as whirlpool_id
                , call_block_time as update_time
                , feeRate as fee_tier
            FROM {{ source('whirlpool_solana', 'whirlpool_call_setFeeRate') }}
        )
        
    SELECT 
        tkA.symbol as tokenA_symbol
        , tkA.decimals as tokenA_decimals
        , account_tokenMintA as tokenA
        , account_tokenVaultA as tokenAVault
        , tkB.symbol as tokenB_symbol
        , tkB.decimals as tokenB_decimals
        , account_tokenMintB as tokenB
        , account_tokenVaultB as tokenBVault
        , ip.tickSpacing
        , ip.account_whirlpool as whirlpool_id
        , fu.update_time
        , fu.fee_tier
        , ip.call_tx_id as init_tx
    FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }} ip
    LEFT JOIN fee_updates fu ON fu.whirlpool_id = ip.account_whirlpool
    LEFT JOIN {{ ref('tokens_solana_fungible') }} tkA ON tkA.token_mint_address = ip.account_tokenMintA 
    LEFT JOIN {{ ref('tokens_solana_fungible') }} tkB ON tkB.token_mint_address = ip.account_tokenMintB
    )
    
    , all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , 'whirlpool' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            ,case
                when lower(tokenA_symbol) > lower(tokenB_symbol) then concat(tokenB_symbol, '-', tokenA_symbol)
                else concat(tokenA_symbol, '-', tokenB_symbol)
            end as token_pair
            , case when sp.aToB = true then COALESCE(tokenB_symbol, tokenB) 
                else COALESCE(tokenA_symbol, tokenA)
                end as token_bought_symbol 
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_2.amount/COALESCE(pow(10,case when sp.aToB = true then wp.tokenB_decimals else tokenA_decimals end),1) as token_bought_amount
            , case when sp.aToB = true then COALESCE(tokenA_symbol, tokenA)
                else COALESCE(tokenB_symbol, tokenB)
                end as token_sold_symbol
            , tr_1.amount as token_sold_amount_raw
            , tr_1.amount/COALESCE(pow(10,case when sp.aToB = true then wp.tokenA_decimals else tokenB_decimals end),1) as token_sold_amount
            , wp.fee_tier
            , wp.whirlpool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , case when sp.aToB = true then wp.tokenB
                else wp.tokenA
                end as token_bought_mint_address
            , case when sp.aToB = true then wp.tokenA 
                else wp.tokenB
                end as token_sold_mint_address
            , case when sp.aToB = true then wp.tokenBVault
                else wp.tokenAVault
                end as token_bought_vault
            , case when sp.aToB = true then wp.tokenAVault 
                else wp.tokenBVault
                end as token_sold_vault
            , wp.update_time
        FROM {{ source('whirlpool_solana', 'whirlpool_call_swap') }} sp
        INNER JOIN whirlpools wp
            ON sp.account_whirlpool = wp.whirlpool_id 
            AND sp.call_block_time >= wp.update_time
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} tr_1 
            ON tr_1.call_tx_id = sp.call_tx_id 
            AND tr_1.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_1.call_inner_instruction_index = 1) 
                OR (sp.call_is_inner = true AND tr_1.call_inner_instruction_index = sp.call_inner_instruction_index + 1))
            {% if is_incremental() %}
            AND tr_1.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND tr_1.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} tr_2 
            ON tr_2.call_tx_id = sp.call_tx_id 
            AND tr_2.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_2.call_inner_instruction_index = 2)
                OR (sp.call_is_inner = true AND tr_2.call_inner_instruction_index = sp.call_inner_instruction_index + 2))
            {% if is_incremental() %}
            AND tr_2.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND tr_2.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        WHERE 1=1
            {% if is_incremental() %}
            AND sp.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND sp.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
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
    , cast(tb.fee_tier as double)/1000000 as fee_tier
    , cast(tb.fee_tier as double)/1000000 * COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price) as fee_usd
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.whirlpool_id as project_program_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , recent_update
FROM
    (
    SELECT 
        *
        , row_number() OVER (partition by tx_id, outer_instruction_index, inner_instruction_index, tx_index order by update_time desc) as recent_update
    FROM all_swaps
    )
    tb
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_bought.minute 
    AND token_bought_mint_address = toBase58(p_bought.contract_address)
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_sold.minute 
    AND token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
WHERE recent_update = 1 