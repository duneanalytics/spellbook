 {{
  config(
        
        schema = 'orca_whirlpool',
        alias = 'trades',
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
        fee_tiers_defaults as (
            --the fee tier has defaults that can be changed. Once the pool is initialized, the fee tier is set to the default fee tier.
            SELECT 
            account_feeTier as fee_tier
            , defaultfeeRate as fee_rate
            , call_block_time as fee_time
            FROM {{ source('whirlpool_solana', 'whirlpool_call_initializeFeeTier') }}

            UNION ALL 

            SELECT
            account_feeTier as fee_tier
            , defaultfeeRate as fee_rate
            , call_block_time as fee_time
            FROM {{ source('whirlpool_solana', 'whirlpool_call_setDefaultFeeRate') }}
        )

        --https://docs.orca.so/reference/trading-fees, should track protocol fees too. and rewards.
        , fee_updates as (
            SELECT 
                whirlpool_id
                , update_time
                , fee_rate
            FROM (
                --get defaultFeeRate at time of pool init based on account_feeTier
                SELECT 
                    fi.account_whirlpool as whirlpool_id
                    , fi.call_block_time as update_time
                    , fi.account_feeTier as fee_tier
                    , ftd.fee_time
                    , ftd.fee_rate
                    , row_number() over (partition by fi.account_whirlpool order by ftd.fee_time desc) as recent_update
                FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }} fi
                LEFT JOIN fee_tiers_defaults ftd ON ftd.fee_tier = account_feeTier AND ftd.fee_time <= fi.call_block_time
            )
            WHERE recent_update = 1
            
            UNION all
            
            --after being initialized, the fee rate can be set manually using setFeeRate on the pool (does not update with defaultFeeRate)
            SELECT 
                account_whirlpool as whirlpool_id
                , call_block_time as update_time
                , feeRate as fee_rate
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
        , fu.fee_rate
        , ip.call_tx_id as init_tx
    FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }} ip
    LEFT JOIN fee_updates fu ON fu.whirlpool_id = ip.account_whirlpool
    LEFT JOIN {{ ref('tokens_solana_fungible') }} tkA ON tkA.token_mint_address = ip.account_tokenMintA 
    LEFT JOIN {{ ref('tokens_solana_fungible') }} tkB ON tkB.token_mint_address = ip.account_tokenMintB
    )

    , two_hop as (
        SELECT
            account_whirlpoolOne as account_whirlpool
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_is_inner
            , call_tx_signer
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
        FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwap') }} sp
        
        UNION ALL
        
        --for second hop, we're going to spoof things so that the join on +1 and +2 still work just fine. 
        SELECT
            account_whirlpoolTwo as account_whirlpool
            , call_outer_instruction_index
            , COALESCE(call_inner_instruction_index,0) + 2 as call_inner_instruction_index
            , true as call_is_inner
            , call_tx_signer
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
        FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwap') }} sp
    )
    
    , all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , 'whirlpool' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.call_outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            ,case
                when lower(tokenA_symbol) > lower(tokenB_symbol) then concat(tokenB_symbol, '-', tokenA_symbol)
                else concat(tokenA_symbol, '-', tokenB_symbol)
            end as token_pair
            , case when tk_1.token_mint_address = wp.tokenA then COALESCE(tokenB_symbol, tokenB) 
                else COALESCE(tokenA_symbol, tokenA)
                end as token_bought_symbol 
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_2.amount/pow(10,case when tk_1.token_mint_address = wp.tokenA then wp.tokenB_decimals else tokenA_decimals end) as token_bought_amount
            , case when tk_1.token_mint_address = wp.tokenA then COALESCE(tokenA_symbol, tokenA)
                else COALESCE(tokenB_symbol, tokenB)
                end as token_sold_symbol
            , tr_1.amount as token_sold_amount_raw
            , tr_1.amount/pow(10,case when tk_1.token_mint_address = wp.tokenA then wp.tokenA_decimals else tokenB_decimals end) as token_sold_amount
            , wp.fee_rate
            , wp.whirlpool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , case when tk_1.token_mint_address = wp.tokenA then wp.tokenB
                else wp.tokenA
                end as token_bought_mint_address
            , case when tk_1.token_mint_address = wp.tokenA then wp.tokenA 
                else wp.tokenB
                end as token_sold_mint_address
            , case when tk_1.token_mint_address = wp.tokenA then wp.tokenBVault
                else wp.tokenAVault
                end as token_bought_vault
            , case when tk_1.token_mint_address = wp.tokenA
                then wp.tokenAVault 
                else wp.tokenBVault
                end as token_sold_vault
            , wp.update_time
        FROM (
                SELECT 
                    account_whirlpool
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_is_inner
                    , call_tx_signer
                    , call_tx_id
                    , call_tx_index
                    , call_block_time
                    , call_block_slot
                    , call_outer_executing_account    
                FROM {{ source('whirlpool_solana', 'whirlpool_call_swap') }} 
                WHERE 1=1
                {% if is_incremental() %}
                AND {{incremental_predicate('call_block_time')}}
                {% else %}
                AND call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
                
                UNION ALL 
                
                SELECT * FROM two_hop
                WHERE 1=1
                {% if is_incremental() %}
                AND {{incremental_predicate('call_block_time')}}
                {% else %}
                AND call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
            )
            sp
        INNER JOIN whirlpools wp
            ON sp.account_whirlpool = wp.whirlpool_id 
            AND sp.call_block_time >= wp.update_time
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} tr_1 
            ON tr_1.call_tx_id = sp.call_tx_id 
            AND tr_1.call_block_slot = sp.call_block_slot
            AND tr_1.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_1.call_inner_instruction_index = 1) 
                OR (sp.call_is_inner = true AND tr_1.call_inner_instruction_index = sp.call_inner_instruction_index + 1))
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_1.call_block_time')}}
            {% else %}
            AND tr_1.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ source('spl_token_solana', 'spl_token_call_transfer') }} tr_2
            ON tr_2.call_tx_id = sp.call_tx_id 
            AND tr_2.call_block_slot = sp.call_block_slot
            AND tr_2.call_outer_instruction_index = sp.call_outer_instruction_index 
            AND ((sp.call_is_inner = false AND tr_2.call_inner_instruction_index = 2) 
                OR (sp.call_is_inner = true AND tr_2.call_inner_instruction_index = sp.call_inner_instruction_index + 2))
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_2.call_block_time')}}
            {% else %}
            AND tr_2.call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_1 ON tk_1.address = tr_1.account_destination
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
    , cast(tb.fee_rate as double)/1000000 as fee_tier
    , cast(tb.fee_rate as double)/1000000 * COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price) as fee_usd
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
FROM (
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