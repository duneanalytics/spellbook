 {{
  config(

        schema = 'orca_whirlpool_v2',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}

{% set project_start_date = '2024-05-27' %} --blocktime of upgrade is 268396603
--you can QA with the tx id 2P2hcK79CV82MqjXa6S4xxKoTHkgqmYkszWkxY1KmASvCek8YfSekSrB8rtdRJSTUYYyuCVxMXFrQd11bXDZqn6K

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
                FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolV2') }} fi
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
        account_tokenMintA as tokenA
        , account_tokenVaultA as tokenAVault
        , account_tokenMintB as tokenB
        , account_tokenVaultB as tokenBVault
        , ip.tickSpacing
        , ip.account_whirlpool as whirlpool_id
        , fu.update_time
        , fu.fee_rate
        , ip.call_tx_id as init_tx
    FROM (
        SELECT
        account_tokenMintA
        , account_tokenMintB
        , account_tokenVaultA
        , account_tokenVaultB
        , account_whirlpool
        , tickSpacing
        , call_tx_id
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePool') }}
        
        UNION ALL 
        
        SELECT
        account_tokenMintA
        , account_tokenMintB
        , account_tokenVaultA
        , account_tokenVaultB
        , account_whirlpool
        , tickSpacing
        , call_tx_id
        FROM {{ source('whirlpool_solana', 'whirlpool_call_initializePoolV2') }}
        ) ip
    LEFT JOIN fee_updates fu ON fu.whirlpool_id = ip.account_whirlpool
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
        FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwapV2') }} sp

        UNION ALL

        --for second hop, we're going to spoof things so that the join on +1 and +2 still work just fine.
            --this might not work in our token extension case since +2 is not guaranteed
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
        FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwapV2') }} sp
    )

    , all_swaps as (
        SELECT
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'whirlpool' as project
            , 2 as version
            , 'solana' as blockchain
            , case when sp.call_outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_1.amount as token_sold_amount_raw
            , wp.fee_rate
            , wp.whirlpool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , case when tr_1.token_mint_address = wp.tokenA then wp.tokenB
                else wp.tokenA
                end as token_bought_mint_address
            , case when tr_1.token_mint_address = wp.tokenA then wp.tokenA
                else wp.tokenB
                end as token_sold_mint_address
            , case when tr_1.token_mint_address = wp.tokenA then wp.tokenBVault
                else wp.tokenAVault
                end as token_bought_vault
            , case when tr_1.token_mint_address = wp.tokenA
                then wp.tokenAVault
                else wp.tokenBVault
                end as token_sold_vault
            , wp.update_time
            --we need a row_number for the transfer out filter to get the first transfer out from the tr_2 join
            , row_number() over (partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index
                                order by COALESCE(tr_2.inner_instruction_index, 0) asc) as first_transfer_out
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
                FROM {{ source('whirlpool_solana', 'whirlpool_call_swapV2') }}
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
        --join on memo program at index + 1 to see if the first token has memo extension active or not. if active everything gets offset by 1
        LEFT JOIN {{ source('solana', 'instruction_calls')}} memo 
            ON memo.tx_id = sp.call_tx_id
            AND memo.block_slot = sp.call_block_slot
            AND memo.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND memo.inner_instruction_index = 1)
                OR (sp.call_is_inner = true AND memo.inner_instruction_index = sp.call_inner_instruction_index + 1)
                )
            AND memo.executing_account = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr'
            {% if is_incremental() %}
            AND {{incremental_predicate('memo.block_time')}}
            {% else %}
            AND memo.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        --token extension transfer could be preceeded by the memo program and followed by a post execution hook + even potentially more instructions.
        INNER JOIN {{ source('tokens_solana','transfers') }} tr_1
            ON tr_1.tx_id = sp.call_tx_id
            AND tr_1.block_slot = sp.call_block_slot
            AND tr_1.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND tr_1.inner_instruction_index = CASE WHEN memo.tx_id IS NOT NULL THEN 2 ELSE 1 END)
                OR (sp.call_is_inner = true AND tr_1.inner_instruction_index = sp.call_inner_instruction_index + CASE WHEN memo.tx_id IS NOT NULL THEN 2 ELSE 1 END))       
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_1.block_time')}}
            {% else %}
            AND tr_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ source('tokens_solana','transfers') }} tr_2
            ON tr_2.tx_id = sp.call_tx_id
            AND tr_2.block_slot = sp.call_block_slot
            AND tr_2.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND (tr_2.inner_instruction_index >= CASE WHEN memo.tx_id IS NOT NULL THEN 3 ELSE 2 END))
                OR (sp.call_is_inner = true AND (tr_2.inner_instruction_index >= sp.call_inner_instruction_index + CASE WHEN memo.tx_id IS NOT NULL THEN 3 ELSE 2 END))
                )
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_2.block_time')}}
            {% else %}
            AND tr_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    )

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , cast(tb.fee_rate as double)/1000000 as fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.whirlpool_id as project_program_id
    , 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' as project_main_id
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
    WHERE first_transfer_out = 1
    )
    tb
WHERE recent_update = 1
