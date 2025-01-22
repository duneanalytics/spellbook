 {{
  config(
        schema = 'pumpdotfun_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}')
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
            --bought
            , case when is_buy = 1 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_bought_mint_address
            , case when is_buy = 1 then token_amount
                else sol_amount
                end as token_bought_amount_raw
            --sold
            , case when is_buy = 0 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_sold_mint_address
            , case when is_buy = 0 then token_amount
                else sol_amount
                end as token_sold_amount_raw
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
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , cast(0.01 as double) as fee_tier
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
