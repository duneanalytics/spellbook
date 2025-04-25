{{ config(
    schema = 'phantom_swapper_solana',
    alias = 'bot_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set project_start_date = '2024-10-01' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with
    fee_addresses as (
        -- Jupiter AND OKX transfer partner fee directly to a tokenaccount owned by 25mY,9Yj,tzv or 8ps
        SELECT fee_receiver
        FROM (
            VALUES
            ('25mYnjJ2MXHZH6NvTTdA63JvjgRVcuiaj6MRiEQNs1Dq')
            , ('9yj3zvLS3fDMqi1F8zhkaWfq8TZpZWHe6cz1Sgt7djXf')
            , ('tzvXws1qhmfdPkPcprezULCDQPAJqzPhbZ3SMrqRPNE')
            , ('8psNvWTrdNTiVRNzAgsou9kETXNJm2SXZyaKuJraVRtf')
        ) AS x (fee_receiver)
        UNION ALL 

        -- Jupiter transfers partner fee to a ReferralTokenAccount owned by a ReferralAccount owned by 25mY
        SELECT referraltokenaccount AS fee_receiver
        FROM (
            SELECT
            account_arguments[1] AS creator
            , account_arguments[3] AS referralaccount
            , account_arguments[4] AS referraltokenaccount
            FROM {{ source('solana','instruction_calls') }}
            WHERE executing_account = 'REFER4ZgmyYx9c6He5XfaTMiGfdLwRnkV4RPp9t9iF3'
            AND account_arguments[2] = '45ruCyfdRkWpRNGEqWzjCiXRHkZs8WXCLQ67Pnpye7Hp'
            AND TRY_CAST(data AS VARCHAR) like '%7d12465f%'
            AND block_time >= TIMESTAMP '2023-11-30' -- First date phantom ReferralTokenAccount created
            AND block_time < TIMESTAMP '2025-02-12' -- Last date phantom ReferralTokenAccount created
            )
        WHERE referralaccount IN (
            'CnmA6Zb8hLrG33AT4RTzKdGv1vKwRBKQQr8iNckvv8Yg'
            , '2rQZb9xqQGwoCMDkpabbzDB9wyPTjSPj9WNhJodTaRHm'
            , '9gnLg6NtVxaASvxtADLFKZ9s8yHft1jXb1Vu6gVKvh1J'
            , 'wtpXRqKLdGc7vpReogsRugv6EFCw4HBHcxm8pFcR84a'
            , 'D1NJy3Qq3RKBG29EDRj28ozbGwnhmM5yBUp8PonSYUnm'
        )
    ),

    trades as (
        select
            'solana' as blockchain,
            trades.block_time,
            cast(date_trunc('day', trades.block_time) as date) as block_date,
            cast(date_trunc('month', trades.block_time) as date) as block_month,
            trades.amount_usd,
            if(token_sold_mint_address = '{{wsol_token}}', 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_mint_address as token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_mint_address as token_sold_address,
            fee_payments.amount_usd as fee_usd,
            fee_payments.amount as fee_token_amount,
            fee_payments.token_symbol as fee_token_symbol,
            fee_payments.token_address as fee_token_address,
            project,
            trades.version,
            token_pair,
            project_program_id as project_contract_address,
            trader_id as user,
            trades.tx_id,
            tx_index,
            outer_instruction_index,
            inner_instruction_index
        from {{ ref('dex_solana_trades') }} as trades
        join
            {{ ref('phantom_swapper_solana_fee_payments_usd') }} as fee_payments
            on (
                trades.tx_id = fee_payments.tx_id
                and fee_payments.block_time = trades.block_time
                and fee_payments.index = 1  -- only get the first fee payment per tx
                and trades.trader_id != fee_payments.fee_receiver
            )
        join {{ source('solana', 'transactions') }} tx ON trades.tx_id = tx.id 
        left join fee_accounts fa1 ON fa1.fee_receiver = trades.trader_id
        left join fee_accounts fa2 ON fa2.fee_receiver = tx.signer
        where
            fa1.fee_receiver IS NULL -- Exclude trades where FeeWallet is trader
            and fa2.fee_receiver IS NULL -- Exclude transactions signed by FeeWallet 
            {% if is_incremental() %}
                and {{ incremental_predicate('trades.block_time') }}
                and {{ incremental_predicate('fee_payments.block_time') }}
            {% else %}
                and trades.block_time >= timestamp '{{project_start_date}}'
                and fee_payments.block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    highest_inner_instruction_index_for_each_trade as (
        select
            tx_id,
            outer_instruction_index,
            max(inner_instruction_index) as highest_inner_instruction_index
        from trades
        group by tx_id, outer_instruction_index
    )
select
    trades.*,
    if(
        inner_instruction_index = highest_inner_instruction_index, true, false
    ) as is_last_trade_in_transaction
from trades
join
    highest_inner_instruction_index_for_each_trade
    on (
        trades.tx_id = highest_inner_instruction_index_for_each_trade.tx_id
        and trades.outer_instruction_index
        = highest_inner_instruction_index_for_each_trade.outer_instruction_index
    )
order by
    block_time desc,
    tx_index desc,
    outer_instruction_index desc,
    inner_instruction_index desc
