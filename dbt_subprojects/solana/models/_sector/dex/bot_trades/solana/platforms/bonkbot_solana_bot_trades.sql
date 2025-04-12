{{ config(
    alias = 'bot_trades',
    schema = 'bonkbot_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set project_start_date = '2023-08-17' %}
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with
    feepayments as (
        select distinct
            tx_id,
            if(balance_change > 0, 'SOL', 'SPL') as feetokentype,
            if(
                balance_change > 0, balance_change / 1e9, token_balance_change
            ) as fee_token_amount,
            if(
                balance_change > 0, '{{wsol_token}}', token_mint_address
            ) as fee_token_mint_address
        from {{ source('solana','account_activity') }}
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= timestamp '{{project_start_date}}'
            {% endif %}
            and tx_success
            and (
                (
                    address = '{{fee_receiver}}' and balance_change > 0  -- SOL fee payments
                )
                or (
                    token_balance_owner = '{{fee_receiver}}'
                    and token_balance_change > 0  -- SPL fee payments
                )
            )
    ),
    -- Eliminate duplicates (e.g. both SOL + WSOL in a single transaction)
    allfeepayments as (
        select
            tx_id,
            min(feetokentype) as feetokentype,
            sum(fee_token_amount) as fee_token_amount,
            fee_token_mint_address
        from feepayments
        group by tx_id, fee_token_mint_address
    ),
    solfeepayments as (select * from allfeepayments where feetokentype = 'SOL'),
    splfeepayments as (select * from allfeepayments where feetokentype = 'SPL'),
    -- Eliminate duplicates (e.g. both SOL + SPL payment in a single transaction)
    allfeepaymentswithsolpaymentpreferred as (
        select
            coalesce(solfeepayments.tx_id, splfeepayments.tx_id) as tx_id,
            coalesce(
                solfeepayments.feetokentype, splfeepayments.feetokentype
            ) as feetokentype,
            coalesce(
                solfeepayments.fee_token_amount, splfeepayments.fee_token_amount
            ) as fee_token_amount,
            coalesce(
                solfeepayments.fee_token_mint_address,
                splfeepayments.fee_token_mint_address
            ) as fee_token_mint_address
        from solfeepayments
        full join splfeepayments on solfeepayments.tx_id = splfeepayments.tx_id
    ),
    bottrades as (
        select
            trades.block_time,
            cast(date_trunc('day', trades.block_time) as date) as block_date,
            cast(date_trunc('month', trades.block_time) as date) as block_month,
            'solana' as blockchain,
            amount_usd,
            if(token_sold_mint_address = '{{wsol_token}}', 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_mint_address as token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_mint_address as token_sold_address,
            fee_token_amount * price as fee_usd,
            fee_token_amount,
            if(feetokentype = 'SOL', 'SOL', symbol) as fee_token_symbol,
            fee_token_mint_address as fee_token_address,
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
            allfeepaymentswithsolpaymentpreferred as feepayments
            on trades.tx_id = feepayments.tx_id
        left join
            {{ source('prices', 'usd') }} as feetokenprices
            on (
                feetokenprices.blockchain = 'solana'
                and fee_token_mint_address = tobase58(feetokenprices.contract_address)
                and date_trunc('minute', block_time) = minute
                {% if is_incremental() %} and {{ incremental_predicate('minute') }}
                {% else %} and minute >= timestamp '{{project_start_date}}'
                {% endif %}
            )
        join
            {{ source('solana','transactions') }} as transactions
            on (
                trades.tx_id = id
                {% if is_incremental() %}
                    and {{ incremental_predicate('transactions.block_time') }}
                {% else %}
                    and transactions.block_time >= timestamp '{{project_start_date}}'
                {% endif %}
            )
        where
            trades.trader_id != '{{fee_receiver}}'  -- Exclude trades signed by FeeWallet
            and transactions.signer != '{{fee_receiver}}'  -- Exclude trades signed by FeeWallet
            {% if is_incremental() %}
                and {{ incremental_predicate('trades.block_time') }}
            {% else %} and trades.block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    highestinnerinstructionindexforeachtrade as (
        select
            tx_id,
            outer_instruction_index,
            max(inner_instruction_index) as highestinnerinstructionindex
        from bottrades
        group by tx_id, outer_instruction_index
    )
select
    block_time,
    block_date,
    block_month,
    'BonkBot' as bot,
    blockchain,
    amount_usd,
    type,
    token_bought_amount,
    token_bought_symbol,
    token_bought_address,
    token_sold_amount,
    token_sold_symbol,
    token_sold_address,
    fee_usd,
    fee_token_amount,
    fee_token_symbol,
    fee_token_address,
    project,
    version,
    token_pair,
    project_contract_address,
    user,
    bottrades.tx_id,
    tx_index,
    bottrades.outer_instruction_index,
    coalesce(inner_instruction_index, 0) as inner_instruction_index,
    if(
        inner_instruction_index = highestinnerinstructionindex, true, false
    ) as is_last_trade_in_transaction
from bottrades
join
    highestinnerinstructionindexforeachtrade
    on (
        bottrades.tx_id = highestinnerinstructionindexforeachtrade.tx_id
        and bottrades.outer_instruction_index
        = highestinnerinstructionindexforeachtrade.outer_instruction_index
    )
order by
    block_time desc,
    tx_index desc,
    outer_instruction_index desc,
    inner_instruction_index desc
