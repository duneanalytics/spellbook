{{
    config(
        alias="bot_trades",
        schema="consortium_key_solana",
        partition_by=["block_month"],
        materialized="incremental",
        file_format="delta",
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key=[
            "blockchain",
            "tx_id",
            "tx_index",
            "outer_instruction_index",
            "inner_instruction_index",
        ],
    )
}}

{% set project_start_date = "2024-03-01" %}
{% set fee_receiver_1 = "6qgwjhV2RQxcPffRdtQBTTEezRykQKXqhcDyv1z3r9tq" %}
{% set wsol_token = "So11111111111111111111111111111111111111112" %}

with
    users as (
        select * from {{ref('consortium_key_solana_bot_users')}}
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
            0 as fee_usd,
            0 AS fee_token_amount,
            'SOL' as fee_token_symbol,
            '{{wsol_token}}' as fee_token_address,
            project,
            version,
            token_pair,
            project_program_id as project_contract_address,
            trader_id as user,
            trades.tx_id,
            tx_index,
            outer_instruction_index,
            inner_instruction_index
        from {{ ref("dex_solana_trades") }} as trades
        join users on trader_id = user
        where
            {% if is_incremental() %} {{ incremental_predicate("trades.block_time") }}
            {% else %} trades.block_time >= timestamp '{{project_start_date}}'
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
    'Consortium Key' as bot,
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
