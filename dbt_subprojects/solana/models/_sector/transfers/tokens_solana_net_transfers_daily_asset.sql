{{ config(
        schema = 'tokens_solana'
        , alias = 'net_transfers_daily_asset'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
)
}}

with raw_transfers as (
    select
        'solana' as blockchain
        , block_date
        , token_mint_address as contract_address
        , symbol
        , from_owner as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
        , count(*) transfer_count
        , max(block_time) as max_block_time  -- Track most recent block_time per symbol
    from
        {{ source('tokens_solana','transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        'solana'
        , block_date
        , token_mint_address
        , symbol
        , from_owner
        , 'sent'

    union all

    select
        'solana' as blockchain
        , block_date
        , token_mint_address as contract_address
        , symbol
        , to_owner as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
        , count(*) transfer_count
        , max(block_time) as max_block_time  -- Track most recent block_time per symbol
    from
        {{ source('tokens_solana','transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        'solana'
        , block_date
        , token_mint_address
        , symbol
        , to_owner
        , 'received'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
        , sum(t.transfer_count) as transfer_count
        , max(t.max_block_time) as max_block_time  -- Preserve max block_time
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
), net_transfers as (
    select
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
        , sum(transfer_count) as transfer_count
        , max(max_block_time) as max_block_time  -- Preserve max block_time
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
), symbol_ranking as (
    -- Determine the most recent symbol for each contract on each day based on block_time
    select
        blockchain
        , block_date
        , contract_address
        , symbol
        , max(max_block_time) as latest_block_time
        , row_number() over (
            partition by blockchain, block_date, contract_address 
            order by max(max_block_time) desc
        ) as symbol_rank
    from
        net_transfers
    group by
        blockchain
        , block_date
        , contract_address
        , symbol
), latest_symbol as (
    -- Pick the #1 ranked symbol for each contract
    select
        blockchain
        , block_date
        , contract_address
        , symbol
    from
        symbol_ranking
    where
        symbol_rank = 1
)
select
    nt.blockchain
    , nt.block_date
    , nt.contract_address
    , ls.symbol  -- Use the latest/canonical symbol
    , sum(nt.net_transfer_amount_usd) as net_transfer_amount_usd
from
    net_transfers nt
inner join
    latest_symbol ls
    on nt.blockchain = ls.blockchain
    and nt.block_date = ls.block_date
    and nt.contract_address = ls.contract_address
where
    nt.net_transfer_amount_usd > 0
group by
    nt.blockchain
    , nt.block_date
    , nt.contract_address
    , ls.symbol