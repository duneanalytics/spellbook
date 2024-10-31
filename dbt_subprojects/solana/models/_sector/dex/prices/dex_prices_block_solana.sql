{{ config(
    schema = 'dex_solana'
    , alias = 'prices_block'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'delete+insert'
    , unique_key = ['block_month', 'blockchain', 'contract_address', 'symbol', 'decimals', 'block_time']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

with dex_trades_raw as (
    select
        'solana' as blockchain
        , block_time
        , token_bought_mint_address
        , token_bought_amount_raw
        , token_bought_amount
        , token_sold_mint_address
        , token_sold_amount_raw
        , token_sold_amount
        , amount_usd
        , array[token_bought_mint_address, token_sold_mint_address] as tokens_swapped
    from
        {{ ref('dex_solana_trades') }}
    where
        1 = 1
        and amount_usd > 0
        and block_time >= now() - interval '10' day
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
),
dex_trades as (
    select distinct
        t.blockchain
        , t.block_time
        , t.token_bought_mint_address
        , t.token_bought_amount_raw
        , t.token_bought_amount
        , t.token_sold_mint_address
        , t.token_sold_amount_raw
        , t.token_sold_amount
        , t.amount_usd
    from
        dex_trades_raw as t
    --only output swaps which contain a trusted token
    inner join {{ source('prices', 'trusted_tokens') }} as tt
        on t.blockchain = tt.blockchain
        and contains(t.tokens_swapped, toBase58(tt.contract_address))
),
dex_bought as (
    select
        d.blockchain
        , d.token_bought_mint_address as contract_address
        , t.symbol as symbol
        , t.decimals as decimals
        , d.block_time as block_time
        , d.token_bought_amount as amount
        , d.amount_usd
        , coalesce(d.amount_usd/d.token_bought_amount, d.amount_usd/(d.token_bought_amount_raw/POW(10, t.decimals))) as price
    from
        dex_trades as d
    inner join {{ ref('tokens_solana_fungible') }} as t
        on d.token_bought_mint_address = t.token_mint_address
    left join {{ source('prices', 'trusted_tokens') }} as ptt
        on d.blockchain = ptt.blockchain
        and d.token_bought_mint_address = toBase58(ptt.contract_address)
    where
        1 = 1
        and d.token_bought_amount > 0
        and d.token_bought_amount_raw > UINT256 '0'
        and t.symbol is not null
        -- filter out tokens that are already in the trusted_tokens table
        and toBase58(ptt.contract_address) is null
), 
dex_sold as (
    select
        d.blockchain
        , d.token_sold_mint_address as contract_address
        , t.symbol as symbol
        , t.decimals as decimals
        , d.block_time as block_time
        , d.token_sold_amount as amount
        , d.amount_usd
        , coalesce(d.amount_usd/d.token_sold_amount, d.amount_usd/(d.token_sold_amount_raw/POW(10, t.decimals))) as price
    from
        dex_trades as d
    inner join {{ ref('tokens_solana_fungible') }} as t
        on d.token_sold_mint_address = t.token_mint_address
    left join {{ source('prices', 'trusted_tokens') }} as ptt
        on d.blockchain = ptt.blockchain
        and d.token_sold_mint_address = toBase58(ptt.contract_address)
    where
        1 = 1
        and d.token_sold_amount > 0
        and d.token_sold_amount_raw > UINT256 '0'
        and t.symbol is not null
        -- filter out tokens that are already in the trusted_tokens table
        and toBase58(ptt.contract_address) is null
),
dex_prices as (
    select
        *
    from
        dex_bought
    union all
    select
        *
    from
        dex_sold
),
volume_filter as (
    --filter out tokens which have less than $10k in volume
    select
        blockchain
        , contract_address
    from
        dex_prices
    group by
        blockchain
        , contract_address
    having
        sum(amount_usd) >= 10000
)
select
    blockchain
    , contract_address
    , symbol
    , decimals
    , cast(date_trunc('day', block_time) as date) as block_date -- for partitioning
    , block_time
    , amount
    , amount_usd
    , price
from
(
    select
        dp.blockchain
        , dp.contract_address
        , dp.symbol
        , dp.decimals
        , dp.block_time
        , sum(dp.amount) as amount
        , sum(dp.amount_usd) as amount_usd
        , approx_percentile(dp.price, 0.5) AS price
    from
        dex_prices as dp
    inner join volume_filter as vf
        on dp.blockchain = vf.blockchain
        and dp.contract_address = vf.contract_address
    group by
        dp.blockchain
        , dp.contract_address
        , dp.symbol
        , dp.decimals
        , dp.block_time
)