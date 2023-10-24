{{config(
    
    alias = 'smart_dex_traders_ethereum'
)}}
/*
** This is a submission for the Dune labels mini hackathon (March 2023)
** Category: Smart Money, Smart DEX Trader (Persona)
** ref link: https://duneanalytics.notion.site/Dune-Labels-Hackathon-2023-9c1edd4b776a44699a1524bf4131105d
**
** What questions does this label help to solve?
**   1. "How many real users are trading on our DEX?" - It's hard for dune wizards to figure out 
**      how many "real" (ie. human, retail) traders are trading on a DEX. Although dune wizards
**      have existing labels such as labels.contracts to identify and filter 
**      out trades that take place via contract interactions, this does not catch all bot activity.
**
**      "Real" user activity is an important metric for DEX projects to track, in order 
**      to advise product strategy and development effectively. 
**
**      > This label seeks to identify "human-like" behavior based on heuristics.
**
**   2. "Who are the top performing retail traders on our DEX?" - While there are dashboards
**      that track portfolio performance eg. @defimochi's https://dune.com/defimochi/token-god-mode,
**      most of the top traders are bots due to the challange of identifying real users. 
**
**      For DEX projects, this is a strong indicator that power users are attacted to the platform.  
**
**      > This label seeks to identify top Retail DEX traders based on Average Cost Basis analysis.       
**   
** How this label defines a Smart DEX Trader:
**   1. Trader = An address exhibitiing "human-like" behavior (Retail trader, ie. not a bot)
**   2. Smart = A trader having a portfolio with high profit multiples
** 
** Scope: 
**   To limit the results returned from this label, the scope is defined as:
**      1. DEX Trades on Ethereum
**      2. Portfolio performance over the last 60 days
**      3. Top 10% of retail traders ranked by profit multiple
**
** Considerations (limitations):
**      1. Heuristics used to detect retail (human-like) behavior are subjective and 
**         oversimplified. This means that the results returned may have false positives (bots) and
**         false negatives (excluded real retail traders).
**      2. A simple portfolio profitability model based on average cost basis is used. Transaction
**         fees, LP and Farming activity are not considered in this model. 
** 
** Future work:
**      1. How may we test the effectiveness of this label?
**      2. How may we improve, or move away from, "real user" heuristics?
**      3. How may we improve portfolio monitoring and profitability metrics?
** 
** Disclaimer: 
**      1. As with anything on the internet, please review the code before using it in 
**         production. 
**      2. Feedback and suggestions are welcome! I'm sure there's lots of room for improvement.
**         Find me on Dune discord: Stone#6981
*/

with 

/*
** In this section, we remove contract addresses as we assume retail traders would not be swapping via contracts.
*/

-- Get all swappers on Ethereum in the last 60 days
swappers as (
    select 
        block_date,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_bought_amount,
        token_sold_amount,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        tx_hash
    from {{ ref('dex_trades') }}
    where 
        blockchain = 'ethereum' 
        and block_date > now() - interval '60' day
        -- remove missing data https://discordapp.com/channels/757637422384283659/757641002138730588/903537780007784510
        and token_bought_symbol != ''
        and token_sold_symbol !=''
        and amount_usd is not null
    order by 1 desc
)

-- Get list of known contract addresses
, addresses_to_exclude as (
    select distinct address from (
        select distinct address from {{ ref('labels_contracts') }}
        union all
        select distinct address from {{ source('ethereum', 'traces') }}
        union all
        select distinct address from {{ ref('labels_mev_ethereum') }}
        union all
        select distinct address from {{ ref('labels_sandwich_attackers') }}
    )
)

-- Remove contract addresses from list of swappers
, swappers_address_only as (
    select s.*
    from swappers as s
    left join addresses_to_exclude as e on s.taker = e.address
    where e.address is NULL
)

/*
** In this section, we apply heuristics (H) to filter for retail behavior.
*/

-- H1. A retail trader does not execute the same amount of trades every day. eg, takes weekends off, prefers mondays.
--     Trades taken should tend twards weekends or weekdays. ie. skewness <> 0
, filter_by_weekday as (
    select taker, 
        skewness(day_of_week(block_date)) as trading_day_variance
    from swappers_address_only
    group by 1
    having
        -- Note on day-of-week skewness: 
        --  negative/positive = distribution of trades is towards end/start of the week
        --  zero = distribution of trades is even across all days of the week
        --  NaN = data does not provide sufficient information about its distribution
        (skewness(day_of_week(block_date)) > 0 or skewness(day_of_week(block_date)) < 0)
)

-- H2. A retail trader trades takes breaks between trades. 
--     ie. average time between trades > 60 mins
, time_between_trades as (
    select 
        taker,
        date_diff('second', lag(block_time, 1) over (partition by taker order by block_time), block_time) / 60  as time_between_trades_mins
    from swappers_address_only
)
-- Note: We can't have nested aggregate functions, so breaking it up into two CTEs
, filter_by_average_time_between_trades as (
    select 
        taker,
        avg(time_between_trades_mins) as average_time_between_trades_mins
    from time_between_trades
    group by 1
    having
        avg(time_between_trades_mins) > 60
)

-- Apply all filters to get retail swappers
, all_filters as ( 
    select t.taker as taker
    from filter_by_average_time_between_trades as t
    left join (select taker from filter_by_weekday) as w on t.taker = w.taker
    where w.taker is not NULL
)
, retail_swappers as (
    select distinct s.taker as taker
    from swappers_address_only as s
    left join (select taker from all_filters) as f on s.taker = f.taker
    where f.taker is not NULL
)

/*
** In this section, we assess the trader's portfolio based on the following metrics:
** P1. Portfolio profit based on average cost-basis 
** P2. Profit multiples
*/

-- Get latest prices
, prices as (
    select symbol, price, contract_address as token_address
    from {{ ref('prices_usd_latest') }}
    where blockchain = 'ethereum'
)

-- P1. Find top traders by profit.
--   reference: @defimochi: https://dune.com/queries/1910280/3145812
--   buy profit = token_bought_amount * (current price - amount_usd/ token_bought_amount)
--   sell profit = token_sold_amount * (current price - amount_usd/ token_sold_amount)
, buys as (
    select 
        'buy' as action,
        block_time,
        token_sold_symbol,
        token_sold_address as token_sold_address,
        token_bought_symbol,
        token_bought_address as token_bought_address,
        token_bought_amount,
        amount_usd, 
        taker,
        amount_usd / token_bought_amount as cost_basis,
        project as venue
    from {{ ref('dex_trades') }}
    where
        blockchain = 'ethereum'
        and block_time > now() - interval '60' day
        -- remove missing data https://discordapp.com/channels/757637422384283659/757641002138730588/903537780007784510
        and token_bought_symbol != ''
        and token_sold_symbol !=''
        and amount_usd is not NULL
)
, buys_profit as (
    select b.*, 
        p.price as market_price_usd, 
        b.token_bought_amount * (p.price - b.amount_usd / b.token_bought_amount) as estimated_profit  
    from buys as b 
    left join prices as p on b.token_bought_address = p.token_address
    where p.price is not NULL
)
, sells as (
    select 
        'sell' as action,
        block_time,
        token_sold_symbol,
        token_sold_address as token_sold_address,
        token_bought_symbol,
        token_bought_address token_bought_address,
        token_sold_amount,
        amount_usd, 
        taker,
        amount_usd / token_sold_amount as cost_basis,
        project as venue
    from {{ ref('dex_trades') }}
    where
        blockchain = 'ethereum'
        and block_time > now() - interval '60' day
        -- remove missing data https://discordapp.com/channels/757637422384283659/757641002138730588/903537780007784510
        and token_bought_symbol != ''
        and token_sold_symbol !=''
        and amount_usd is not NULL
)
, sells_profit as (
    select s.*, 
        p.price as market_price_usd, 
        s.token_sold_amount * (p.price - s.amount_usd / s.token_sold_amount) as estimated_profit  
    from sells as s
    left join prices as p on s.token_sold_address = p.token_address
    where p.price is not NULL
)
, profit_leaderboard as (
    select taker, 
        sum(estimated_profit) as total_profit_USD,
        sum(estimated_profit) / sum(amount_usd) as profit_multiple
    from (
        select * from sells_profit
        union all
        select * from buys_profit
    )
    group by 1
    having sum(estimated_profit) > 0 
)
-- Apply filter to get retail leaderboard
, retail_leaderboard as (
    select 
        r.taker as taker,
        p.total_profit_USD as total_profit_USD,
        p.profit_multiple as profit_multiple
    from retail_swappers as r
    left join profit_leaderboard as p on r.taker = p.taker
    where p.taker is not NULL
)

-- Get top 10% of retail traders
, percentiles as (
	select taker, 
	    total_profit_USD,
	    profit_multiple,
		percent_rank() over (order by profit_multiple) as percent_rank
	from retail_leaderboard
)
, retail_top_percent as (
    select taker, 
	    total_profit_USD,
	    profit_multiple,
		percent_rank 
	from percentiles
    where percent_rank >= 0.90
)

select 
    'ethereum' as blockchain,
    taker as address, 
    'Top 10% Dex Traders (Retail)' as name, 
    'dex' as category, 
    'stone' as contributor,
    'query' as source,
    TIMESTAMP '2023-03-14' as created_at,
    now() as updated_at,
    'smart_dex_traders' as model_name, 
    'persona' as label_type

from retail_top_percent
