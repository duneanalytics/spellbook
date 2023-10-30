{{ config(
    
    alias = 'wallet_metrics',
    materialized='table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["NazihKalo", "hosuke"]\') }}'
    )
}}

with
--- filtering out wash trades based on definition in this model https://github.com/duneanalytics/spellbook/blob/main/models/nft/nft_wash_trades.sql
nft_trades_no_wash as
( select nft.*
    from {{ref('nft_trades')}} nft
    INNER JOIN {{ref('nft_wash_trades')}} wt ON wt.block_number=nft.block_number
    AND wt.unique_trade_id=nft.unique_trade_id
    where is_wash_trade = FALSE ),

--- adding in mints because a mint can be interpreted as a buy for $0 or gas fees 
nft_trades_no_wash_w_mints as (
select aggregator_address       as aggregator_address,
       aggregator_name          as aggregator_name,
       amount_original          as amount_original,
       amount_raw,
       amount_usd               as amount_usd,
       block_number             as block_number,
       block_time            as block_time,
       blockchain               as blockchain,
       buyer                    as buyer,
       collection               as collection,
       currency_contract        as currency_contract,
       currency_symbol          as currency_symbol,
       evt_type                 as evt_type,
       nft_contract_address     as nft_contract_address,
       number_of_items,
       project                  as project,
       project_contract_address as project_contract_address,
       seller                   as seller,
       token_id                 as token_id,
       token_standard           as token_standard,
       trade_category           as trade_category,
       trade_type               as trade_type,
       tx_from                  as tx_from,
       tx_hash                  as tx_hash,
       tx_to                    as tx_to,
       version                  as version
from nft_trades_no_wash

UNION ALL

select aggregator_address       as aggregator_address,
       aggregator_name          as aggregator_name,
       amount_original          as amount_original,
       amount_raw,
       amount_usd               as amount_usd,
       block_number             as block_number,
       block_time            as block_time,
       blockchain               as blockchain,
       buyer                    as buyer,
       collection               as collection,
       currency_contract        as currency_contract,
       currency_symbol          as currency_symbol,
       evt_type                 as evt_type,
       nft_contract_address     as nft_contract_address,
       number_of_items,
       project                  as project,
       project_contract_address as project_contract_address,
       seller                   as seller,
       token_id                 as token_id,
       token_standard           as token_standard,
       trade_category           as trade_category,
       trade_type               as trade_type,
       tx_from                  as tx_from,
       tx_hash                  as tx_hash,
       tx_to                    as tx_to,
       version                  as version
from {{ref('nft_mints')}}
)
,
-- creating a longform version of buys and sells 
buys_and_sells_nft_trades_no_wash_w_mints as
(
    --sells
    SELECT
        src.seller as wallet,
        project,
        collection,
        src.nft_contract_address,
        token_standard,
        token_id,
        'sell' as trade_type,
        block_time,
        tx_hash,
        amount_usd,
        src.amount_original eth_amount
    FROM
        nft_trades_no_wash_w_mints src
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL

    UNION ALL

    --buys
    SELECT
        src.buyer as wallet,
        project,
        collection,
        src.nft_contract_address,
        token_standard,
        token_id,
        'buy' as trade_type,
        block_time,
        tx_hash,
        -1 * amount_usd as amount_usd,
        -1 * src.amount_original as eth_amount
    FROM
        nft_trades_no_wash_w_mints src
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL
),
----- FLOOR PRICES -------
----- FIRST SOURCE: Reservoir - includes offchain sources
reservoir_floors as (
    select contract,
        price_decimal,
        row_number() over (partition by contract order by created_at desc) rn_desc
    from {{source('reservoir', 'collection_floor_ask_events')}}
    where 1 = 1
    and valid_until_dt > current_date
),
reservoir_floors_latest_avg as
(
    select contract
        , avg(price_decimal) avg_floor_price
    from reservoir_floors
    -- use avg of latest 3 floor prices
    where rn_desc <= 3
    group by 1
),
----- SECOND SOURCE: nft_ethereum.collection_stats - based on {{ref('nft_trades')}} 5th percentile of latest traded day
eth_collection_stats_floor as (
    select nft_contract_address,
        price_p5_eth,
        row_number() over (partition by nft_contract_address order by block_date desc) rn
    from {{ref('nft_ethereum_collection_stats')}}
),

eth_collection_stats_latest_floor as (
    select nft_contract_address,
        price_p5_eth
    from eth_collection_stats_floor
    where rn = 1
),

buys_and_sells_w_index as (
select wallet,
       nft_contract_address,
       project,
       collection,
       token_standard,
       token_id,
       trade_type,
       block_time,
       tx_hash,
       amount_usd,
       eth_amount,
       row_number() over (partition by wallet, nft_contract_address, token_id order by block_time) nft_tx_index,
       lag(eth_amount)
           over (partition by wallet, nft_contract_address, token_id order by block_time)          prev_trade_eth_amount,
       lag(amount_usd)
           over (partition by wallet, nft_contract_address, token_id order by block_time)          prev_trade_usd_amount
from buys_and_sells_nft_trades_no_wash_w_mints
),

lastest_eth_price_usd as (
select blockchain,
       minute,
       price
from {{ref('prices_usd_latest')}}
where blockchain = 'ethereum' and symbol = 'WETH'
),




all_trades_profit_and_unrealized_profit as (
select b.wallet,
       b.nft_contract_address,
       b.project,
       b.collection,
       b.token_standard,
       b.token_id,
       case when s.wallet is not null then 1 else 0 end                                        nft_was_sold,
       b.block_time                                                                            buy_block_time,
       s.block_time                                                                            sell_block_time,
       -- Sell time else current time (for calculating ROI)
       coalesce(s.block_time, current_timestamp)                                               sell_block_time_or_current_time,
       b.tx_hash                                                                               buy_tx_hash,
       s.tx_hash                                                                               sell_tx_hash,
       b.amount_usd                                                                            buy_amount_usd,
       s.amount_usd                                                                            sell_amount_usd,
       b.eth_amount                                                                            buy_amount_eth,
       -- Sell amount else current floor (for calculating ROI)
       s.eth_amount                                                                            sell_amount_eth,
       case when s.wallet is not null then s.eth_amount + b.eth_amount else 0 end              eth_profit_realized
from buys_and_sells_w_index b
left join buys_and_sells_w_index s
    on b.wallet = s.wallet
    and b.nft_contract_address = s.nft_contract_address
    and b.token_id = s.token_id
    and b.nft_tx_index + 1 = s.nft_tx_index
where 1 = 1
  and b.trade_type = 'buy'
  and coalesce(s.trade_type, 'sell') = 'sell'
order by b.block_time desc
),

--- Hacky split to fix bloom size error 
all_trades_profit_and_unrealized_profit_w_floors as 

    (select 
        b.*,
        coalesce(floors1.avg_floor_price * p.price, floors2.price_p5_eth * p.price, 0)          floor_usd,
               coalesce(sell_amount_usd, floors1.avg_floor_price * p.price, floors2.price_p5_eth * p.price, 0) +
               buy_amount_usd                                                                            usd_profit,
        
        coalesce(floors1.avg_floor_price, floors2.price_p5_eth, 0)                              floor_eth,
        coalesce(sell_amount_eth, floors1.avg_floor_price, floors2.price_p5_eth, 0)                sell_amount_eth_or_floor,
        case
           when nft_was_sold = 0 then coalesce(floors1.avg_floor_price, floors2.price_p5_eth, 0) + buy_amount_eth
           else 0 end                                                                          eth_profit_unrealized,
        coalesce(sell_amount_eth, floors1.avg_floor_price, floors2.price_p5_eth, 0) + buy_amount_eth eth_profit
    FROM all_trades_profit_and_unrealized_profit b
    left join reservoir_floors_latest_avg floors1
        on floors1.contract = b.nft_contract_address
    left join eth_collection_stats_latest_floor floors2
        on floors2.nft_contract_address = b.nft_contract_address
    CROSS JOIN lastest_eth_price_usd p),

aggregated_wallet_trading_stats as (
select wallet,
       count(distinct nft_contract_address)                                                  unique_collections_traded,
       count(1)                                                                              buys_count,
       sum(nft_was_sold)                                                                     sells_count,
       sum(nft_was_sold)                                                                     trades_count,
       sum(case when eth_profit_realized > 0 then 1 else 0 end)                              profitable_trades_count,
       --- 0 profit trades are also considered uprofitable
       sum(case when nft_was_sold = 1 and eth_profit_realized <= 0 then 1 else 0 end)        unprofitable_trades_count,
        CASE
            WHEN sum(nft_was_sold) = 0 THEN NULL
            ELSE (sum(case when eth_profit_realized > 0 then 1 else 0 end) * 1.00 / sum(nft_was_sold))
        END AS win_percentage,
        CASE
            WHEN sum(nft_was_sold) = 0 THEN NULL
            ELSE (sum(case when eth_profit_realized < 0 then 1 else 0 end) * 1.00 / sum(nft_was_sold))
        END AS loss_percentage,
        CASE
            WHEN sum(nft_was_sold) = 0 THEN NULL
            ELSE (sum(case when nft_was_sold = 1 and eth_profit_realized = 0 then 1 else 0 end) * 1.00 / sum(nft_was_sold))
        END AS breakeven_percentage,
       sum(buy_amount_eth * -1)                                                              spent_eth,
       sum((case when nft_was_sold = 1 then buy_amount_eth else 0 end) * -1)                 spent_eth_realized,
       sum((case when nft_was_sold = 0 then buy_amount_eth else 0 end) * -1)                 spent_eth_unrealized,
       sum(sell_amount_eth_or_floor)                                                         gained_eth,
       sum(sell_amount_eth)                                                                  gained_eth_realized,
       sum(case when nft_was_sold = 0 then floor_eth else 0 end)                             gained_eth_unrealized,
        CASE
            WHEN sum(buy_amount_eth * -1) = 0 THEN NULL
            ELSE ((sum(sell_amount_eth_or_floor) * 1.00 / sum(buy_amount_eth * -1)) - 1)
        END AS roi_eth,

        CASE
            WHEN sum((case when nft_was_sold = 1 then buy_amount_eth else 0 end) * -1) = 0 THEN NULL
            ELSE ((sum(sell_amount_eth) * 1.00 / sum((case when nft_was_sold = 1 then buy_amount_eth else 0 end) * -1)) - 1)
        END AS roi_eth_realized,

        CASE
            WHEN sum((case when nft_was_sold = 0 then buy_amount_eth else 0 end) * -1) = 0 THEN NULL
            ELSE ((sum(floor_eth) * 1.00 / sum((case when nft_was_sold = 0 then buy_amount_eth else 0 end) * -1)) - 1)
        END AS roi_eth_unrealized,
       sum(eth_profit)                                                                       eth_profit,
       sum(eth_profit_realized)                                                              eth_profit_realized,
       sum(eth_profit_unrealized)                                                            eth_profit_unrealized,
       avg(case when eth_profit_realized > 0 then eth_profit_realized end)                   avg_win_size,
       avg(case when eth_profit_realized < 0 then eth_profit_realized end)                   avg_loss_size,
       -- count(distinct date_trunc('week', buy_block_time)) +  unique_weeks_active,
       count(distinct date_trunc('week', buy_block_time))                                    unique_weeks_buying,
       count(distinct date_trunc('week', sell_block_time))                                   unique_weeks_selling

from all_trades_profit_and_unrealized_profit_w_floors
group by 1
)

select * from aggregated_wallet_trading_stats