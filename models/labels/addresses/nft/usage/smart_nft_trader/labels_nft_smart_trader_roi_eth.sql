{{config(
    
    alias = 'nft_smart_trader_roi_eth'
)}}

with  

aggregated_wallet_trading_stats AS (
    select * 
    from {{ref('nft_ethereum_wallet_metrics')}}
    where trades_count >= 10
        and unique_collections_traded >= 3
        and spent_eth >= 1
        and spent_eth_realized >= 1
        and roi_eth > 0
),

     
aggregated_wallet_trading_stats_w_ranks AS (
    select ROW_NUMBER() OVER (ORDER BY roi_eth_realized DESC) rank_roi,
           count(1) over () AS                                total_count,
           *
    from aggregated_wallet_trading_stats
),
        
        
aggregated_wallet_trading_stats_w_label AS (
    select 'ethereum'                                                                 AS blockchain,
           wallet                                                                     AS address,
           CASE
               WHEN (rank_roi * 1.00 / total_count) * 100 <= 3
                   AND (rank_roi * 1.00 / total_count) * 100 > 2
                   THEN 'Top 3% Smart NFT Trader (ROI Realized in ETH w filters)'
               WHEN (rank_roi * 1.00 / total_count) * 100 <= 2
                   AND (rank_roi * 1.00 / total_count) * 100 > 1
                   THEN 'Top 2% Smart NFT Trader (ROI Realized in ETH w filters)'
               WHEN (rank_roi * 1.00 / total_count) * 100 <= 1
                   THEN 'Top 1% Smart NFT Trader (ROI Realized in ETH w filters)' END AS name,
           'nft'                                                                      AS category,
           'NazihKalo'                                                                AS contributor,
           'query'                                                                    AS source,
           date '2023-03-05'                                                          AS created_at,
           current_timestamp                                                          AS updated_at,
           'nft_traders_roi'                                                          AS model_name,
           'usage'                                                                    AS label_type
           -- uncomment line below to see stats on the trader 
           -- , *  
    from aggregated_wallet_trading_stats_w_ranks order by roi_eth_realized desc
)

select *
from aggregated_wallet_trading_stats_w_label
where name is not null

