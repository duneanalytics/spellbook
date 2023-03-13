{{config(alias='nft_smart_trader_ROI_eth')}}

with  

aggregated_wallet_trading_stats as  
        (select * 
        from {{ref('nft_ethereum_wallet_metrics')}}
        where trades_count >= 10
            and unique_collections_traded >= 3
            and spent_eth >= 1
            and spent_eth_realized >= 1
            and ROI_eth > 0),

     
aggregated_wallet_trading_stats_w_ranks as     
    (select 
    ROW_NUMBER() OVER(ORDER BY ROI_eth_realized DESC) rank_roi,
    count(1) over () as total_count, 
    *
    from aggregated_wallet_trading_stats),
        
        
aggregated_wallet_trading_stats_w_label as  
        (select 
        'ethereum' as blockchain,
        wallet as address,
        CASE WHEN (rank_roi*1.00 / total_count)*100 <= 3
                              AND (rank_roi*1.00 / total_count)*100 > 2
                            THEN 'Top 3% Smart NFT Trader (ROI Realized in ETH w filters)'
                        WHEN (rank_roi*1.00 / total_count)*100 <= 2
                              AND (rank_roi*1.00 / total_count)*100 > 1
                            THEN 'Top 2% Smart NFT Trader (ROI Realized in ETH w filters)'
                        WHEN (rank_roi*1.00 / total_count)*100  <= 1
                            THEN 'Top 1% Smart NFT Trader (ROI Realized in ETH w filters)' END AS name,
        'nft' AS category,
        'NazihKalo' AS contributor,
        'query' AS source,
        date '2023-03-05' as created_at,
        current_timestamp as updated_at,
        'nft_traders_roi' as model_name,
        'usage' as label_type
        -- uncomment line below to see stats on the trader 
        -- , *  
        from aggregated_wallet_trading_stats_w_ranks order by ROI_eth_realized desc )

select * from aggregated_wallet_trading_stats_w_label 
where name is not null

