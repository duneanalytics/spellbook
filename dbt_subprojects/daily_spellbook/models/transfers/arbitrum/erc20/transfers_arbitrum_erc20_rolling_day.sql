{{ config(
        
        alias = 'erc20_rolling_day')
}}

        SELECT
            blockchain, 
            block_day, 
            wallet_address,
            token_address,
            symbol, 
            CAST(NOW() as timestamp) as last_updated,
            ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address ORDER BY block_day DESC) as recency_index,
            SUM(amount_raw) OVER (PARTITION BY token_address, wallet_address ORDER BY block_day) as amount_raw, 
            SUM(amount) OVER (PARTITION BY token_address, wallet_address ORDER BY block_day) as amount          
        FROM 
        {{ ref('transfers_arbitrum_erc20_agg_day') }}