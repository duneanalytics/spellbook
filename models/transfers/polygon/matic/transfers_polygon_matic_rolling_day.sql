{{ config(
        tags = ['dunesql'],
        alias = alias('matic_rolling_day'))
}}

        SELECT
            blockchain, 
            day, 
            wallet_address,
            token_address,
            symbol, 
            CAST(NOW() as timestamp) as last_updated,
            ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address ORDER BY day DESC) as recency_index,
            SUM(amount_raw) OVER (PARTITION BY token_address, wallet_address ORDER BY day) as amount_raw, 
            SUM(amount) OVER (PARTITION BY token_address, wallet_address ORDER BY day) as amount          
        FROM 
        {{ ref('transfers_polygon_matic_agg_day') }}