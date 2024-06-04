{{ config(
        
        alias = 'xdai_rolling_day')
}}

        SELECT
            blockchain, 
            day, 
            wallet_address,
            counterparty,
            token_address,
            symbol, 
            CAST(NOW() as timestamp) as last_updated,
            ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY day DESC) as recency_index,
            SUM(amount_raw) OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY day) as amount_raw, 
            SUM(amount) OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY day) as amount          
        FROM 
        {{ ref('transfers_gnosis_xdai_agg_day') }}