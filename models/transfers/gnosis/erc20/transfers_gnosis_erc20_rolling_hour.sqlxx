{{ config(
        
        alias = 'erc20_rolling_hour')
}}


SELECT
    blockchain, 
    block_hour, 
    wallet_address,
    counterparty,
    token_address,
    symbol, 
    CAST(NOW() as timestamp) as last_updated,
    ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY block_hour DESC) as recency_index,
    SUM(amount_raw) OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY block_hour) as amount_raw, 
    SUM(amount) OVER (PARTITION BY token_address, wallet_address, counterparty ORDER BY block_hour) as amount          
FROM 
{{ ref('transfers_gnosis_erc20_agg_hour') }}
