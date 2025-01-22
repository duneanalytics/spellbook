{{ config(
    
    schema = 'aztec_v2_ethereum',
    alias = 'daily_deposits',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set first_transfer_date = '2022-06-06' %} -- first tx date 

WITH 

daily_transfers as (
    SELECT  
        date_trunc('day', evt_block_time) as date
        , contract_address as token_address
        , count(*) as num_tfers -- number of transfers
        , count(distinct evt_tx_hash) as num_rollups -- number of rollups
        , sum(case when spec_txn_type in ('User Deposit','User Withdrawal') then value_norm else 0 end ) as abs_value_norm
        , sum(case when spec_txn_type = 'User Deposit' then value_norm else 0 end ) as user_deposit_value_norm
        , sum(case when spec_txn_type = 'User Withdrawal' then value_norm else 0 end ) as user_withdrawal_value_norm
    FROM {{ref('aztec_v2_ethereum_rollupbridge_transfers')}}
    where spec_txn_type in ('User Deposit','User Withdrawal')
    group by 1, 2 
),

token_addresses as (
    SELECT 
        DISTINCT(token_address) as token_address FROM daily_transfers
), 

token_prices_token as (
    SELECT 
        date_trunc('day', p.minute) as day, 
        p.contract_address as token_address, 
        p.symbol, 
        AVG(p.price) as price
    FROM 
    {{ source('prices', 'usd') }} p 
    WHERE p.minute >= TIMESTAMP '{{first_transfer_date}}'
    AND p.contract_address IN (SELECT token_address FROM token_addresses)
    AND p.blockchain = 'ethereum'
    GROUP BY 1, 2, 3 
),

token_prices_eth as (
    SELECT 
        date_trunc('day', p.minute) as day, 
        AVG(p.price) as price,
        1 as price_eth
    FROM 
    {{ source('prices', 'usd') }} p 
    WHERE p.minute >= TIMESTAMP '{{first_transfer_date}}'
    AND p.blockchain = 'ethereum'
    AND p.symbol = 'WETH'
    GROUP BY 1, 3 
),

token_prices as (
    SELECT 
        tt.day, 
        tt.token_address,
        tt.symbol,
        tt.price as price_usd, 
        tt.price/te.price as price_eth,
        te.price as eth_price -- to be used later 
    FROM 
    token_prices_token tt 
    INNER JOIN 
    token_prices_eth te 
        ON tt.day = te.day 
)


 select dt.date
        , dt.token_address
        , er.symbol
        , dt.num_rollups
        , dt.num_tfers
        , dt.abs_value_norm
        , dt.abs_value_norm * COALESCE(p.price_usd, b.price) as abs_volume_usd
        , dt.abs_value_norm * COALESCE(p.price_eth, b.price_eth) as abs_volume_eth
        , dt.user_deposit_value_norm * COALESCE(p.price_usd, b.price) as user_deposits_usd
        , dt.user_deposit_value_norm * COALESCE(p.price_eth, b.price_eth) as user_deposits_eth
        , dt.user_withdrawal_value_norm * COALESCE(p.price_usd, b.price) as user_withdrawals_usd
        , dt.user_withdrawal_value_norm * COALESCE(p.price_eth, b.price_eth) as user_withdrawals_eth
    FROM daily_transfers dt
    LEFT JOIN {{source('tokens', 'erc20')}} er ON dt.token_address = er.contract_address AND er.blockchain = 'ethereum'
    LEFT join token_prices p on dt.date = p.day and dt.token_address = p.token_address
    LEFT JOIN token_prices_eth b on dt.date = b.day AND dt.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee -- using this to get price for missing ETH token 
; 