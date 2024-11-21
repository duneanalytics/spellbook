{{ config(
    
    schema = 'aztec_v2_ethereum',
    alias = 'daily_bridge_activity',
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
        , bridge_protocol
        , bridge_address
        , contract_address as token_address
        , count(*) as num_tfers -- number of transfers
        , count(distinct evt_tx_hash) as num_rollups -- number of rollups
        , sum(case when spec_txn_type in ('Bridge to Protocol','Protocol to Bridge') then value_norm else 0 end ) as abs_value_norm
        , sum(case when spec_txn_type = 'Bridge to Protocol' then value_norm else 0 end ) as input_value_norm
        , sum(case when spec_txn_type = 'Protocol to Bridge' then value_norm else 0 end ) as output_value_norm
    FROM {{ref('aztec_v2_ethereum_rollupbridge_transfers')}}
    where bridge_protocol != '' -- exclude all txns that don't interact with the bridges
    group by 1,2,3,4
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
        , dt.bridge_protocol
        , dt.bridge_address
        , dt.token_address
        , er.symbol 
        , dt.num_rollups
        , dt.num_tfers
        , dt.abs_value_norm
        , dt.abs_value_norm * COALESCE(p.price_usd, b.price) as abs_volume_usd
        , dt.abs_value_norm * COALESCE(p.price_eth, b.price_eth) as abs_volume_eth
        , dt.input_value_norm * COALESCE(p.price_usd, b.price) as input_volume_usd
        , dt.input_value_norm * COALESCE(p.price_eth, b.price_eth) as input_volume_eth
        , dt.output_value_norm * COALESCE(p.price_usd, b.price) as output_volume_usd
        , dt.output_value_norm * COALESCE(p.price_eth, b.price_eth) as output_volume_eth
    from daily_transfers dt
    LEFT JOIN {{source('tokens', 'erc20')}} er ON dt.token_address = er.contract_address AND er.blockchain = 'ethereum'
    LEFT join token_prices p on dt.date = p.day and dt.token_address = p.token_address AND dt.token_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    LEFT JOIN token_prices_eth b on dt.date = b.day AND dt.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee -- using this to get price for missing ETH token 