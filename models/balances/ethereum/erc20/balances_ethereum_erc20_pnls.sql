{{ config(   schema = 'balances_ethereum',
        tags = ['dunesql'],
        alias = alias('erc20_pnls'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time']
    )
}}

WITH 

distinct_weth_pairs_tmp as ( -- gets the distinct weth pair addresses on uniswap in past two months 
    SELECT 
        token_bought_address as token_address,
        project_contract_address as pair_address,
        COUNT(*) as num_trades
    FROM 
    {{ ref('uniswap_v2_ethereum_trades') }}
    WHERE block_time > CAST(NOW() as timestamp) - Interval '2' Month
    AND token_sold_symbol IN ('WETH', 'ETH')
    AND token_bought_symbol NOT IN ('USDC', 'USDT', 'DAI', 'ARB', 'OP', 'LINK', 'COMP') -- manually removing this for optimization
    GROUP BY 1, 2 
    
    UNION ALL 
    
    SELECT 
        token_sold_address as token_address,
        project_contract_address as pair_address,
        COUNT(*) as num_trades
    FROM 
    {{ ref('uniswap_v2_ethereum_trades') }}
    WHERE block_time > CAST(NOW() as timestamp) - Interval '2' Month
    AND token_bought_symbol IN ('WETH', 'ETH')
    AND token_sold_symbol NOT IN ('USDC', 'USDT', 'DAI', 'ARB', 'OP', 'LINK', 'COMP') -- manually removing this for optimization
    GROUP BY 1, 2 
),

distinct_weth_pairs as (
    SELECT 
        token_address,
        pair_address,
        SUM(num_trades) as total_trades 
    FROM 
    distinct_weth_pairs_tmp
    GROUP BY 1, 2 
),

minter_addresses as (
    SELECT 
        MIN_BY("from", evt_block_time) as mint_address, 
        contract_address as token_address 
    FROM 
    {{ source('erc20_ethereum', 'evt_transfer') }}
    WHERE contract_address IN (SELECT token_address FROM distinct_weth_pairs)
    GROUP BY 2 
),

hourly_total_supply as (
    SELECT 
        t.block_hour, 
        SUM(COALESCE(t.amount, t.amount_raw/1e18)) as total_supply, 
        t.token_address
    FROM 
    {{ ref('balances_ethereum_erc20_hour') }} t 
    INNER JOIN 
    minter_addresses m 
        ON t.token_address = m.token_address
        AND t.wallet_address != m.mint_address
    WHERE block_hour > CAST(NOW() as timestamp) - Interval '2' Month
    AND t.token_address IN (SELECT token_address FROM distinct_weth_pairs)
    GROUP BY 1, 3 
), 
 
buy_transactions as (
    SELECT 
        taker as trader, 
        tx_hash,
        tx_from, 
        block_time, 
        block_month,
        COALESCE(token_bought_amount, token_bought_amount_raw/1e18) as token_bought_amount, 
        amount_usd/(COALESCE(token_bought_amount, token_bought_amount_raw/1e18)) as token_price, 
        token_bought_address, 
        token_sold_amount as eth_sold, 
        amount_usd as usd_value,
        ROW_NUMBER() OVER (PARTITION BY trader, token_bought_address ORDER BY block_time) as trade_id
    FROM 
    {{ ref('uniswap_v2_ethereum_trades') }}
    WHERE block_time > CAST(NOW() as timestamp) - Interval '2' Month
    AND token_sold_symbol IN ('WETH', 'ETH')
    AND token_bought_symbol NOT IN ('USDC', 'USDT', 'DAI', 'ARB', 'OP', 'LINK', 'COMP') -- manually removing this for optimization
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

market_cap as (
    SELECT 
        b.*, 
        b.token_price * h.total_supply as market_cap
    FROM 
    buy_transactions b 
    LEFT JOIN 
    hourly_total_supply h 
        ON b.token_bought_address = h.token_address
        AND date_trunc('hour', b.block_time) = h.block_hour
        AND h.total_supply >= 0 
), 

sell_transactions as (
    SELECT 
        taker as trader, 
        tx_hash,
        tx_from, 
        block_time, 
        block_month,
        COALESCE(token_sold_amount, token_sold_amount_raw/1e18) as token_sold_amount, 
        amount_usd/(COALESCE(token_sold_amount, token_sold_amount_raw/1e18)) as token_price,
        SUM(COALESCE(token_sold_amount, token_sold_amount_raw/1e18)) OVER (PARTITION BY trader, token_sold_address ORDER BY block_time) as cum_sold_amount,
        token_sold_address, 
        token_bought_amount as eth_bought, 
        amount_usd as usd_value
    FROM 
    {{ ref('uniswap_v2_ethereum_trades') }}
    WHERE block_time > CAST(NOW() as timestamp) - Interval '2' Month
    AND token_bought_symbol IN ('WETH', 'ETH')
    AND token_sold_symbol NOT IN ('USDC', 'USDT', 'DAI', 'ARB', 'OP', 'LINK', 'COMP') -- manually removing this for optimization
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

sell_filters as (
    SELECT 
        a.*, 
        m.token_bought_amount,
        m.trader, 
        m.token_bought_address,
        m.tx_hash, 
        m.token_price as buy_price, 
        m.usd_value as buy_volume, 
        m.eth_sold
    FROM 
    (
    SELECT 
        MAX(st.cum_sold_amount) as sold_amount, 
        AVG(st.price) as avg_sell_price, 
        MAX(st.block_time) as last_sell_time
        m.trade_id
    FROM 
    market_cap m 
    INNER JOIN 
    sell_transactions st 
        ON m.token_bought_address = st.token_sold_address
        AND m.trader = st.trader
        AND st.block_time > m.block_time
        AND st.cum_sold_amount <= m.token_bought_amount
    GROUP BY 3 
    ) a 
    INNER JOIN 
    market_cap m 
        ON a.trade_id = m.trade_id
),

trade_data_enriched as (
    SELECT 
        trader, 
        trade_id, 
        buy_volume, 
        sold_amount * avg_sell_price as sell_volume, 
        buy_volume - (sold_amount * avg_sell_price) as trade_pnl
    FROM 
    sell_filters
)

SELECT * FROM trade_data_enriched