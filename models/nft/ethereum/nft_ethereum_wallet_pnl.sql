{{ config(
    alias = 'wallet_pnl',
    materialized='incremental',
    file_format = 'parquet',
    incremental_strategy = 'insert_overwrite',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

WITH 

trades as (
    SELECT 
    -- non incremental run
    {% if not is_incremental() %}
        seller as wallet, 
        nft_contract_address, 
        'Sells' as trade_type,
        SUM(amount_original) as eth_amount, 
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_trades') }}
    WHERE currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 3, 6

    UNION ALL 

    SELECT 
        buyer as wallet, 
        nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(amount_original) as eth_amount,
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_trades') }}
    WHERE currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 3, 6
    {% endif %}
    -- incremental run 
    {% if is_incremental() %}
        seller as wallet, 
        nft_contract_address, 
        'Sells' as trade_type,
        SUM(amount_original) as eth_amount, 
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_trades') }}
    WHERE block_time >= (SELECT MAX(last_updated) - Interval '5 Minutes' FROM {{this}})
    AND currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 3, 6

    UNION ALL 

    SELECT 
        buyer as wallet, 
        nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(amount_original) as eth_amount,
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_trades') }}
    WHERE block_time >= (SELECT MAX(last_updated) FROM {{this}})
    AND currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 3, 6
    {% endif %}
), 

combined as (
    SELECT 
     {% if not is_incremental() %}
        wallet, 
        nft_contract_address, 
        last_updated,
        COALESCE(SUM(CASE 
            WHEN trade_type = 'Buys'
            THEN ABS(eth_amount) 
            ELSE 0 
        END), 0) as eth_spent, 
        COALESCE(SUM(CASE 
            WHEN trade_type = 'Sells'
            THEN eth_amount
            ELSE 0 
        END), 0) as eth_received, 
        SUM(eth_amount) as pnl, 
        SUM(trades) as trades 
    FROM 
    trades 
    GROUP BY 1, 2
    {% endif %}
    {% if is_incremental() %}
    SELECT 
        prev.wallet, 
        prev.nft_contract_address,
        t.last_updated,
        COALESCE(SUM(CASE 
            WHEN t.trade_type = 'Buys'
            THEN ABS(t.eth_amount) 
            ELSE 0 
        END), 0) + prev.eth_spent as eth_spent,
        COALESCE(SUM(CASE 
            WHEN t.trade_type = 'Sells'
            THEN t.eth_amount
            ELSE 0 
        END), 0) + prev.eth_received as eth_received,
        COALESCE(SUM(t.eth_amount), 0) +  prev.pnl as pnl, 
        COALESCE(SUM(t.trades), 0) + prev.trades as trades 
    FROM 
    {{this}} prev 
    LEFT JOIN 
    trades t 
        ON prev.wallet = t.wallet 
        AND prev.nft_contract_address = t.nft_contract_address
    GROUP BY 1, 2
    {% endif %}
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY pnl DESC) as index, 
    * 
FROM 
combined