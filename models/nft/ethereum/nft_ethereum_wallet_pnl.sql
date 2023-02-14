{{ config(
    alias = 'wallet_pnl',
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['wallet', 'nft_contract_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

WITH trades as
(
    --sells
    SELECT
        seller as wallet, 
        nft_contract_address, 
        'Sells' as trade_type,
        SUM(amount_original) as eth_amount, 
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
        {{ ref('nft_trades') }}
    WHERE
        currency_symbol IN ('ETH', 'WETH')
        AND blockchain = 'ethereum'
        AND buyer != seller 
        AND number_of_items = 1
        -- AND trade_type = 'Single Item Trade' --replace with filter above on number_of_items = 1
        AND amount_original IS NOT NULL 
        AND block_time < NOW() - Interval '5 Minutes'
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY
        1, 2, 3, 6

    UNION ALL 

    --buys
    SELECT 
        buyer as wallet, 
        nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(amount_original) as eth_amount,
        COUNT(*) as trades, 
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
        {{ ref('nft_trades') }}
    WHERE
        currency_symbol IN ('ETH', 'WETH')
        AND blockchain = 'ethereum'
        AND buyer != seller 
        AND number_of_items = 1
        -- AND trade_type = 'Single Item Trade' --replace with filter above on number_of_items = 1
        AND amount_original IS NOT NULL 
        AND block_time < NOW() - Interval '5 Minutes'
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY
        1, 2, 3, 6
), 
combined as
(
    {% if not is_incremental() %}
    SELECT
        wallet, 
        nft_contract_address, 
        last_updated,
        COALESCE
        (
            SUM
            (
                CASE 
                    WHEN trade_type = 'Buys'
                    THEN ABS(eth_amount) 
                    ELSE 0 
                END
            )
        , 0
        ) as eth_spent, 
        COALESCE
        (
            SUM
            (
                CASE 
                    WHEN trade_type = 'Sells'
                    THEN eth_amount
                    ELSE 0 
                END
            )
        , 0
        ) as eth_received, 
        SUM(eth_amount) as pnl, 
        SUM(trades) as trades 
    FROM 
        trades 
    GROUP BY
        1, 2, 3
    {% else %}
    SELECT
        t.wallet, 
        t.nft_contract_address,
        t.last_updated,
        COALESCE
        (
            SUM
            (
                CASE 
                    WHEN t.trade_type = 'Buys'
                    THEN ABS(t.eth_amount) + COALESCE(prev.eth_spent, 0)
                    ELSE 0 
                END
            )
        , 0
        ) as eth_spent,
        COALESCE
        (
            SUM
            (
                CASE 
                    WHEN t.trade_type = 'Sells'
                    THEN t.eth_amount + COALESCE(prev.eth_received, 0)
                    ELSE 0
                END
            )
        , 0
        ) as eth_received,
        SUM(t.eth_amount + COALESCE(prev.pnl, 0)) as pnl,
        SUM(t.trades + COALESCE(prev.trades, 0)) as trades
    FROM 
        trades t
    LEFT JOIN
        {{this}} prev
        ON prev.wallet = t.wallet 
        AND prev.nft_contract_address = t.nft_contract_address
    GROUP BY
        1, 2, 3
    {% endif %}
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY pnl DESC) as index, 
    * 
FROM 
    combined
;