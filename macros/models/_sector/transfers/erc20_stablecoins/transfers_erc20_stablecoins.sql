{% macro transfers_erc20_stablecoins(blockchain) %}
WITH
    stables_in_tx AS (
        SELECT 
            evt_tx_hash
            , COUNT(*) AS stables_in_tx 
        FROM (
            SELECT distinct 
                t.contract_address
                , t.evt_tx_hash 
            FROM {{ source('erc20_' + blockchain, 'evt_transfer') }} t
            INNER JOIN (
                SELECT 
                    blockchain
                    , contract_address 
                FROM {{ source('tokens_' + blockchain, 'stablecoins') }}
            ) s
                ON s.blockchain = '{{blockchain}}' 
                AND s.contract_address = t.contract_address
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
            {% endif %}
        ) GROUP BY 1, 2
    )
    , transfers AS (
        SELECT
            '{{blockchain}}' AS blockchain
            , t.evt_block_time
            , t.evt_tx_hash
            , t.contract_address
            , s.symbol
            , t.value / POW(10, s.decimals) AS amount
            , x.stables_in_tx
        FROM {{ source('erc20_' + blockchain, 'evt_transfer') }} t
        INNER JOIN (SELECT * FROM stables_in_tx) x 
            ON x.evt_tx_hash = t.evt_tx_hash
        INNER JOIN (
            SELECT 
                blockchain
                , symbol
                , contract_address
                , decimals 
            FROM {{ source('tokens_' + blockchain, 'stablecoins') }}
        ) s
            ON s.blockchain = '{{blockchain}}'
            AND s.contract_address = t.contract_address
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    )
    , transfer_volume AS (
        SELECT 
            blockchain
            , symbol
            , evt_tx_hash
            , sum(amount) AS volume
        FROM transfers
        GROUP BY 1, 2, 3
    )
    , tx_costs AS (
        SELECT distinct -- otherwise you get an entry for every event
            t.blockchain
            , t.contract_address
            , t.symbol
            , t.evt_tx_hash
            , t.evt_block_time
            , date_trunc('DAY', t.evt_block_time) AS evt_block_date
            , t.stables_in_tx
            , CASE
                WHEN t.blockchain = 'arbitrum' THEN ((effective_gas_price/1e9) * (gas_used/1e9))
                WHEN t.blockchain = 'base' THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                WHEN t.blockchain = 'blast' THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                WHEN t.blockchain = 'optimism' THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                WHEN t.blockchain = 'scroll' THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                WHEN t.blockchain = 'tron' THEN ((gas_used) * (0.00042))
                --WHEN t.blockchain = 'zkevm' THEN ((effective_gas_price/1e9) * (gas_used/1e9)) --zkevm uses effective_gas_price but this is not in table
                ELSE ((tx.gas_price/1e9) * (tx.gas_used/1e9))
            END AS tx_fee_gas_coin
        FROM transfers t
        INNER JOIN (
            SELECT 
                gas_price
                , gas_used
                , hash 
                , CASE WHEN '{{blockchain}}' = 'arbitrum' THEN effective_gas_price ELSE NULL END AS effective_gas_price
                , CASE
                    WHEN '{{blockchain}}' = 'base' THEN l1_fee
                    WHEN '{{blockchain}}' = 'blast' THEN l1_fee
                    WHEN '{{blockchain}}' = 'optimism' THEN l1_fee
                    WHEN '{{blockchain}}' = 'scroll' THEN l1_fee
                    ELSE NULL
                END AS l1_fee
            FROM {{ source(blockchain, 'transactions') }}
            {% if is_incremental() %}
            WHERE block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        ) tx
            ON tx.hash = t.evt_tx_hash 
    )
    , costs_and_transfers AS (
        SELECT 
            c.blockchain
            , c.contract_address
            , c.symbol
            , c.evt_tx_hash
            , c.evt_block_time
            , c.evt_block_date
            , c.tx_fee_gas_gwei
            , c.stables_in_tx
            , t.volume
            , p.gas_coin
            , p.median_price_day AS gas_coin_median_price_day
        FROM tx_costs c
        INNER JOIN (
          SELECT
                symbol
                , evt_tx_hash
                , volume
            FROM transfer_volume
        ) t
            ON t.evt_tx_hash = c.evt_tx_hash 
            AND t.symbol = c.symbol
        LEFT JOIN ( -- Add on USD price of tx
            SELECT 
                CASE 
                    WHEN symbol = 'ETH' THEN 'ethereum'
                    WHEN symbol = 'MATIC' THEN 'polygon'
                    WHEN symbol = 'XDAI' THEN 'gnosis'
                    WHEN symbol = 'BNB' THEN 'bnb'
                    WHEN symbol = 'AVAX' THEN 'avalanche_c'
                    WHEN symbol = 'TRX' THEN 'tron'
                    WHEN symbol = 'FTM' THEN 'fantom'
                    WHEN symbol = 'CELO' THEN 'celo'
                END AS chain
                , minute
                , symbol AS gas_coin
                , APPROX_PERCENTILE(price, 0.50) AS median_price_day
            FROM {{ source('prices', 'usd') }}
            WHERE symbol in ('ETH', 'MATIC', 'XDAI', 'BNB', 'AVAX', 'TRX', 'FTM', 'CELO') AND blockchain IS NULL 
            {% if is_incremental() %}
            AND {{ incremental_predicate('minute') }}
            {% endif %}
            GROUP BY 1, 2
        ) p
            ON p.chain = IF(c.blockchain in ('polygon', 'gnosis', 'bnb', 'avalanche_c', 'tron', 'fantom', 'celo'), c.blockchain, 'ethereum')
            AND date_trunc('day', p.minute) = c.evt_block_date
    )

SELECT * FROM costs_and_transfers
{% endmacro %}
