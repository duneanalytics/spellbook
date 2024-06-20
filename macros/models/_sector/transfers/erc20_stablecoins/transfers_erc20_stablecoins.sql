{% macro transfers_erc20_stablecoins(
    blockchain = null
    , first_stablecoin_deployed = null
    ) 
%}


WITH stables_in_tx AS (
        SELECT 
            tx_hash
            , COUNT(*) AS stables_in_tx 
        FROM (
            SELECT distinct 
                t.contract_address
                , t.tx_hash 
            FROM (
                {{
                    transfers_enrich(
                        base_transfers = ref('tokens_' + blockchain + '_base_transfers')
                        , tokens_erc20_model = source('tokens', 'erc20')
                        , prices_model = source('prices', 'usd')
                        , evms_info_model = ref('evms_info')
                        , transfers_start_date = first_stablecoin_deployed
                        , blockchain = blockchain
                    )
                }} 
            ) t
            INNER JOIN (
                SELECT contract_address 
                FROM {{ source('tokens_' + blockchain, 'stablecoins') }}
            ) s
                ON s.contract_address = t.contract_address
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('block_time') }}
            {% endif %}
        ) GROUP BY 1
    )
    , transfers AS (
        SELECT
            '{{blockchain}}' AS blockchain
            , t.block_time
            , t.block_date
            , t.tx_hash
            , t.contract_address
            , s.symbol
            , COALESCE(t.amount, t.amount_raw / POW(10, s.decimals)) AS amount
            , x.stables_in_tx
        FROM ( 
            {{
                transfers_enrich(
                    base_transfers = ref('tokens_' + blockchain + '_base_transfers')
                    , tokens_erc20_model = source('tokens', 'erc20')
                    , prices_model = source('prices', 'usd')
                    , evms_info_model = ref('evms_info')
                    , transfers_start_date = first_stablecoin_deployed
                    , blockchain = blockchain
                )
            }}
        ) t
        INNER JOIN (
            SELECT tx_hash, stables_in_tx 
            FROM stables_in_tx
        ) x 
            ON x.tx_hash = t.tx_hash
        INNER JOIN (
            SELECT 
                symbol
                , contract_address
                , decimals 
            FROM {{ source('tokens_' + blockchain, 'stablecoins') }}
        ) s
            ON s.contract_address = t.contract_address
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
    )
    , transfer_volume AS (
        SELECT 
            blockchain
            , symbol
            , tx_hash
            , sum(amount) AS volume
        FROM transfers
        GROUP BY 1, 2, 3
    )
    , tx_costs AS (
        SELECT distinct -- otherwise you get an entry for every event
            t.blockchain
            , t.contract_address
            , t.symbol
            , t.tx_hash
            , t.block_time
            , t.block_date
            , t.stables_in_tx
            , tx.tx_fee_gas_coin
        FROM transfers t
        INNER JOIN (
            SELECT 
                hash 
                , CASE
                    WHEN blockchain = 'arbitrum' THEN ((effective_gas_price/1e9) * (gas_used/1e9))
                    WHEN blockchain = 'base'     THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                    WHEN blockchain = 'blast'    THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                    WHEN blockchain = 'optimism' THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                    WHEN blockchain = 'scroll'   THEN ((gas_price/1e9) * (gas_used/1e9)) + (l1_fee/1e18)
                    WHEN blockchain = 'tron'     THEN ((gas_used) * (0.00042))
                    --WHEN t.blockchain = 'zkevm' THEN ((effective_gas_price/1e9) * (gas_used/1e9)) --zkevm uses effective_gas_price but this is not in table
                    ELSE ((gas_price/1e9) * (gas_used/1e9))
                END AS tx_fee_gas_coin
            FROM {{ ref('evms_transactions') }}
            WHERE blockchain = '{{blockchain}}'
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
        ) tx
            ON tx.hash = t.tx_hash 
    )
    , costs_and_transfers AS (
        SELECT 
            c.blockchain
            , c.contract_address
            , c.symbol
            , c.tx_hash
            , c.block_time
            , c.block_date
            , c.tx_fee_gas_coin
            , c.stables_in_tx
            , t.volume
            , p.gas_coin
            , p.median_price_day AS gas_coin_median_price_day
        FROM tx_costs c
        INNER JOIN (
          SELECT
                symbol
                , tx_hash
                , volume
            FROM transfer_volume
        ) t
            ON t.tx_hash = c.tx_hash 
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
{%- endmacro %}
