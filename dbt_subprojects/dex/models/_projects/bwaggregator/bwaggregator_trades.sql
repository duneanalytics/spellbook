{{ config (
        schema = 'bwaggregator',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells (
            blockchains = \'["arbitrum","base","bnb","ethereum","polygon"]\',
            spell_type = "project", 
            spell_name = "bwaggregator", 
            contributors = \'["kunwh"]\'
        ) }}'
    )
}}

-- This model filters trades from dex.trades that are associated with the BWAggregator project.
-- Currently, dex_aggregator trades only include transactions on EVM 
WITH paymaster_tx AS (
    SELECT DISTINCT 
        blockchain, 
        tx_hash,
        MAX(CASE 
            WHEN contract_address = 0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973 THEN '2' 
            WHEN contract_address = 0xE17162B840cb9A8f6D9920E5832D58f6461caCe8 THEN '1' 
        END) AS version

    FROM evms.logs
    
    WHERE block_date >= date('2025-10-01')
        AND contract_address IN (
            0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973,
            0xE17162B840cb9A8f6D9920E5832D58f6461caCe8
        )
        AND topic0 = 0x89a885b6900024aaed2c0845aad74f2204445bf00ac135917c70f57540e557b3
    GROUP BY
        blockchain,
        tx_hash
)

SELECT
    trade.blockchain,
    'bwaggregator' project,
    CASE 
        WHEN paymaster_tx.version IS NOT NULL THEN paymaster_tx.version
        WHEN tx_to = 0xE17162B840cb9A8f6D9920E5832D58f6461caCe8 THEN '1' 
        ELSE '2'
    END as version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_symbol,
    token_sold_symbol,
    token_pair,
    token_bought_amount,
    token_sold_amount,
    token_bought_amount_raw,
    token_sold_amount_raw,
    amount_usd,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    trade.tx_hash,
    tx_from,
    tx_to,
    CAST(ARRAY[-1] as array<bigint>) AS trace_address,
    evt_index
FROM dex.trades trade
LEFT JOIN paymaster_tx ON trade.tx_hash = paymaster_tx.tx_hash AND trade.blockchain = paymaster_tx.blockchain

WHERE trade.block_date >= DATE('2025-10-01')
    AND (
    -- By aggregator contracts
        tx_to in (
            0xE17162B840cb9A8f6D9920E5832D58f6461caCe8, 
            0xBc1D9760bd6ca468CA9fB5Ff2CFbEAC35d86c973, 
            0x6752b178E2Ed13BCeE6951cEF907B44C95c5D630, 
            0x704dE6944dE10b69a5357B9cB976Dbe89d6eA414
        )
    -- By gas paymaster transactions
        OR paymaster_tx.tx_hash IS NOT NULL
    )
;