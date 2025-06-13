{% macro get_likely_bot_addresses(chain) %}

WITH sender_transfer_rates AS (
    SELECT 
        "from" as sender,
        COUNT(*) as num_txs,
        COUNT(DISTINCT "to") as num_recipients,
        COUNT(*)/COUNT(DISTINCT "to") as txs_per_recipient,
        COUNT(DISTINCT "from") as num_senders,
        COUNT(*)/COUNT(DISTINCT "from") as txs_per_sender,
        COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) as txs_per_hour,
        MIN(block_time) as first_tx,
        MAX(block_time) as last_tx,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number AND r.blockchain = '{{ chain }}'
        ) THEN t.hash END) as num_erc20_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as num_nft_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number AND r.blockchain = '{{ chain }}'
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as num_token_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number AND r.blockchain = '{{ chain }}'
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_erc20_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_nft_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number AND r.blockchain = '{{ chain }}'
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_token_tfer_txs,
        -- Future bot types (commented out until upstream models are migrated)
        0 as num_dex_trade_txs,
        0 as num_perp_trade_txs,
        0 as num_nft_trade_txs,
        0.0 as pct_dex_trade_txs,
        0.0 as pct_perp_trade_txs,
        0.0 as pct_nft_trade_txs
    FROM {{ source(chain, 'transactions') }} t
    GROUP BY 1
    HAVING 
        -- Early bots: > 25 txs / hour per address
        (COUNT(*) >= 100 AND
        COUNT(*)/COUNT(DISTINCT "from") / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 25)
        OR
        -- Established bots: less than 30 senders & > 2.5k txs & > 0.5 txs / hr
        (COUNT(*) >= 2500 AND COUNT(DISTINCT "from") <= 30
        AND COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 0.5)
        OR
        -- Wider distribution bots: > 2.5k txs and > 1k txs per sender & > 0.5 txs / hr
        (COUNT(*) >= 2500 AND COUNT(*)/COUNT(DISTINCT "from") >= 1000
        AND COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 0.5)
)

SELECT 
    '{{ chain }}' as blockchain,
    sender as address,
    'likely bots' as category,
    CASE
        WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot address'
        WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot address'
        WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot address'
        ELSE 'non-token bot address'
    END as name,
    'msilb7' as contributor,
    'query' as source,
    timestamp '2023-03-11' as created_at,
    now() as updated_at,
    'likely_bot_addresses' as model_name,
    'persona' as label_type
FROM sender_transfer_rates

-- Future bot types (commented out until upstream models are migrated)
-- UNION ALL
-- SELECT 
--     chain as blockchain,
--     sender as address,
--     'likely bot types' as category,
--     CASE
--         WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trade bot address'
--         WHEN pct_nft_trade_txs >= 0.5 THEN 'nft trade bot address'
--         WHEN pct_perp_trade_txs >= 0.5 THEN 'perp trade bot address'
--         WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot address'
--         WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot address'
--         WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot address'
--         ELSE 'non-token bot address'
--     END as name,
--     'msilb7' as contributor,
--     'query' as source,
--     timestamp '2023-03-11' as created_at,
--     now() as updated_at,
--     'likely_bot_addresses' as model_name,
--     'persona' as label_type
-- FROM sender_transfer_rates

{% endmacro %}

{% macro get_likely_bot_contracts(chain) %}

WITH sender_transfer_rates AS (
    SELECT 
        "to" as contract,
        COUNT(*) as num_txs,
        COUNT(DISTINCT "from") as num_senders,
        COUNT(*)/COUNT(DISTINCT "from") as txs_per_sender,
        COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) as txs_per_hour,
        MIN(block_time) as first_tx,
        MAX(block_time) as last_tx,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('erc20_' ~ chain, 'evt_Transfer') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number
        ) THEN t.hash END) as num_erc20_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as num_nft_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('erc20_' ~ chain, 'evt_Transfer') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as num_token_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('erc20_' ~ chain, 'evt_Transfer') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_erc20_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_nft_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('erc20_' ~ chain, 'evt_Transfer') }} r 
            WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_token_tfer_txs,
        -- Future bot types (commented out until upstream models are migrated)
        0 as num_dex_trade_txs,
        0 as num_perp_trade_txs,
        0 as num_nft_trade_txs,
        0.0 as pct_dex_trade_txs,
        0.0 as pct_perp_trade_txs,
        0.0 as pct_nft_trade_txs
    FROM {{ source(chain, 'transactions') }} t
    GROUP BY 1
    HAVING 
        -- Early bots: > 25 txs / hour per address
        (COUNT(*) >= 100 AND
        COUNT(*)/COUNT(DISTINCT "from") / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 25)
        OR
        -- Established bots: less than 30 senders & > 2.5k txs & > 0.5 txs / hr
        (COUNT(*) >= 2500 AND COUNT(DISTINCT "from") <= 30
        AND COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 0.5)
        OR
        -- Wider distribution bots: > 2.5k txs and > 1k txs per sender & > 0.5 txs / hr
        (COUNT(*) >= 2500 AND COUNT(*)/COUNT(DISTINCT "from") >= 1000
        AND COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) >= 0.5)
)

SELECT 
    '{{ chain }}' as blockchain,
    contract as address,
    'likely bots' as category,
    CASE
        WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot contract'
        WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot contract'
        WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot contract'
        ELSE 'non-token bot contract'
    END as name,
    'msilb7' as contributor,
    'query' as source,
    timestamp '2023-03-11' as created_at,
    now() as updated_at,
    'likely_bot_contracts' as model_name,
    'persona' as label_type
FROM sender_transfer_rates

-- Future bot types (commented out until upstream models are migrated)
-- UNION ALL
-- SELECT 
--     chain as blockchain,
--     contract as address,
--     'likely bot types' as category,
--     CASE
--         WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trade bot contract'
--         WHEN pct_nft_trade_txs >= 0.5 THEN 'nft trade bot contract'
--         WHEN pct_perp_trade_txs >= 0.5 THEN 'perp trade bot contract'
--         WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot contract'
--         WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot contract'
--         WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot contract'
--         ELSE 'non-token bot contract'
--     END as name,
--     'msilb7' as contributor,
--     'query' as source,
--     timestamp '2023-03-11' as created_at,
--     now() as updated_at,
--     'likely_bot_contracts' as model_name,
--     'persona' as label_type
-- FROM sender_transfer_rates

{% endmacro %} 