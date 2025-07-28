{% macro get_date_ranges(chain, days_back=90) %}

WITH chain_start AS (
    SELECT MIN(block_time) as first_block_time
    FROM {{ source(chain, 'transactions') }}
)

SELECT 
    {% if is_incremental() %}
        CASE 
            WHEN date_diff('day', (SELECT MAX(updated_at) FROM {{ this }}), current_date) <= {{ days_back }} THEN
                date_add('day', -{{ days_back }}, current_date)
            ELSE
                (SELECT MAX(updated_at) FROM {{ this }})
        END
    {% else %}
        cs.first_block_time
    {% endif %} as start_time,
    {% if is_incremental() %}
        CASE 
            WHEN date_diff('day', (SELECT MAX(updated_at) FROM {{ this }}), current_date) <= {{ days_back }} THEN
                current_date
            ELSE
                date_add('day', {{ days_back }}, (SELECT MAX(updated_at) FROM {{ this }}))
        END
    {% else %}
        date_add('day', {{ days_back }}, cs.first_block_time)
    {% endif %} as end_time
FROM chain_start cs

{% endmacro %}

{% macro get_likely_bot_addresses(chain, days_back=90) %}

WITH date_ranges AS (
    {{ get_date_ranges(chain, days_back) }}
),

sender_transfer_rates AS (
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
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_erc20_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_nft_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_token_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_erc20_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_nft_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_token_tfer_txs,
        -- DEX trade metrics
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('dex', 'trades') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_dex_trade_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('dex', 'trades') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_dex_trade_txs,
        -- Future bot types (commented out until upstream models are migrated)
        0 as num_perp_trade_txs,
        0 as num_nft_trade_txs,
        0.0 as pct_perp_trade_txs,
        0.0 as pct_nft_trade_txs,
        -- Failed transaction metrics
        cast(COUNT(CASE WHEN success = false THEN 1 END) as double) / cast(COUNT(*) as double) as pct_failed_txs
    FROM {{ source(chain, 'transactions') }} t
    CROSS JOIN date_ranges dr
    WHERE t.block_time BETWEEN dr.start_time AND dr.end_time
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
        OR
        -- Failed transaction bots: > 100 txs and > 90% failed
        (COUNT(*) >= 100 AND cast(COUNT(CASE WHEN success = false THEN 1 END) as double) / cast(COUNT(*) as double) > 0.9)
),

final_results AS (
    SELECT 
        '{{ chain }}' as blockchain,
        sender as address,
        'likely bots' as category,
        CASE
            WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot address'
            WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot address'
            WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot address'
            WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trading bot address'
            WHEN pct_failed_txs > 0.9 THEN 'failed transaction bot address'
            ELSE 'non-token bot address'
        END as name,
        'msilb7' as contributor,
        'query' as source,
        timestamp '2023-03-11' as created_at,
        MAX(last_tx) as updated_at,
        'likely_bot_addresses' as model_name,
        'persona' as label_type
    FROM sender_transfer_rates
    GROUP BY 1,2,3,4,5,6,7,9,10
)

SELECT * FROM final_results

{% endmacro %}

{% macro get_likely_bot_contracts(chain, days_back=90) %}

WITH date_ranges AS (
    {{ get_date_ranges(chain, days_back) }}
),

sender_transfer_rates AS (
    SELECT 
        "to" as contract,
        COUNT(*) as num_txs,
        COUNT(DISTINCT "from") as num_senders,
        COUNT(*)/COUNT(DISTINCT "from") as txs_per_sender,
        COUNT(*) / (cast(date_diff('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0)) as txs_per_hour,
        MIN(block_time) as first_tx,
        MAX(block_time) as last_tx,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_erc20_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_nft_tfer_txs,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_token_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_erc20_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_nft_tfer_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('tokens', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}' AND token_standard = 'erc20'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) OR EXISTS (
            SELECT 1 FROM {{ source('nft', 'transfers') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_token_tfer_txs,
        -- DEX trade metrics
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('dex', 'trades') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as num_dex_trade_txs,
        cast(COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM {{ source('dex', 'trades') }} r 
            WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.blockchain = '{{ chain }}'
            AND r.block_time BETWEEN dr.start_time AND dr.end_time
            AND r.block_month BETWEEN date_trunc('month', dr.start_time) AND date_trunc('month', dr.end_time)
        ) THEN t.hash END) as double) / cast(COUNT(*) as double) as pct_dex_trade_txs,
        -- Future bot types (commented out until upstream models are migrated)
        0 as num_perp_trade_txs,
        0 as num_nft_trade_txs,
        0.0 as pct_perp_trade_txs,
        0.0 as pct_nft_trade_txs,
        -- Failed transaction metrics
        cast(COUNT(CASE WHEN success = false THEN 1 END) as double) / cast(COUNT(*) as double) as pct_failed_txs
    FROM {{ source(chain, 'transactions') }} t
    CROSS JOIN date_ranges dr
    WHERE t.block_time BETWEEN dr.start_time AND dr.end_time
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
        OR
        -- Failed transaction bots: > 100 txs and > 90% failed
        (COUNT(*) >= 100 AND cast(COUNT(CASE WHEN success = false THEN 1 END) as double) / cast(COUNT(*) as double) > 0.9)
),

final_results AS (
    SELECT 
        '{{ chain }}' as blockchain,
        contract as address,
        'likely bots' as category,
        CASE
            WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot contract'
            WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot contract'
            WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot contract'
            WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trading bot contract'
            WHEN pct_failed_txs > 0.9 THEN 'failed transaction bot contract'
            ELSE 'non-token bot contract'
        END as name,
        'msilb7' as contributor,
        'query' as source,
        timestamp '2023-03-11' as created_at,
        MAX(last_tx) as updated_at,
        'likely_bot_contracts' as model_name,
        'persona' as label_type
    FROM sender_transfer_rates
    GROUP BY 1,2,3,4,5,6,7,9,10
)

SELECT * FROM final_results

{% endmacro %} 