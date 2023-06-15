{{ config(
    alias = 'daily_deposits'
    )
}}

WITH 

deposit_events AS (
    SELECT
        collateralId as collateral_id, 
        tokenId AS collateral_token_id,
        tokenContract AS collateral_token_contract,
        evt_block_time AS event_time,
        1 AS balance_change
    FROM
    {{source('astaria_ethereum', 'CollateralToken_evt_Deposit721')}}
    -- {% if is_incremental() %} 
    -- WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    -- {% endif %}
),

release_events AS (
    SELECT
        assetId AS collateral_token_id,
        underlyingAsset AS collateral_token_contract,
        evt_block_time AS event_time,
        -1 AS balance_change
    FROM
    {{source('astaria_ethereum', 'CollateralToken_evt_ReleaseTo')}}
    -- {% if is_incremental() %} -- commenting out since can;t figure an incremental version without making the query complex
    -- WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    -- {% endif %}
    --     astaria_ethereum.CollateralToken_evt_ReleaseTo
),

all_events AS (
    SELECT collateral_token_id, collateral_token_contract, event_time, balance_change FROM deposit_events
    UNION ALL
    SELECT collateral_token_id, collateral_token_contract, event_time, balance_change FROM release_events
),

    days as (
        select
            explode(
                sequence(
                    to_date('2023-04-27'), date_trunc('day', now()), interval 1 day
                )
            ) as day
    ),

rolling_balance AS (
    SELECT 
        date_trunc('day', event_time) AS day,
        collateral_token_id,
        collateral_token_contract,
        SUM(balance_change) OVER (
            PARTITION BY collateral_token_id, collateral_token_contract 
            ORDER BY date_trunc('day', event_time)
        ) AS balance_over_time,
        lead(date_trunc('day', event_time), 1, current_timestamp) OVER (
            PARTITION BY collateral_token_id, collateral_token_contract 
            ORDER BY date_trunc('day', event_time)
        ) AS next_day
    FROM 
        all_events
),

daily_balances AS (
    SELECT 
        d.day, 
        rb.collateral_token_id,
        rb.collateral_token_contract,
        COALESCE(SUM(rb.balance_over_time), 0) as daily_balance
    FROM 
        rolling_balance rb 
    INNER JOIN 
        days d 
        ON rb.day <= d.day 
        AND d.day < rb.next_day
    GROUP BY 1, 2, 3
), 


nft_data AS (
    SELECT
        contract_address,
        name AS nft_collection,
        symbol AS nft_symbol,
        standard as nft_token_standard
    FROM
    {{ ref('tokens_ethereum_nft') }}
),

collateral_ids AS (
    SELECT
        DISTINCT collateral_token_id,
        collateral_token_contract,
        collateral_Id
    FROM
        deposit_events
)

SELECT 
    db.day, 
    db.collateral_token_id,
    db.collateral_token_contract,
    db.daily_balance,
    nd.nft_collection,
    nd.nft_symbol,
    nd.nft_token_standard,
    ci.collateral_Id
FROM 
    daily_balances db
LEFT JOIN
    nft_data nd
    ON db.collateral_token_contract = nd.contract_address
LEFT JOIN
    collateral_ids ci
    ON db.collateral_token_id = ci.collateral_token_id AND db.collateral_token_contract = ci.collateral_token_contract