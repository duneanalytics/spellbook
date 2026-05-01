{{ config(
    schema = 'aptos_stablecoins',
    alias = 'usdc_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'from_index', 'to_index'],
    partition_by = ['block_month'],
    post_hook='{{ expose_spells(blockchains = \'["aptos"]\',
        spell_type = "project",
        spell_name = "aptos_stablecoins",
        contributors = \'["ying-w"]\') }}'
) }}
WITH events AS (
    SELECT
        *,
    CASE -- start with moving money out of store
        WHEN activity_type = 'Withdraw' THEN -amount
        WHEN activity_type = 'Mint' THEN -amount
        ELSE amount -- Deposit or Burn
    END AS net_amount
    FROM {{ ref('aptos_usdc_volume') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
), cumulative AS (
    SELECT *,
        SUM(net_amount) OVER (PARTITION BY tx_version ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_tracker
    FROM events
), sessioning AS (
    SELECT *,
        COALESCE(SUM(IF(balance_tracker >= 0, 1, 0)) OVER (PARTITION BY tx_version ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS session_id,
        balance_tracker > 0 AS missing_event -- qc
    FROM cumulative
), session_sum AS (
    SELECT *,
        SUM(amount) OVER (
            PARTITION BY tx_version, session_id, IF(activity_type IN ('Withdraw', 'Mint'), 1, 0)
            ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS amount_csum,
        SUM(amount) OVER (
            PARTITION BY tx_version, session_id, IF(activity_type IN ('Withdraw', 'Mint'), 1, 0)
            ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS amount_csum_prev
    FROM sessioning
), session_counter AS (
    SELECT
        tx_version,
        tx_hash,
        session_id,
        COUNT(1) AS n_events,
        SUM(IF(activity_type = 'Withdraw', 1, 0)) AS n_withdraw,
        SUM(IF(activity_type = 'Deposit', 1, 0)) AS n_deposit,
        SUM(IF(activity_type = 'Mint', 1, 0)) AS n_mint,
        SUM(IF(activity_type = 'Burn', 1, 0)) AS n_burn,
        MAX(amount) AS amount_max,
        MAX(IF(activity_type = 'Withdraw', amount, 0)) AS max_withdraw,
        MAX(IF(activity_type = 'Deposit', amount, 0)) AS max_deposit,
        MAX(IF(activity_type = 'Mint', amount, 0)) AS max_mint,
        MAX(IF(activity_type = 'Burn', amount, 0)) AS max_burn
    FROM sessioning
    GROUP BY tx_version, tx_hash, session_id
), transfers_multi_fifo AS (
    SELECT
        f.block_date,
        f.block_time,
        date(date_trunc('month', f.block_time)) as block_month,
        f.tx_version,
        f.tx_hash,
        f.session_id,
        f.store_owner AS from_account,
        f.fungible_store AS from_store, -- is same as to_store on Mint
        t.store_owner AS to_account,
        t.fungible_store AS to_store, -- can be null on Burn
        LEAST(
            LEAST(t.amount_csum - COALESCE(f.amount_csum_prev, 0), t.amount),
            LEAST(f.amount_csum - COALESCE(t.amount_csum_prev, 0), f.amount)
        ) AS amount,
        -- f.amount_csum AS from_amount_csum,
        -- t.amount_csum AS to_amount_csum,
        -- f.amount_csum_prev AS from_amount_csum_prev,
        -- t.amount_csum_prev AS to_amount_csum_prev,
        -- f.amount AS from_amount,
        -- t.amount AS to_amount,
        f.event_index AS from_index,
        t.event_index AS to_index,
        sc.n_withdraw + sc.n_mint > 1 AS from_many,
        sc.n_deposit + sc.n_burn > 1 AS to_many,
        sc.n_mint > 0 AS has_mint,
        sc.n_burn > 0 AS has_burn
    FROM (SELECT * FROM session_sum WHERE activity_type IN ('Withdraw', 'Mint')) f
    LEFT JOIN (SELECT * FROM session_sum WHERE activity_type IN ('Deposit', 'Burn')) t
    ON f.tx_version = t.tx_version
    AND f.session_id = t.session_id
    LEFT JOIN session_counter sc
    ON f.tx_version = sc.tx_version
    AND f.session_id = sc.session_id
    WHERE 1=1
    AND f.amount_csum > COALESCE(t.amount_csum_prev,0)
    AND t.amount_csum > COALESCE(f.amount_csum_prev,0)
)

SELECT *, ROW_NUMBER() OVER (PARTITION BY tx_version ORDER BY amount DESC) AS amount_rank
FROM transfers_multi_fifo
-- WHERE from_store != COALESCE(to_store, '') -- exclude self transfers, ex. 2891465845
