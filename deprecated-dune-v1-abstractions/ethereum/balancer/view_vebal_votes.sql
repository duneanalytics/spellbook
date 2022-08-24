BEGIN;
DROP VIEW IF EXISTS balancer.view_vebal_votes CASCADE;

CREATE VIEW balancer.view_vebal_votes AS

WITH calendar AS (
        SELECT generate_series('2022-04-07'::timestamptz, CURRENT_DATE, '1 week'::interval) AS start_date
    ),
    
    rounds_info AS (
        SELECT
            start_date,
            FLOOR(EXTRACT(EPOCH FROM start_date)) AS start_timestamp,
            start_date + '7d' AS end_date,
            FLOOR(EXTRACT(EPOCH FROM start_date + '7d')) AS end_timestamp,
            ROW_NUMBER() OVER (ORDER BY start_date) AS round_id
        FROM calendar
    ),
    
    votes_info AS (
        SELECT
            evt_block_time,
            block_timestamp,
            "user" AS provider,
            gauge_addr AS gauge,
            weight / 1e4 AS weight,
            unlocked_at,
            slope,
            bias
        FROM balancer."GaugeController_evt_VoteForGauge" v
        JOIN balancer.view_vebal_slopes d
        ON d.provider = v."user"
        AND d.block_number = (
            SELECT MAX(block_number)
            FROM balancer.view_vebal_slopes
            WHERE block_number <= v.evt_block_number
            AND provider = v."user"
        )
    ),
    
    votes_with_gaps AS (
        SELECT 
            *,
            LEAD(round_id::int, 1, 9999) OVER (PARTITION BY provider, gauge ORDER BY round_id) AS next_round
        FROM (
            SELECT
                COALESCE(round_id, 1) AS round_id,
                provider,
                gauge,
                weight,
                block_timestamp,
                unlocked_at,
                slope,
                bias
            FROM votes_info v
            LEFT JOIN rounds_info r
            ON v.evt_block_time >= r.start_date
            AND v.evt_block_time < r.end_date
        ) foo
    ),
    
    running_votes AS (
        SELECT
            r.round_id,
            r.start_date,
            r.end_date,
            r.end_timestamp,
            provider,
            bias,
            slope,
            block_timestamp,
            gauge,
            weight
        FROM rounds_info r
        LEFT JOIN votes_with_gaps v
        ON v.round_id <= r.round_id
        AND r.round_id < v.next_round
        AND r.end_timestamp <= v.unlocked_at
    )

SELECT
    round_id,
    start_date,
    end_date,
    gauge,
    provider,
    ((bias - slope * (end_timestamp - block_timestamp)) * weight) AS vote 
FROM running_votes;
COMMIT;