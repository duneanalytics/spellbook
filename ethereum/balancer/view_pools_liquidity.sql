CREATE SCHEMA IF NOT EXISTS balancer;

CREATE
OR REPLACE VIEW balancer.view_pools_liquidity AS
SELECT
    DAY,
    pool_id AS pool,
    SUM(usd_amount) AS liquidity
FROM
    (
        SELECT
            *
        FROM
            balancer_v1.view_liquidity
        UNION
        ALL
        SELECT
            *
        FROM
            balancer_v2.view_liquidity
    ) l
GROUP BY
    1,
    2