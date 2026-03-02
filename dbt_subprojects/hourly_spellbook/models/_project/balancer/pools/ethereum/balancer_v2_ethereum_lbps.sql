{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'lbps',
        unique_key = ['pool_id'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

{% set create_lbp_contracts = [
                                source('balancer_v2_ethereum', 'LiquidityBootstrappingPoolFactory_call_create'),
                                source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create')
                                ] %}
{% set lbp_weight_upgrade_contracts = [
                                source('balancer_v2_ethereum', 'LiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled'),
                                source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled')
                                ] %}

WITH lbps_call_create AS (
        {% for create_lbp_contract in create_lbp_contracts %}
        SELECT * FROM {{ create_lbp_contract }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ),

    lbps_list AS (
        SELECT
            cc.tokens,
            lower(cc.symbol) AS name,
            c.poolId AS pool_id,
            bytearray_substring(c.poolId, 1, 20) AS pool_address
        FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} c
        INNER JOIN lbps_call_create cc
            ON c.evt_tx_hash = cc.call_tx_hash
            AND cc.call_success
    ),

    lbps_weight_update AS (
        {% for lbp_weight_upgrade_contract in lbp_weight_upgrade_contracts %}
        SELECT * FROM {{ lbp_weight_upgrade_contract }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ),

    last_weight_update AS (
        SELECT *
        FROM (
            SELECT
                contract_address AS pool_address,
                date_format(from_unixtime(cast(startTime AS bigint)), '%Y-%m-%d %H:%i') AS start_time,
                date_format(from_unixtime(cast(endTime AS bigint)), '%Y-%m-%d %H:%i') AS end_time,
                startWeights AS start_weights,
                ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_time DESC) AS ranking
            FROM lbps_weight_update
        ) w
        WHERE ranking = 1
    ),

    lbps_tokens_weights AS (
        SELECT
            l.name,
            l.pool_id,
            l.pool_address,
            w.start_time,
            w.end_time,
            t.token,
            t.start_weight
        FROM lbps_list l
        INNER JOIN last_weight_update w ON w.pool_address = l.pool_address
        CROSS JOIN UNNEST(l.tokens, w.start_weights) AS t(token, start_weight)
    ),

    lbps_info AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY start_weight DESC) AS ranking
            FROM lbps_tokens_weights
            WHERE token NOT IN (
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, -- WETH
                0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, -- USDC
                0x6b175474e89094c44da98b954eedeac495271d0f, -- DAI
                0x88acdd2a6425c3faae4bc9650fd7e27e0bebb7ab, -- MIST
                0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5  -- OHM
            )
        ) l
        WHERE ranking = 1
    )

SELECT
    l.name,
    l.pool_id,
    l.token AS token_sold,
    t.symbol AS token_symbol,
    l.start_time,
    COALESCE(l.end_time, '2999-01-01') AS end_time
FROM lbps_info l
LEFT JOIN {{ source('tokens', 'erc20') }} t
    ON l.token = t.contract_address
    AND t.blockchain = 'ethereum'
ORDER BY l.pool_id

