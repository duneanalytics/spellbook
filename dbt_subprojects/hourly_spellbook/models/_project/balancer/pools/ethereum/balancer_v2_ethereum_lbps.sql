{{
    config(
tags=['prod_exclude'],
        schema = 'balancer_v2_ethereum',
        alias = 'lbps',
        unique_key = ['pool_id'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

{% set create_lbp_contracts = [
                                source('balancer_v2_ethereum', 'LiquidityBootstrappingPoolFactory_call_create'),
                                source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create')
                                ] %}
{% set lbp_weight_upgrade_contracts = [
                                source('balancer_v2_ethereum', 'LiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled'),
                                source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled')
                                ] %}
{% set non_lbp_tokens = ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                        '0x6b175474e89094c44da98b954eedeac495271d0f',
                        '0x88acdd2a6425c3faae4bc9650fd7e27e0bebb7ab',
                        '0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5'
                        ) %} -- WETH, USDC, DAI, MIST & OHM

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
            tokens,
            lower(symbol) AS name,
            poolId AS pool_id,
            SUBSTRING(poolId, 0, 42) AS pool_address
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
                from_unixtime(startTime, 'yyyy-MM-dd HH:mm') AS start_time,
                from_unixtime(endTime, 'yyyy-MM-dd HH:mm') AS end_time,
                startWeights AS start_weights,
                ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_time DESC) AS ranking
            FROM lbps_weight_update c
        ) w
        WHERE ranking = 1
    ),

    zipped_lbps_tokens_weights AS (
        SELECT 
            name,
            pool_id,
            l.pool_address,
            start_time,
            end_time,
            explode(arrays_zip(tokens, start_weights)) AS zipped
        FROM lbps_list l
        LEFT JOIN last_weight_update w
        ON w.pool_address = l.pool_address
    ),
    
    lbps_tokens_weights AS (
        SELECT 
            name,
            pool_id,
            pool_address,
            start_time,
            end_time,
            zipped.tokens AS token,
            zipped.start_weights AS start_weight
        FROM zipped_lbps_tokens_weights
    ),
    
    lbps_info AS (
        SELECT 
            *
        FROM (
            SELECT 
                *,
               ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY start_weight DESC) AS ranking 
            FROM lbps_tokens_weights
            WHERE token NOT IN {{ non_lbp_tokens }}
        ) l
        WHERE ranking = 1
    )
    
    SELECT 
        name,
        pool_id,
        token AS token_sold,
        t.symbol AS token_symbol,
        start_time,
        COALESCE(end_time, '2999-01-01') AS end_time
    FROM lbps_info l
    LEFT JOIN {{ source('tokens', 'erc20') }} t
    ON l.token = t.contract_address
    AND t.blockchain = 'ethereum'
    ORDER BY pool_id
    
