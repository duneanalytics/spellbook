CREATE OR REPLACE VIEW balancer.view_lbps AS
WITH lbp_pools AS (
        SELECT name, address AS pool
        FROM labels.labels
        WHERE "type" = 'balancer_lbp'
        AND author IN ('balancerlabs', 'markusbkoch', 'mangool', 'rabmarut')
    ),

    token_denorms AS (
        SELECT 
            name,
            contract_address AS pool, 
            SUM(denorm) AS denorm_sum 
        FROM balancer."BPool_call_bind" b
        INNER JOIN lbp_pools p  ON p.pool = b.contract_address
        GROUP BY 1, 2
    ),
    
    token_weights AS (
        SELECT name, pool, token, denorm / denorm_sum AS weight 
        FROM balancer."BPool_call_bind" b
        INNER JOIN token_denorms d ON d.pool = b.contract_address
    ),
    
    tokens_sold AS (
        SELECT name, pool, token, weight AS initial_weight
        FROM (SELECT name, pool, token, weight, ROW_NUMBER() OVER (PARTITION BY pool ORDER BY weight DESC) AS ranking
        FROM token_weights) w
        WHERE ranking = 1
    ),
    
    rebinds AS (
        SELECT t.*, denorm, call_block_number, call_block_time, ROW_NUMBER() OVER (PARTITION BY pool ORDER BY call_block_number) AS id
        FROM balancer."BPool_call_rebind" r INNER JOIN tokens_sold t ON t.pool = r.contract_address AND t.token = r.token WHERE call_success
    ),
    
    weights_changes AS (
        SELECT r1.name, r1.pool, r1.token, r1.initial_weight, r1.call_block_number, r1.call_block_time
        FROM rebinds r1
        JOIN rebinds r2 
        ON r1.id = r2.id - 1 AND r1.pool = r2.pool
        WHERE r1.denorm != r2.denorm 
    ),
    
    first_change AS (
        SELECT u.*
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY pool ORDER BY call_block_number) AS ranking
        FROM weights_changes) u
        WHERE ranking = 1
    ),
    
    last_change AS (
        SELECT u.*
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY pool ORDER BY call_block_number DESC) AS ranking
        FROM weights_changes) u
        WHERE ranking = 1
    ),
    
    limit_blocks AS (
        SELECT r.name, r.pool, r.token AS token_sold, e.symbol AS token_symbol, r.initial_weight, 
        f.call_block_number AS initial_block, f.call_block_time AS initial_time,
        COALESCE(u.call_block_number, r.call_block_number) AS final_block, 
        COALESCE(u.call_block_time, r.call_block_time) AS final_time
        FROM last_change r
        INNER JOIN first_change f ON f.pool = r.pool AND f.token = r.token
        LEFT JOIN balancer."BPool_call_unbind" u ON r.pool = u.contract_address AND r.token = u.token
        LEFT JOIN erc20.tokens e ON e.contract_address = r.token
    )
    
SELECT * FROM limit_blocks