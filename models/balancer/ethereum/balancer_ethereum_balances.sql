{{config(alias='balances')}} 

WITH calendar AS -- This CTE generates a series of continuous day values
    (
    SELECT explode(sequence(to_date('2020-02-28'), current_date, interval 1 day)) AS day -- 2020-02-28 is the day on which the first Balancer V1 pool was created by the factory contract
    ),
    
balancer_v1_pools AS -- This CTE returns the list of all pools that have been created by the Balancer V1 factory contract
    (
    SELECT
        pool as pools
    FROM {{ source('balancer_ethereum', 'BFactory_evt_LOG_NEW_POOL') }}
    ),

/*
balancer_supported_ERC20 AS -- This is a temporary placeholder for a label that whitelists all balancer supported ERC20 tokens
    (
    SELECT
        contract_address
    FROM balancer_ethereum.supported_erc20 
    ),
*/

balancer_inflows AS -- This CTE returns all the inflows of ERC20 to Balancer V1 factory created pools and to the Balancer V2 Vault contract
    (
    SELECT -- This query returns all the inflows of ERC20 to Balancer V1 factory created pools
        p.pools as pool,
        date_trunc('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        SUM(e.value) AS amount
    FROM
        (
        SELECT -- This subquery optimises the greater query by decreasing the data set from erc20_ethereum.evt_transfer
            `to` AS address,
            evt_block_time,
            contract_address,
            value
        FROM {{ source('erc20_ethereum', 'evt_transfer') }}
        WHERE evt_block_time >= '2020-02-28 00:00' -- 2020-02-28 is the day on which the first Balancer V1 pool was created by the factory contract
        ) e
    INNER JOIN balancer_v1_pools p
        ON e.address = p.pools
    -- WHERE e.contract_address IN (SELECT * FROM balancer_supported_ERC20) -- This is a temporary placeholder for an ERC20 filter
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT -- This query returns all the inflows of ERC20 to the Balancer V2 vault contract
        e.address as pool,
        date_trunc('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        SUM(e.value) AS amount
    FROM
        (
        SELECT -- This subquery optimises the greater query by decreasing the data set from erc20_ethereum.evt_transfer
            `to` AS address,
            evt_block_time,
            contract_address,
            value
        FROM {{ source('erc20_ethereum', 'evt_transfer') }}
        WHERE evt_block_time >= '2021-04-21 00:00' -- 2021-04-21 is the day on which the first Balancer V2 inbound transaction was effected
        ) e
    WHERE e.address = '0xba12222222228d8ba445958a75a0704d566bf2c8' -- Balancer V2 vault contract
    -- AND e.contract_address IN (SELECT * FROM balancer_supported_ERC20) -- This is a temporary placeholder for an ERC20 filter
    GROUP BY 1, 2, 3
    ),

balancer_outflows AS -- This CTE returns all the outflows of ERC20 from Balancer V1 factory created pools and from the Balancer V2 Vault
    (
    SELECT -- This query returns all the outflows of ERC20 from Balancer V1 factory created pools
        p.pools as pool,
        date_trunc('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        -SUM(e.value) AS amount
    FROM
        (
        SELECT -- This subquery optimises the greater query by decreasing the data set from erc20_ethereum.evt_transfer
            `from` AS address,
            evt_block_time,
            contract_address,
            value
        FROM {{ source('erc20_ethereum', 'evt_transfer') }}
        WHERE evt_block_time >= '2020-02-28 00:00' -- 2020-02-28 is the day on which the first Balancer V1 pool was created by the factory contract
        ) e
    INNER JOIN balancer_v1_pools p
        ON e.address = p.pools
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT -- This query returns all the outflows of ERC20 from the Balancer V2 vault contract
        e.address as pool,
        date_trunc('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        -SUM(e.value) AS amount
    FROM
        (
        SELECT -- This subquery optimises the greater query by decreasing the data set from erc20_ethereum.evt_transfer
            `from` AS address,
            evt_block_time,
            contract_address,
            value
        FROM {{ source('erc20_ethereum', 'evt_transfer') }}
        WHERE evt_block_time >= '2021-04-21 00:00' -- 2021-04-21 is the day on which the first Balancer V2 inbound transaction was effected
        ) e
    WHERE e.address = '0xba12222222228d8ba445958a75a0704d566bf2c8' -- Balancer V2 vault contract
    GROUP BY 1, 2, 3
    ) ,

running_cumulative_balances_by_token AS  -- This CTE returns all the cumulative daily balances of tokens of all Balancer V1 pools and the Balancer V2 vault
    (
    SELECT
        c.day,
        z.pool,
        z.token,
        z.cumulative_amount
    FROM calendar c
    LEFT JOIN
        (
        SELECT  -- This sub query calculates the daily cumulative changes in token balances by token and by pool
            y.pool,
            y.day,
            y.token,
            Lead(y.day, 1, now()) OVER (PARTITION BY y.token, y.pool ORDER BY y.day ASC) AS day_of_next_change, -- Extrapolated from last values
            SUM(y.amount) OVER (PARTITION BY y.pool, y.token ORDER BY y.day ASC) AS cumulative_amount
        FROM
            (
            SELECT  -- This sub query performs a SUM function on inflows and outflows to get net daily flows to/from Balancer
                x.pool,
                x.day,
                x.token,
                SUM(COALESCE(x.amount, 0)) AS amount
            FROM
                (
                SELECT * -- This sub query aggregates all inflows and outflows from the Balancer inflow and outflow CTEs
                FROM balancer_inflows
                
                UNION ALL
                
                SELECT * 
                FROM balancer_outflows
                ) x
            GROUP BY 1, 2, 3
            ) y
        ) z
        ON z.day <= c.day -- Logic to ensure proper extrapolation
        AND c.day < z.day_of_next_change -- Logic to ensure proper extrapolation
    ORDER BY c.day DESC
    )

SELECT *
FROM running_cumulative_balances_by_token;