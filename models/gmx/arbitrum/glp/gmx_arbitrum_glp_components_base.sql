{{ config(
    alias = alias('glp_components_base'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'minute'],
        )
}}

{% set project_start_date = '2021-08-31 08:13' %}

WITH minute AS  -- This CTE generates a series of minute values
    (
    SELECT *
    FROM
        (
        {% if not is_incremental() %}
        SELECT explode(sequence(TIMESTAMP '{{project_start_date}}', CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute -- 2021-08-31 08:13 is the timestamp of the first vault transaction
        {% endif %}
        {% if is_incremental() %}
        SELECT explode(sequence(date_trunc("day", now() - interval '1 week'), CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute
        {% endif %}
        )
    ),

/*
poolAmounts are the amount of supported tokens that are located in the GMX vault.
https://arbiscan.io/address/0x489ee077994B6658eAfA855C308275EAd8097C4A
*/

glp_frax_poolAmounts AS -- This CTE returns the average amount of FRAX tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of FRAX tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of FRAX tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0x17fc002b466eec40dae837fc4be5c67993ddbd6f' -- FRAX Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_usdt_poolAmounts AS -- This CTE returns the average amount of USDT tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of USDT tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of USDT tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9' -- USDT Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_wbtc_poolAmounts AS -- This CTE returns the average amount of WBTC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of WBTC tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of WBTC tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_usdc_poolAmounts AS -- This CTE returns the average amount of USDC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of USDC tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of USDC tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_uni_poolAmounts AS -- This CTE returns the average amount of UNI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of UNI tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of UNI tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_link_poolAmounts AS -- This CTE returns the average amount of LINK tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of LINK tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of LINK tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_weth_poolAmounts AS -- This CTE returns the average amount of WETH tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of WETH tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of WETH tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_dai_poolAmounts AS -- This CTE returns the average amount of DAI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of DAI tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of DAI tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_poolAmounts')}}
        WHERE _0 = '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' -- DAI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

/*
reservedAmounts are the amount of supported tokens that are reserved to cover long positions on the supported tokens.
FRAX, USDT, USDC and DAI are not included as you cannot open shorts on those tokens.
*/

glp_wbtc_reservedAmounts AS -- This CTE returns the average amount of reserved WBTC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of reserved WBTC tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of reserved WBTC tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_reservedAmounts')}}
        WHERE _0 = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_uni_reservedAmounts AS -- This CTE returns the average amount of reserved UNI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of reserved UNI tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of reserved UNI tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_reservedAmounts')}}
        WHERE _0 = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_link_reservedAmounts AS -- This CTE returns the average amount of reserved LINK tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of reserved LINK tokens in the pool if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of reserved LINK tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_reservedAmounts')}}
        WHERE _0 = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_weth_reservedAmounts AS -- This CTE returns the average amount of reserved WETH tokens in the pool for a designated minute
    (
    SELECT
        a.minute, -- This query averages the amount of reserved WETH tokens in the pool if more than one transaction is reccorded for a designated minute
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the amount of reserved WETH tokens in the pool
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_reservedAmounts')}}
        WHERE _0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

/*
guaranteedUSD are the total value of the long positions valued at the time of position entry.
This is because as soon as a long position opens, the asset's USD value (that's currently in the pool) effectively gets locked in.
FRAX, USDT, USDC and DAI are not included as you cannot open shorts on those tokens.
*/

glp_wbtc_guaranteedUsd AS -- This CTE returns the guaranteed USD amount against WBTC tokens in the pool for a designated minute
    (
    SELECT
            a.minute, -- This query averages the amount of guaranteed USD against WBTC tokens in the pool if more than one transaction is reccorded for a designated minute
            AVG(a.amount) AS amount
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches the amount of guaranteed USD against WBTC tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_guaranteedUsd')}}
            WHERE _0 = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_uni_guaranteedUsd AS -- This CTE returns the guaranteed USD amount against UNI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of guaranteed USD against UNI tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS amount
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches the amount of guaranteed USD against UNI tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_guaranteedUsd')}}
            WHERE _0 = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_link_guaranteedUsd AS -- This CTE returns the guaranteed USD amount against LINK tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of guaranteed USD against LINK tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS amount
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches the amount of guaranteed USD against LINK tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_guaranteedUsd')}}
            WHERE _0 = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_weth_guaranteedUsd AS -- This CTE returns the guaranteed USD amount against WETH tokens in the pool for a designated minute
    (
    SELECT -- This query averages the amount of guaranteed USD against WETH tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS amount
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches the amount of guaranteed USD against WETH tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_guaranteedUsd')}}
            WHERE _0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

/*
getMaxPrice returns the maximum price of a supported token in the vault from the vault price feed contract.
*/

glp_frax_getMaxPrice AS -- This CTE returns the maximum price of FRAX tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of FRAX tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of FRAX tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0x17fc002b466eec40dae837fc4be5c67993ddbd6f' -- FRAX Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_usdt_getMaxPrice AS -- This CTE returns the maximum price of USDT tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of USDT tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of USDT tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9' -- USDT Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_wbtc_getMaxPrice AS -- This CTE returns the maximum price of WBTC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of WBTC tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of WBTC tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_usdc_getMaxPrice AS -- This CTE returns the maximum price of USDC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of USDC tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of USDC tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_uni_getMaxPrice AS -- This CTE returns the maximum price of UNI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of UNI tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of UNI tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_link_getMaxPrice AS -- This CTE returns the maximum price of LINK tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of LINK tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of LINK tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_weth_getMaxPrice AS -- This CTE returns the maximum price of WETH tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of WETH tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of WETH tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_dai_getMaxPrice AS -- This CTE returns the maximum price of DAI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the maximum price of DAI tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches maximum price of DAI tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMaxPrice')}}
            WHERE _token = '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' -- DAI Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

/*
getMinPrice returns the maximum price of a supported token in the vault from the vault price feed contract.
*/

glp_frax_getMinPrice AS -- This CTE returns the minimum price of FRAX tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of FRAX tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of FRAX tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0x17fc002b466eec40dae837fc4be5c67993ddbd6f' -- FRAX Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

glp_usdt_getMinPrice AS -- This CTE returns the minimum price of USDT tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of USDT tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of USDT tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9' -- USDT Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_wbtc_getMinPrice AS -- This CTE returns the minimum price of WBTC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of WBTC tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of WBTC tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_usdc_getMinPrice AS -- This CTE returns the minimum price of USDC tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of USDC tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of USDC tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_uni_getMinPrice AS -- This CTE returns the minimum price of UNI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of UNI tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of UNI tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_link_getMinPrice AS -- This CTE returns the minimum price of LINK tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of LINK tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of LINK tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_weth_getMinPrice AS -- This CTE returns the minimum price of WETH tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of WETH tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of WETH tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,
    
glp_dai_getMinPrice AS -- This CTE returns the minimum price of DAI tokens in the pool for a designated minute
    (
    SELECT -- This query averages the minimum price of DAI tokens in the pool if more than one transaction is reccorded for a designated minute
            a.minute,
            AVG(a.amount) AS price
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and fetches minimum price of DAI tokens in the pool
                date_trunc('minute', call_block_time) AS minute,
                output_0/1e18 AS amount
            FROM {{source('gmx_arbitrum', 'Vault_call_getMinPrice')}}
            WHERE _token = '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' -- DAI Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND call_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
    ) ,

/*
globalShortAveragePrices returns the volume weighted average price of all shorts.
FRAX, USDT, USDC and DAI are not included as you cannot open shorts on those tokens.
*/

glp_wbtc_globalShortAveragePrices AS -- This CTE returns volume weighted average price of all WBTC shorts for a designated minute
    (
    SELECT -- This query averages the volume weighted average price of all WBTC shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.price) AS price
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches volume weighted average price of all WBTC shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS price
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortAveragePrices')}}
        WHERE _0 = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_uni_globalShortAveragePrices AS -- This CTE returns volume weighted average price of all UNI shorts for a designated minute
    (
    SELECT -- This query averages the volume weighted average price of all UNI shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.price) AS price
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches volume weighted average price of all UNI shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS price
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortAveragePrices')}}
        WHERE _0 = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_link_globalShortAveragePrices AS -- This CTE returns volume weighted average price of all LINK shorts for a designated minute
    (
    SELECT -- This query averages the volume weighted average price of all LINK shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.price) AS price
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches volume weighted average price of all LINK shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS price
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortAveragePrices')}}
        WHERE _0 = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_weth_globalShortAveragePrices AS -- This CTE returns volume weighted average price of all WETH shorts for a designated minute
    (
    SELECT -- This query averages the volume weighted average price of all WETH shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.price) AS price
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches volume weighted average price of all WETH shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS price
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortAveragePrices')}}
        WHERE _0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

/*
globalShortSizes returns the sum of all shorts reported in the asset currency.
FRAX, USDT, USDC and DAI are not included as you cannot open shorts on those tokens.
*/

glp_wbtc_globalShortSizes AS -- This CTE returns average sum of all WBTC shorts for a designated minute
    (
    SELECT -- This query averages sum of all WBTC shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the sum of all WBTC shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortSizes')}}
        WHERE _0 = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,

glp_uni_globalShortSizes AS -- This CTE returns average sum of all UNI shorts for a designated minute
    (
    SELECT -- This query averages sum of all UNI shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the sum of all UNI shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortSizes')}}
        WHERE _0 = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_link_globalShortSizes AS -- This CTE returns average sum of all LINK shorts for a designated minute
    (
    SELECT -- This query averages sum of all LINK shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the sum of all LINK shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortSizes')}}
        WHERE _0 = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    ) ,
    
glp_weth_globalShortSizes AS -- This CTE returns average sum of all WETH shorts for a designated minute
    (
    SELECT -- This query averages sum of all WETH shorts if more than one transaction is reccorded for a designated minute
        a.minute,
        AVG(a.amount) AS amount
    FROM
        (
        SELECT -- This subquery truncates the block time to a minute and fetches the sum of all WETH shorts
            date_trunc('minute', call_block_time) AS minute,
            output_0 AS amount
        FROM {{source('gmx_arbitrum', 'Vault_call_globalShortSizes')}}
        WHERE _0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND call_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    GROUP BY a.minute
    )

SELECT  -- This subquery collates calculates the value of each components required to derrive AUM data
    y.minute,
    TRY_CAST(date_trunc('DAY', y.minute) AS date) AS block_date,
    
    y.frax_poolAmounts/1e18 AS frax_available_assets, -- FRAX Pool Amounts - Decimal Places 18
    (0.5 * ((y.frax_getMaxPrice + y.frax_getMinPrice) + ABS(y.frax_getMaxPrice - y.frax_getMinPrice)))/1e12 AS frax_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    y.usdt_poolAmounts/1e6 AS usdt_available_assets, -- USDT Pool Amounts - Decimal Places 6
    (0.5 * ((y.usdt_getMaxPrice + y.usdt_getMinPrice) + ABS(y.usdt_getMaxPrice - y.usdt_getMinPrice)))/1e12 AS usdt_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    (y.wbtc_poolAmounts - y.wbtc_reservedAmounts)/1e8 AS wbtc_available_assets, -- WBTC Available Assets - Decimal Places 8
    y.wbtc_guaranteedUsd/1e30 AS wbtc_longs, --USDG Decimal Places 30
    (0.5 * ((y.wbtc_getMaxPrice + y.wbtc_getMinPrice) + ABS(y.wbtc_getMaxPrice - y.wbtc_getMinPrice)))/1e12 AS wbtc_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    y.wbtc_globalShortAveragePrices/1e30 AS wbtc_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    y.wbtc_globalShortSizes/1e30 AS wbtc_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    y.usdc_poolAmounts/1e6 AS usdc_available_assets, -- USDC Pool Amounts - Decimal Places 6
    (0.5 * ((y.usdc_getMaxPrice + y.usdc_getMinPrice) + ABS(y.usdc_getMaxPrice - y.usdc_getMinPrice)))/1e12 AS usdc_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    (y.uni_poolAmounts - y.uni_reservedAmounts)/1e18 AS uni_available_assets, -- UNI Available Assets - Decimal Places 8
    y.uni_guaranteedUsd/1e30 AS uni_longs, --USDG Decimal Places 30
    (0.5 * ((y.uni_getMaxPrice + y.uni_getMinPrice) + ABS(y.uni_getMaxPrice - y.uni_getMinPrice)))/1e12 AS uni_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    y.uni_globalShortAveragePrices/1e30 AS uni_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    y.uni_globalShortSizes/1e30 AS uni_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    (y.link_poolAmounts - y.link_reservedAmounts)/1e18 AS link_available_assets, -- UNI Available Assets - Decimal Places 8
    y.link_guaranteedUsd/1e30 AS link_longs, --USDG Decimal Places 30
    (0.5 * ((y.link_getMaxPrice + y.link_getMinPrice) + ABS(y.link_getMaxPrice - y.link_getMinPrice)))/1e12 AS link_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    y.link_globalShortAveragePrices/1e30 AS link_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    y.link_globalShortSizes/1e30 AS link_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    (y.weth_poolAmounts - y.weth_reservedAmounts)/1e18 AS weth_available_assets, -- WETH Available Assets - Decimal Places 18
    y.weth_guaranteedUsd/1e30 AS weth_longs, --USDG Decimal Places 30
    (0.5 * ((y.weth_getMaxPrice + y.weth_getMinPrice) + ABS(y.weth_getMaxPrice - y.weth_getMinPrice)))/1e12 AS weth_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    y.weth_globalShortAveragePrices/1e30 AS weth_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    y.weth_globalShortSizes/1e30 AS weth_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    y.dai_poolAmounts/1e18 AS dai_available_assets, -- DAI Pool Amounts - Decimal Places 18
    (0.5 * ((y.dai_getMaxPrice + y.dai_getMinPrice) + ABS(y.dai_getMaxPrice - y.dai_getMinPrice)))/1e12 AS dai_current_price -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
FROM
    (
    SELECT -- This subquery removes null values
        x.minute,
        
        COALESCE(x.frax_poolAmounts,0) AS frax_poolAmounts,
        COALESCE(x.frax_getMaxPrice,0) AS frax_getMaxPrice,
        COALESCE(x.frax_getMinPrice,0) AS frax_getMinPrice,
        
        COALESCE(x.usdt_poolAmounts,0) AS usdt_poolAmounts,
        COALESCE(x.usdt_getMaxPrice,0) AS usdt_getMaxPrice,
        COALESCE(x.usdt_getMinPrice,0) AS usdt_getMinPrice,
        
        COALESCE(x.wbtc_poolAmounts,0) AS wbtc_poolAmounts,
        COALESCE(x.wbtc_reservedAmounts,0) AS wbtc_reservedAmounts,
        COALESCE(x.wbtc_guaranteedUsd,0) AS wbtc_guaranteedUsd,
        COALESCE(x.wbtc_getMaxPrice,0) AS wbtc_getMaxPrice,
        COALESCE(x.wbtc_getMinPrice,0) AS wbtc_getMinPrice,
        COALESCE(x.wbtc_globalShortAveragePrices,0) AS wbtc_globalShortAveragePrices,
        COALESCE(x.wbtc_globalShortSizes,0) AS wbtc_globalShortSizes,
        
        COALESCE(x.usdc_poolAmounts,0) AS usdc_poolAmounts,
        COALESCE(x.usdc_getMaxPrice,0) AS usdc_getMaxPrice,
        COALESCE(x.usdc_getMinPrice,0) AS usdc_getMinPrice,
        
        COALESCE(x.uni_poolAmounts,0) AS uni_poolAmounts,
        COALESCE(x.uni_reservedAmounts,0) AS uni_reservedAmounts,
        COALESCE(x.uni_guaranteedUsd,0) AS uni_guaranteedUsd,
        COALESCE(x.uni_getMaxPrice,0) AS uni_getMaxPrice,
        COALESCE(x.uni_getMinPrice,0) AS uni_getMinPrice,
        COALESCE(x.uni_globalShortAveragePrices,0) AS uni_globalShortAveragePrices,
        COALESCE(x.uni_globalShortSizes,0) AS uni_globalShortSizes,
        
        COALESCE(x.link_poolAmounts,0) AS link_poolAmounts,
        COALESCE(x.link_reservedAmounts,0) AS link_reservedAmounts,
        COALESCE(x.link_guaranteedUsd,0) AS link_guaranteedUsd,
        COALESCE(x.link_getMaxPrice,0) AS link_getMaxPrice,
        COALESCE(x.link_getMinPrice,0) AS link_getMinPrice,
        COALESCE(x.link_globalShortAveragePrices,0) AS link_globalShortAveragePrices,
        COALESCE(x.link_globalShortSizes,0) AS link_globalShortSizes,
        
        COALESCE(x.weth_poolAmounts,0) AS weth_poolAmounts,
        COALESCE(x.weth_reservedAmounts,0) AS weth_reservedAmounts,
        COALESCE(x.weth_guaranteedUsd,0) AS weth_guaranteedUsd,
        COALESCE(x.weth_getMaxPrice,0) AS weth_getMaxPrice,
        COALESCE(x.weth_getMinPrice,0) AS weth_getMinPrice,
        COALESCE(x.weth_globalShortAveragePrices,0) AS weth_globalShortAveragePrices,
        COALESCE(x.weth_globalShortSizes,0) AS weth_globalShortSizes,
        
        COALESCE(x.dai_poolAmounts,0) AS dai_poolAmounts,
        COALESCE(x.dai_getMaxPrice,0) AS dai_getMaxPrice,
        COALESCE(x.dai_getMinPrice,0) AS dai_getMinPrice
    FROM
        (
        SELECT -- This subquery collates all the data extracted from the vault contract functions, joins them to the minute series, and uses last data to extrapolate over null values
            a.minute,
            
            last(b1.amount, true) OVER (ORDER BY a.minute ASC) AS frax_poolAmounts,
            last(b2.price, true) OVER (ORDER BY a.minute ASC) AS frax_getMaxPrice,
            last(b3.price, true) OVER (ORDER BY a.minute ASC) AS frax_getMinPrice,
            
            last(c1.amount, true) OVER (ORDER BY a.minute ASC) AS usdt_poolAmounts,
            last(c2.price, true) OVER (ORDER BY a.minute ASC) AS usdt_getMaxPrice,
            last(c3.price, true) OVER (ORDER BY a.minute ASC) AS usdt_getMinPrice,
            
            last(d1.amount, true) OVER (ORDER BY a.minute ASC) AS wbtc_poolAmounts,
            last(d2.amount, true) OVER (ORDER BY a.minute ASC) AS wbtc_reservedAmounts,
            last(d3.amount, true) OVER (ORDER BY a.minute ASC) AS wbtc_guaranteedUsd,
            last(d4.price, true) OVER (ORDER BY a.minute ASC) AS wbtc_getMaxPrice,
            last(d5.price, true) OVER (ORDER BY a.minute ASC) AS wbtc_getMinPrice,
            last(d6.price, true) OVER (ORDER BY a.minute ASC) AS wbtc_globalShortAveragePrices,
            last(d7.amount, true) OVER (ORDER BY a.minute ASC) AS wbtc_globalShortSizes,
            
            last(e1.amount, true) OVER (ORDER BY a.minute ASC) AS usdc_poolAmounts,
            last(e2.price, true) OVER (ORDER BY a.minute ASC) AS usdc_getMaxPrice,
            last(e3.price, true) OVER (ORDER BY a.minute ASC) AS usdc_getMinPrice,
            
            last(f1.amount, true) OVER (ORDER BY a.minute ASC) AS uni_poolAmounts,
            last(f2.amount, true) OVER (ORDER BY a.minute ASC) AS uni_reservedAmounts,
            last(f3.amount, true) OVER (ORDER BY a.minute ASC) AS uni_guaranteedUsd,
            last(f4.price, true) OVER (ORDER BY a.minute ASC) AS uni_getMaxPrice,
            last(f5.price, true) OVER (ORDER BY a.minute ASC) AS uni_getMinPrice,
            last(f6.price, true) OVER (ORDER BY a.minute ASC) AS uni_globalShortAveragePrices,
            last(f7.amount, true) OVER (ORDER BY a.minute ASC) AS uni_globalShortSizes,
            
            last(g1.amount, true) OVER (ORDER BY a.minute ASC) AS link_poolAmounts,
            last(g2.amount, true) OVER (ORDER BY a.minute ASC) AS link_reservedAmounts,
            last(g3.amount, true) OVER (ORDER BY a.minute ASC) AS link_guaranteedUsd,
            last(g4.price, true) OVER (ORDER BY a.minute ASC) AS link_getMaxPrice,
            last(g5.price, true) OVER (ORDER BY a.minute ASC) AS link_getMinPrice,
            last(g6.price, true) OVER (ORDER BY a.minute ASC) AS link_globalShortAveragePrices,
            last(g7.amount, true) OVER (ORDER BY a.minute ASC) AS link_globalShortSizes,
            
            last(h1.amount, true) OVER (ORDER BY a.minute ASC) AS weth_poolAmounts,
            last(h2.amount, true) OVER (ORDER BY a.minute ASC) AS weth_reservedAmounts,
            last(h3.amount, true) OVER (ORDER BY a.minute ASC) AS weth_guaranteedUsd,
            last(h4.price, true) OVER (ORDER BY a.minute ASC) AS weth_getMaxPrice,
            last(h5.price, true) OVER (ORDER BY a.minute ASC) AS weth_getMinPrice,
            last(h6.price, true) OVER (ORDER BY a.minute ASC) AS weth_globalShortAveragePrices,
            last(h7.amount, true) OVER (ORDER BY a.minute ASC) AS weth_globalShortSizes,
            
            last(i1.amount, true) OVER (ORDER BY a.minute ASC) AS dai_poolAmounts,
            last(i2.price, true) OVER (ORDER BY a.minute ASC) AS dai_getMaxPrice,
            last(i3.price, true) OVER (ORDER BY a.minute ASC) AS dai_getMinPrice
            
        FROM minute a
        
        LEFT JOIN glp_frax_poolAmounts b1
            ON a.minute = b1.minute
        LEFT JOIN glp_frax_getMaxPrice b2
            ON a.minute = b2.minute
        LEFT JOIN glp_frax_getMinPrice b3
            ON a.minute = b3.minute
        
        LEFT JOIN glp_usdt_poolAmounts c1
            ON a.minute = c1.minute
        LEFT JOIN glp_usdt_getMaxPrice c2
            ON a.minute = c2.minute
        LEFT JOIN glp_usdt_getMinPrice c3
            ON a.minute = c3.minute
        
        LEFT JOIN glp_wbtc_poolAmounts d1
            ON a.minute = d1.minute
        LEFT JOIN glp_wbtc_reservedAmounts d2
            ON a.minute = d2.minute
        LEFT JOIN glp_wbtc_guaranteedUsd d3
            ON a.minute = d3.minute
        LEFT JOIN glp_wbtc_getMaxPrice d4
            ON a.minute = d4.minute
        LEFT JOIN glp_wbtc_getMinPrice d5
            ON a.minute = d5.minute
        LEFT JOIN glp_wbtc_globalShortAveragePrices d6
            ON a.minute = d6.minute
        LEFT JOIN glp_wbtc_globalShortSizes d7
            ON a.minute = d7.minute
        
        LEFT JOIN glp_usdc_poolAmounts e1
            ON a.minute = e1.minute
        LEFT JOIN glp_usdc_getMaxPrice e2
            ON a.minute = e2.minute
        LEFT JOIN glp_usdc_getMinPrice e3
            ON a.minute = e3.minute
        
        LEFT JOIN glp_uni_poolAmounts f1
            ON a.minute = f1.minute
        LEFT JOIN glp_uni_reservedAmounts f2
            ON a.minute = f2.minute
        LEFT JOIN glp_uni_guaranteedUsd f3
            ON a.minute = f3.minute
        LEFT JOIN glp_uni_getMaxPrice f4
            ON a.minute = f4.minute
        LEFT JOIN glp_uni_getMinPrice f5
            ON a.minute = f5.minute
        LEFT JOIN glp_uni_globalShortAveragePrices f6
            ON a.minute = f6.minute
        LEFT JOIN glp_uni_globalShortSizes f7
            ON a.minute = f7.minute
        
        LEFT JOIN glp_link_poolAmounts g1
            ON a.minute = g1.minute
        LEFT JOIN glp_link_reservedAmounts g2
            ON a.minute = g2.minute
        LEFT JOIN glp_link_guaranteedUsd g3
            ON a.minute = g3.minute
        LEFT JOIN glp_link_getMaxPrice g4
            ON a.minute = g4.minute
        LEFT JOIN glp_link_getMinPrice g5
            ON a.minute = g5.minute
        LEFT JOIN glp_link_globalShortAveragePrices g6
            ON a.minute = g6.minute
        LEFT JOIN glp_link_globalShortSizes g7
            ON a.minute = g7.minute
        
        LEFT JOIN glp_weth_poolAmounts h1
            ON a.minute = h1.minute
        LEFT JOIN glp_weth_reservedAmounts h2
            ON a.minute = h2.minute
        LEFT JOIN glp_weth_guaranteedUsd h3
            ON a.minute = h3.minute
        LEFT JOIN glp_weth_getMaxPrice h4
            ON a.minute = h4.minute
        LEFT JOIN glp_weth_getMinPrice h5
            ON a.minute = h5.minute
        LEFT JOIN glp_weth_globalShortAveragePrices h6
            ON a.minute = h6.minute
        LEFT JOIN glp_weth_globalShortSizes h7
            ON a.minute = h7.minute
        
        LEFT JOIN glp_dai_poolAmounts i1
            ON a.minute = i1.minute
        LEFT JOIN glp_dai_getMaxPrice i2
            ON a.minute = i2.minute
        LEFT JOIN glp_dai_getMinPrice i3
            ON a.minute = i3.minute
        ) x
    ) y