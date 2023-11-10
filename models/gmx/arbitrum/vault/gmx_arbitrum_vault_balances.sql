{{ config(
tags=['prod_exclude'],
        alias = 'vault_balances',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'minute'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "gmx",
                                    \'["1chioku"]\') }}'
        )
}}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
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
GMX vault address is: https://arbiscan.io/address/0x489ee077994B6658eAfA855C308275EAd8097C4A
*/

vault_balances_frax AS -- This CTE returns the balance of FRAX tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of FRAX tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of FRAX tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of FRAX tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e18) AS transfer_value -- FRAX 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
                AND `contract_address` = '0x17fc002b466eec40dae837fc4be5c67993ddbd6f' -- FRAX Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND evt_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
    
            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e18 AS transfer_value -- FRAX 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
                AND `contract_address` = '0x17fc002b466eec40dae837fc4be5c67993ddbd6f' -- FRAX Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND evt_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_usdt AS -- This CTE returns the balance of USDT tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of USDT tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of USDT tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of USDT tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e6) AS transfer_value -- USDT 6dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
                AND `contract_address` = '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9' -- USDT Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND evt_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e6 AS transfer_value -- USDT 6dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
                AND `contract_address` = '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9' -- USDT Arbitrum Smart Contract
                {% if not is_incremental() %}
                AND evt_block_time >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_wbtc AS -- This CTE returns the balance of WBTC tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of WBTC tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of WBTC tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of WBTC tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e8) AS transfer_value -- WBTC 8dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e8 AS transfer_value -- WBTC 8dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f' -- WBTC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_usdc AS -- This CTE returns the balance of USDC tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of USDC tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of USDC tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of USDC tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e6) AS transfer_value -- USDC 6dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e6 AS transfer_value -- USDC 6dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' -- USDC Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_uni AS -- This CTE returns the balance of UNI tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of UNI tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of UNI tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of UNI tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e18) AS transfer_value -- UNI 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e18 AS transfer_value -- UNI 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0' -- UNI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_link AS -- This CTE returns the balance of LINK tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of LINK tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of LINK tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of LINK tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e18) AS transfer_value -- LINK 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e18 AS transfer_value -- LINK 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a'-- GMX Arbitrum Vault Address
            AND `contract_address` = '0xf97f4df75117a78c1a5a0dbb814af92458539fb4' -- LINK Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_weth AS -- This CTE returns the balance of WETH tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of WETH tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of WETH tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of WETH tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e18) AS transfer_value -- WETH 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e18 AS transfer_value -- WETH 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    ) ,

vault_balances_dai AS -- This CTE returns the balance of DAI tokens in the GMX Arbitrum Vault in a designated minute
    (
    SELECT -- This query aggregates the cumulative balance of DAI tokens in the GMX Arbitrum Vault over the minute series
        b.minute,
        SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of DAI tokens in the GMX Arbitrum Vault in a designated minute
            a.minute,
            SUM(a.transfer_value) AS transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and aggregates the tranfers of DAI tokens to and from the GMX Arbitrum Vault
                date_trunc('minute', evt_block_time) AS minute,
                ((value)/1e18) AS transfer_value -- DAI 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x489ee077994b6658eafa855c308275ead8097c4a' -- GMX Arbitrum Vault Address
            AND `contract_address` = '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' -- DAI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL
                                    
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                ((-1 * value))/1e18 AS transfer_value -- DAI 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `from` = '0x489ee077994b6658eafa855c308275ead8097c4a' --- GMX Arbitrum Vault Address
            AND `contract_address` = '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' -- DAI Arbitrum Smart Contract
            {% if not is_incremental() %}
            AND evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            ) a
        GROUP BY a.minute
        ) b
    )

SELECT -- This CTE returns the balance of all supported tokens in the GMX Arbitrum Vault in a designated minute
    x.minute AS minute,
    TRY_CAST(date_trunc('DAY', x.minute) AS date) AS block_date,
    COALESCE(x.frax_balance,0) AS frax_balance, -- Removes null values
    COALESCE(x.usdt_balance,0) AS usdt_balance, -- Removes null values
    COALESCE(x.wbtc_balance,0) AS wbtc_balance, -- Removes null values
    COALESCE(x.usdc_balance,0) AS usdc_balance, -- Removes null values
    COALESCE(x.uni_balance,0) AS uni_balance, -- Removes null values
    COALESCE(x.link_balance,0) AS link_balance, -- Removes null values
    COALESCE(x.weth_balance,0) AS weth_balance, -- Removes null values
    COALESCE(x.dai_balance,0) AS dai_balance -- Removes null values
FROM
    (
    SELECT -- This subquery collates all the data extracted from the vault balance CTE, joins them to the minute series, and uses last data to extrapolate over null values
        a.minute,
        last(b.balance, true) OVER (ORDER BY a.minute ASC) AS frax_balance,
        last(c.balance, true) OVER (ORDER BY a.minute ASC) AS usdt_balance,
        last(d.balance, true) OVER (ORDER BY a.minute ASC) AS wbtc_balance,
        last(e.balance, true) OVER (ORDER BY a.minute ASC) AS usdc_balance,
        last(f.balance, true) OVER (ORDER BY a.minute ASC) AS uni_balance,
        last(g.balance, true) OVER (ORDER BY a.minute ASC) AS link_balance,
        last(h.balance, true) OVER (ORDER BY a.minute ASC) AS weth_balance,
        last(i.balance, true) OVER (ORDER BY a.minute ASC) AS dai_balance
    FROM minute a
    LEFT JOIN vault_balances_frax b
        ON a.minute = b.minute
    LEFT JOIN vault_balances_usdt c
        ON a.minute = c.minute
    LEFT JOIN vault_balances_wbtc d
        ON a.minute = d.minute
    LEFT JOIN vault_balances_usdc e
        ON a.minute = e.minute
    LEFT JOIN vault_balances_uni f
        ON a.minute = f.minute
    LEFT JOIN vault_balances_link g
        ON a.minute = g.minute
    LEFT JOIN vault_balances_weth h
        ON a.minute = h.minute
    LEFT JOIN vault_balances_dai i
        ON a.minute = i.minute
    ) x