{{ config(
    schema = 'balancer_v2_optimism',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook = '{{ expose_spells(\'["optimism"]\',
                                "project",
                                "balancer_v2",
                                \'["mendesfabio", "jacektrocinski", "thetroyharris"]\') }}'
    )
}}

{% set project_start_date = '2022-05-23' %}

WITH    
    fees_base AS (
        SELECT *, 
            MAX(index) OVER(PARTITION BY tx_hash, contract_address) AS max_index_same_tx 
        FROM {{ ref('balancer_v2_optimism_pools_fees') }} 
    ),
    most_case_fees AS (
        SELECT * FROM fees_base 
        WHERE index = max_index_same_tx
    ),
    edge_case_fees AS (
        SELECT 
            s.evt_block_number,
            s.evt_tx_hash,
            s.evt_index,
            f.swap_fee_percentage 
        FROM {{ source ('balancer_v2_optimism', 'Vault_evt_Swap') }} s
            INNER JOIN fees_base f 
                ON s.evt_tx_hash = f.tx_hash AND f.index < s.evt_index
    ),
    swap_fees AS (
        SELECT
            swap.evt_block_number,
            swap.evt_tx_hash,
            swap.evt_index,
            SUBSTRING(swap.poolId, 0, 42) AS contract_address,
            swap.evt_block_time,
            MAX(fees.block_time) AS max_fee_evt_block_time
        FROM
            {{ source ('balancer_v2_optimism', 'Vault_evt_Swap') }} swap
            LEFT JOIN {{ ref('balancer_v2_optimism_pools_fees') }} fees
                ON fees.contract_address = SUBSTRING(swap.poolId, 0, 42)
                AND fees.block_time <= swap.evt_block_time
        {% if is_incremental() %}
        WHERE swap.evt_block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    ),
    dexs AS (
        SELECT
            swap.evt_block_number,
            swap.evt_block_time AS block_time,
            '' AS taker,
            '' AS maker,
            swap.amountOut AS token_bought_amount_raw,
            swap.amountIn AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            swap.tokenOut AS token_bought_address,
            swap.tokenIn AS token_sold_address,
            swap_fees.contract_address AS project_contract_address,
            swap.poolId AS poolId,
            COALESCE(most.swap_fee_percentage, edge.swap_fee_percentage) / POWER(10, 18) AS swap_fee,
            swap.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            swap.evt_index
        FROM
            swap_fees
            INNER JOIN {{ source ('balancer_v2_optimism', 'Vault_evt_Swap') }} swap
                ON swap.evt_block_number = swap_fees.evt_block_number
                AND swap.evt_tx_hash = swap_fees.evt_tx_hash
                AND swap.evt_index = swap_fees.evt_index
            LEFT JOIN most_case_fees most
                ON most.contract_address = swap_fees.contract_address
                AND most.block_time = swap_fees.max_fee_evt_block_time
                AND most.tx_hash <> swap_fees.evt_tx_hash
            LEFT JOIN edge_case_fees edge
                ON edge.evt_block_number = swap_fees.evt_block_number
                AND edge.evt_tx_hash = swap_fees.evt_tx_hash
                AND edge.evt_index = swap_fees.evt_index
        WHERE
            swap.tokenIn <> swap_fees.contract_address
            AND swap.tokenOut <> swap_fees.contract_address
    ),
    bpa AS (
        SELECT
            dexs.evt_block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.hour) AS bpa_max_block_time
        FROM
            dexs
            LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_bought_address
                AND bpt_prices.hour <= dexs.block_time
                {% if not is_incremental() %}
                AND bpt_prices.hour >= '{{ project_start_date }}'
                {% endif %}
                {% if is_incremental() %}
                AND bpt_prices.hour >= DATE_TRUNC("day", NOW() - interval '1 week')
                {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    ),
    bpb AS (
        SELECT
            dexs.evt_block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.hour) AS bpb_max_block_time
        FROM
            dexs
            LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_sold_address
                AND bpt_prices.hour <= dexs.block_time
                {% if not is_incremental() %}
                AND bpt_prices.hour >= '{{ project_start_date }}'
                {% endif %}
                {% if is_incremental() %}
                AND bpt_prices.hour >= DATE_TRUNC("day", NOW() - interval '1 week')
                {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    )

SELECT
    'optimism' AS blockchain,
    'balancer' AS project,
    '2' AS version,
    DATE_TRUNC('DAY', dexs.block_time) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN
        CONCAT(erc20b.symbol, '-', erc20a.symbol)
    ELSE
        CONCAT(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) AS token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, 18)) AS token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
    -- dexs.token_bought_amount_raw,
    -- dexs.token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd,
        dexs.token_bought_amount_raw / POWER(10, p_bought.decimals) * p_bought.price,
        dexs.token_sold_amount_raw / POWER(10, p_sold.decimals) * p_sold.price,
        dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) * bpa_bpt_prices.median_price,
        dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, 18))  * bpb_bpt_prices.median_price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    COALESCE(dexs.taker, tx.`from`) AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.poolId,
    dexs.swap_fee,
    dexs.tx_hash,
    tx.`from` AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM
    dexs
    INNER JOIN {{ source ('optimism', 'transactions') }} tx
        ON tx.hash = dexs.tx_hash
        {% if not is_incremental() %}
        AND tx.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND tx.block_time >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref ('tokens_erc20_legacy') }} erc20a
        ON erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'optimism'
    LEFT JOIN {{ ref ('tokens_erc20_legacy') }} erc20b
        ON erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'optimism'
    LEFT JOIN {{ source ('prices', 'usd') }} p_bought
        ON p_bought.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'optimism'
        {% if not is_incremental() %}
        AND p_bought.minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND p_bought.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source ('prices', 'usd') }} p_sold
        ON p_sold.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'optimism'
        {% if not is_incremental() %}
        AND p_sold.minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND p_sold.minute >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    INNER JOIN bpa
        ON bpa.evt_block_number = dexs.evt_block_number
        AND bpa.tx_hash = dexs.tx_hash
        AND bpa.evt_index = dexs.evt_index
    LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpa_bpt_prices
        ON bpa_bpt_prices.contract_address = bpa.contract_address
        AND bpa_bpt_prices.hour = bpa.bpa_max_block_time
        {% if not is_incremental() %}
        AND bpa_bpt_prices.hour >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND bpa_bpt_prices.hour >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
    INNER JOIN bpb
        ON bpb.evt_block_number = dexs.evt_block_number
        AND bpb.tx_hash = dexs.tx_hash
        AND bpb.evt_index = dexs.evt_index
    LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpb_bpt_prices
        ON bpb_bpt_prices.contract_address = bpb.contract_address
        AND bpb_bpt_prices.hour = bpb.bpb_max_block_time
        {% if not is_incremental() %}
        AND bpa_bpt_prices.hour >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND bpa_bpt_prices.hour >= DATE_TRUNC("day", NOW() - interval '1 week')
        {% endif %}
