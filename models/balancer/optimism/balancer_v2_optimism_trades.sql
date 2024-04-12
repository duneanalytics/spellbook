{{
    config(
        schema = 'balancer_v2_optimism',
        alias = 'trades',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["optimism"]\',
                                        "project",
                                        "balancer_v2",
                                        \'["mendesfabio", "jacektrocinski", "thetroyharris", "tomfutago", "viniabussafi"]\') }}'
    )
}}

WITH
    dexs_base AS (
        SELECT
            tx_hash,
            evt_index,
            pool_id,
            swap_fee
        FROM {{ ref('balancer_v2_optimism_base_trades') }}
    ),
    dexs AS (
        SELECT
            dexs.blockchain,
            dexs.project,
            dexs.version,
            dexs.block_month,
            dexs.block_date,
            dexs.block_time,
            dexs.block_number,
            dexs.token_bought_symbol,
            dexs.token_sold_symbol,
            dexs.token_pair,
            dexs.token_bought_amount,
            dexs.token_sold_amount,
            dexs.token_bought_amount_raw,
            dexs.token_sold_amount_raw,
            dexs.amount_usd,
            dexs.token_bought_address,
            dexs.token_sold_address,
            dexs.taker,
            dexs.maker,
            dexs.project_contract_address,
            dexs.tx_hash,
            dexs.tx_from,
            dexs.tx_to,
            dexs.evt_index,
            dexs_base.pool_id,
            dexs_base.swap_fee
        FROM {{ ref('dex_trades') }} dexs
            INNER JOIN dexs_base
                ON dexs.tx_hash = dexs_base.tx_hash
                AND dexs.evt_index = dexs_base.evt_index
        WHERE dexs.blockchain = 'optimism'
            AND dexs.project = 'balancer'
            AND dexs.version = '2'
    ),
    bpa AS (
        SELECT
            dexs.block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.day) AS bpa_max_block_date
        FROM dexs
            LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_bought_address
                AND bpt_prices.day <= DATE_TRUNC('day', dexs.block_time)
        GROUP BY 1, 2, 3, 4, 5
    ),
    bpb AS (
        SELECT
            dexs.block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.day) AS bpb_max_block_date
        FROM dexs
            LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_sold_address
                AND bpt_prices.day <= DATE_TRUNC('day', dexs.block_time)
        GROUP BY 1, 2, 3, 4, 5
    )

SELECT
    dexs.blockchain,
    dexs.project,
    dexs.version,
    dexs.block_date,
    dexs.block_number,
    dexs.block_month,
    dexs.block_time,
    dexs.token_bought_symbol,
    dexs.token_sold_symbol,
    dexs.token_pair,
    dexs.token_bought_amount,
    dexs.token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd,
        dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) * bpa_bpt_prices.bpt_price,
        dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, 18))  * bpb_bpt_prices.bpt_price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.pool_id,
    dexs.swap_fee,
    dexs.tx_hash,
    dexs.tx_from,
    dexs.tx_to,
    dexs.evt_index
FROM dexs
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
        ON erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = dexs.blockchain
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
        ON erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = dexs.blockchain
    INNER JOIN bpa
        ON bpa.block_number = dexs.block_number
        AND bpa.tx_hash = dexs.tx_hash
        AND bpa.evt_index = dexs.evt_index
    LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpa_bpt_prices
        ON bpa_bpt_prices.contract_address = bpa.contract_address
        AND bpa_bpt_prices.day = bpa.bpa_max_block_date
    INNER JOIN bpb
        ON bpb.block_number = dexs.block_number
        AND bpb.tx_hash = dexs.tx_hash
        AND bpb.evt_index = dexs.evt_index
    LEFT JOIN {{ ref('balancer_v2_optimism_bpt_prices') }} bpb_bpt_prices
        ON bpb_bpt_prices.contract_address = bpb.contract_address
        AND bpb_bpt_prices.day = bpb.bpb_max_block_date
