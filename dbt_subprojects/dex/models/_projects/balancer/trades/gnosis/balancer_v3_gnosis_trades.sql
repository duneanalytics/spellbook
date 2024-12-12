{{
    config(
        schema = 'balancer_v3_gnosis',
        alias = 'trades',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["gnosis"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

WITH
    dexs_base AS (
        SELECT
            tx_hash,
            evt_index,
            pool_id,
            swap_fee,
            pool_symbol,
            pool_type
        FROM {{ ref('balancer_v3_gnosis_base_trades') }}
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
            dexs_base.swap_fee,
            dexs_base.pool_symbol,
            dexs_base.pool_type
        FROM {{ ref('dex_trades') }} dexs
            INNER JOIN dexs_base
                ON dexs.tx_hash = dexs_base.tx_hash
                AND dexs.evt_index = dexs_base.evt_index
        WHERE dexs.blockchain = 'gnosis'
            AND dexs.project = 'balancer'
            AND dexs.version = '3'
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
            LEFT JOIN {{ source('balancer', 'bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_bought_address
                AND bpt_prices.day <= DATE_TRUNC('day', dexs.block_time)
                AND bpt_prices.blockchain = 'gnosis'                
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
            LEFT JOIN {{ source('balancer', 'bpt_prices') }} bpt_prices
                ON bpt_prices.contract_address = dexs.token_sold_address
                AND bpt_prices.day <= DATE_TRUNC('day', dexs.block_time)
                AND bpt_prices.blockchain = 'gnosis'
        GROUP BY 1, 2, 3, 4, 5
    ),

    erc4626_prices AS(
        SELECT
            minute,
            wrapped_token,
            decimals,
            APPROX_PERCENTILE(median_price, 0.5) AS price,
            LEAD(minute, 1, NOW()) OVER (PARTITION BY wrapped_token ORDER BY minute) AS time_of_next_change
        FROM {{ source('balancer_v3', 'erc4626_token_prices') }}
        WHERE blockchain = 'gnosis'
        GROUP BY 1, 2, 3
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
        dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, erc4626a.decimals, 18)) * COALESCE(bpa_bpt_prices.bpt_price, erc4626a.price),
        dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, erc4626b.decimals, 18))  * COALESCE(bpb_bpt_prices.bpt_price, erc4626b.price)
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.pool_id,
    dexs.swap_fee,
    dexs.pool_symbol,
    dexs.pool_type,
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
    LEFT JOIN {{ source('balancer', 'bpt_prices') }} bpa_bpt_prices
        ON bpa_bpt_prices.contract_address = bpa.contract_address
        AND bpa_bpt_prices.day = bpa.bpa_max_block_date
        AND bpa_bpt_prices.blockchain = 'gnosis'        
    INNER JOIN bpb
        ON bpb.block_number = dexs.block_number
        AND bpb.tx_hash = dexs.tx_hash
        AND bpb.evt_index = dexs.evt_index   
    LEFT JOIN {{ source('balancer', 'bpt_prices') }} bpb_bpt_prices
        ON bpb_bpt_prices.contract_address = bpb.contract_address
        AND bpb_bpt_prices.day = bpb.bpb_max_block_date
        AND bpb_bpt_prices.blockchain = 'gnosis'
    LEFT JOIN erc4626_prices erc4626a
        ON erc4626a.wrapped_token = dexs.token_bought_address
        AND erc4626a.minute <= dexs.block_time
        AND c.day < erc4626a.time_of_next_change
    LEFT JOIN erc4626_prices erc4626b
        ON erc4626b.wrapped_token = dexs.token_sold_address
        AND erc4626b.minute <= dexs.block_time
        AND c.day < erc4626b.time_of_next_change   