{{ config(
    alias = 'inorganic_volume_filter_blur',
    materialized = 'view'
)
}}

{% set project_start_date = '2022-10-19' %} -- blur start date 

WITH 

trades as (
        SELECT 
            t.* 
        FROM 
        {{ ref('nft_trades') }} t 
        WHERE 1 = 1
        AND t.project IN ('opensea', 'blur')
        AND t.blockchain = 'ethereum'
), 

royal_settings as (
        SELECT 
            collection, 
            fee
        FROM 
        (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY collection ORDER BY evt_block_time DESC) as ordering 
        FROM 
        (
        SELECT 
            * 
        FROM 
        {{ source('looksrare_ethereum', 'RoyaltyFeeRegistry_evt_RoyaltyFeeUpdate') }}
        ) x1 
        ) x2 
        WHERE ordering = 1 
),

mt_filter as (
        SELECT 
            *,
            'mt' as filter
        FROM
        (
        SELECT 
            date_trunc('day', block_time) as day,
            nft_contract_address,
            token_id as nft_token_id, 
            COUNT(1) as num_sales 
        FROM 
        trades
        WHERE project = 'blur'
        AND token_standard = 'erc721'
        GROUP BY 1, 2, 3
        ) trade_count 
        WHERE true 
        AND num_sales >= 3 
), 

sb_filter as (
        SELECT 
            *, 
            'sb' as filter 
        FROM 
        (
        SELECT 
            date_trunc('day', t.block_time) as day, 
            CASE WHEN t.seller > t.buyer THEN t.seller ELSE t.buyer END as address1, 
            CASE WHEN t.seller > t.buyer THEN t.buyer ELSE t.seller END as address2, 
            COUNT(DISTINCT(tx_hash)) as num_sales 
        FROM 
        trades t 
        LEFT JOIN 
        {{ ref('nft_ethereum_aggregators') }} agg 
            ON agg.contract_address = t.buyer 
        WHERE t.project = 'blur'
        AND agg.contract_address IS NULL 
        GROUP BY 1, 2, 3 
        ) foo 
), 

lv_filter as (
        SELECT 
            *, 
            'lv' as filter 
        FROM 
        (
        SELECT 
            day, 
            nft_address,
            os_vol, 
            SUM(os_vol) OVER (PARTITION BY nft_address ORDER BY day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 30d_vol 
        FROM
        (
        SELECT 
            date_trunc('day', t.block_time) as day,
            x2.nft_address,
            SUM(t.amount_usd) as os_vol 
        FROM 
        (
        SELECT 
            DISTINCT(nft_contract_address) as nft_address
        FROM 
        trades 
        WHERE project = 'blur'
        ) x2 
        LEFT JOIN trades t 
            ON x2.nft_address = t.nft_contract_address
        WHERE t.project = 'opensea'
        GROUP BY 1, 2 
        ) foo 
        ) foo2 
        WHERE true 
        AND 30d_vol < 100 
),

hp_filter as (
        SELECT 
            *, 
            'hp' as filter 
        FROM 
        (
        SELECT 
            day, 
            nft_address,
            high_price,
            10 * highprice_cutoff as highprice_cutoff
        FROM
        (
        SELECT 
            day, 
            nft_address, 
            highprice_cutoff as high_price, 
            MAX(highprice_cutoff) OVER (PARTITION BY nft_address ORDER BY day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as highprice_cutoff
        FROM
        (
        SELECT 
            date_trunc('day', t.block_time) as day, 
            x2.nft_address,
            MAX(t.amount_usd) as highprice_cutoff 
        FROM 
        (
        SELECT 
            DISTINCT(t.nft_contract_address) as nft_address
        FROM 
        trades t 
        INNER JOIN 
        royal_settings r 
            ON t.nft_contract_address = r.collection 
            AND r.fee = 0 
        WHERE t.project = 'blur'
        ) x2 
        LEFT JOIN trades t 
            ON x2.nft_address = t.nft_contract_address
        WHERE t.project = 'opensea'
        GROUP BY 1, 2 
        ) foo 
        ) foo2 
        ) foo3 
),

wf_filter as (
        SELECT 
            DISTINCT 
            t.buyer, 
            t.seller, 
            'wf' as filter 
        FROM 
        trades t 
        LEFT JOIN 
        {{ ref('opensea_inorganic_volume_filter_wallet_funders') }} f1 
            ON f1.wallet = t.buyer 
        LEFT JOIN 
        {{ ref('opensea_inorganic_volume_filter_wallet_funders') }} f2
            ON f2.wallet = t.seller 
        WHERE t.project = 'blur'
        AND f1.funder = f2.funder 
        OR (f1.funder = t.seller OR f2.funder = t.buyer)
),

circular_buyer as (
        SELECT 
            *, 
            'circular_buyer' as filter 
        FROM 
        (
        SELECT 
            COUNT(*) as cnt, 
            token_id as nft_token_id,
            nft_contract_address,
            buyer 
        FROM 
        trades t 
        WHERE t.project = 'blur'
        AND token_standard = 'erc721'
        AND buyer <> LOWER('0x39da41747a83aee658334415666f3ef92dd0d541')
        GROUP BY 2, 3, 4 
        ) foo 
        WHERE cnt >= 2 
),

circular_seller as (
        SELECT 
            *, 
            'circular_seller' as filter 
        FROM 
        (
        SELECT 
            COUNT(*) as cnt, 
            token_id as nft_token_id,
            nft_contract_address,
            seller 
        FROM 
        trades t 
        WHERE t.project = 'blur'
        AND token_standard = 'erc721'
        AND buyer <> LOWER('0x39da41747a83aee658334415666f3ef92dd0d541')
        GROUP BY 2, 3, 4 
        ) foo 
        WHERE cnt >= 2 
),

trades_enrich as (
        SELECT 
            date_trunc('day', t.block_time) as day, 
            t.block_time, 
            t.project, 
            t.nft_contract_address,
            t.token_id as nft_token_id,
            t.tx_hash, 
            erc20.symbol as currency,
            t.amount_raw / POW(10, erc20.decimals) as amount, 
            p.price as usd_price, 
            t.amount_raw / POW(10, erc20.decimals) * p.price as usd_amount, 
            t.buyer,
            t.seller,
            t.unique_trade_id 
        FROM 
        trades t 
        LEFT JOIN {{ source('prices', 'usd') }} p 
            ON p.minute = date_trunc('minute', t.block_time)
            AND p.contract_address = t.currency_contract 
            AND p.blockchain = 'ethereum'
            AND p.minute >= '{{project_start_date}}'
        LEFT JOIN 
        {{ ref('tokens_erc20') }} erc20 
            ON t.currency_contract = erc20.contract_address
            AND erc20.blockchain = 'ethereum'
        WHERE t.project = 'blur'
),

filtered_trades as (
        SELECT 
            t.*, 
            CASE WHEN mt.filter IS NOT NULL THEN true ELSE false END as mt_filter,
            CASE WHEN sb.filter IS NOT NULL THEN true ELSE false END as sb_filter,
            CASE WHEN lv.filter IS NOT NULL THEN true ELSE false END as lv_filter,
            CASE WHEN hp.filter IS NOT NULL THEN true ELSE false END as hp_filter,
            CASE WHEN wf.filter IS NOT NULL THEN true ELSE false END as wf_filter,
            CASE WHEN cb.filter IS NOT NULL THEN true ELSE false END as cb_filter,
            CASE WHEN cs.filter IS NOT NULL THEN true ELSE false END as cs_filter,
            FILTER(array(mt.filter, sb.filter, lv.filter, hp.filter, wf.filter, cb.filter, cs.filter), x -> x IS NOT NULL) as inorganic_filters
        FROM 
        trades_enrich t 
        LEFT JOIN 
        mt_filter mt 
            ON mt.day = t.day 
            AND mt.nft_contract_address = t.nft_contract_address
            AND mt.nft_token_id = t.nft_token_id
        LEFT JOIN 
        sb_filter sb 
            ON sb.day = t.day 
            AND ((t.buyer = sb.address1 and t.seller = sb.address2)
            OR (t.seller = sb.address1 and t.buyer = sb.address2))
        LEFT JOIN 
        lv_filter lv 
            ON lv.nft_address = t.nft_contract_address
            AND lv.day = t.day 
        LEFT JOIN 
        hp_filter hp 
            ON t.nft_contract_address = hp.nft_address
            AND t.day = hp.day 
            AND t.usd_amount > hp.highprice_cutoff
        LEFT JOIN 
        wf_filter wf 
            ON wf.buyer = t.buyer 
            AND wf.seller = t.seller 
        LEFT JOIN 
        circular_buyer cb 
            ON t.nft_contract_address = cb.nft_contract_address
            AND t.buyer = cb.buyer
            AND t.nft_token_id = cb.nft_token_id
        LEFT JOIN
        circular_seller cs 
            ON t.nft_contract_address = cs.nft_contract_address
            AND t.seller = cs.seller
            AND t.nft_token_id = cs.nft_token_id
)

SELECT 
*, 
CASE WHEN cardinality(inorganic_filters) > 0 AND inorganic_filters IS NOT NULL THEN true ELSE false END as any_filter 
FROM 
filtered_trades