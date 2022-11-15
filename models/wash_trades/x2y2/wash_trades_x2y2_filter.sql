{{ config(
    alias = 'x2y2_filter',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'block_time', 'nft_contract_address', 'nft_token_id', 'tx_hash', 'wash_filters'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "wash_trades",
                                \'["henrystats"]\') }}')
}}

{% set project_start_date = '2022-02-04' %} -- x2y2 start date 

WITH 

trades as (
        SELECT 
            t.* 
        FROM 
        {{ ref('nft_trades') }} t 
        WHERE 1 = 1
        {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        AND t.project IN ('opensea', 'x2y2')
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
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
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
        WHERE project = 'x2y2'
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
        WHERE t.project = 'x2y2'
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
            x2.nft_address,
            SUM(o.amount_usd) as os_vol 
        FROM 
        (
        SELECT 
            DISTINCT(nft_contract_address) as nft_address
        FROM 
        trades 
        WHERE project = 'x2y2'
        ) x2 
        LEFT JOIN trades t 
            ON x2.nft_address = t.nft_contract_address
        WHERE t.project = 'opensea'
        GROUP BY 1 
        ) foo 
        WHERE true 
        AND os_vol < 100 
),

hp_filter as (
        SELECT 
            *, 
            'hp' as filter 
        FROM 
        (
        SELECT 
            x2.nft_address,
            10 * MAX(os.amount_usd) as highprice_cutoff 
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
        WHERE t.project = 'x2y2'
        ) x2 
        LEFT JOIN trades t 
            ON x2.nft_address = t.nft_contract_address
        WHERE t.project = 'opensea'
        GROUP BY 1 
        ) foo 
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
        {{ ref('wash_trades_wallet_funders') }} f1 
            ON f1.wallet = t.buyer 
        LEFT JOIN 
        {{ ref('wash_trades_wallet_funders') }} f2
            ON f2.wallet = t.seller 
        WHERE t.project = 'x2y2'
        AND f1.funder = f2.funder 
        OR (f1.funder = t.seller OR f2.funder = t.buyer)
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
            t.seller 
        FROM 
        trades t 
        LEFT JOIN {{ source('prices', 'usd') }} p 
            ON p.minute = date_trunc('minute', t.block_time)
            AND p.contract_address = t.currency_contract 
            AND p.blockchain = 'ethereum'
            {% if not is_incremental() %}
            AND p.minute >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ ref('tokens_erc20') }} erc20 
            ON t.currency_contract = erc20.contract_address
            AND erc20.blockchain = 'ethereum'
        WHERE t.project = 'x2y2'
),

filtered_trades as (
        SELECT 
            t.*, 
            CASE WHEN mt.filter IS NOT NULL THEN true ELSE FALSE END as mt_filter,
            CASE WHEN sb.filter IS NOT NULL THEN true ELSE FALSE END as sb_filter,
            CASE WHEN lv.filter IS NOT NULL THEN true ELSE FALSE END as lv_filter,
            CASE WHEN hp.filter IS NOT NULL THEN true ELSE FALSE END as hp_filter,
            CASE WHEN wf.filter IS NOT NULL THEN true ELSE FALSE END as wf_filter,
            COALESCE(FILTER(array(mt.filter, sb.filter, lv.filter, hp.filter, wf.filter), x -> x IS NOT NULL), '{}') as wash_filters
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
        LEFT JOIN 
        hp_filter hp 
            ON t.nft_contract_address = hp.nft_address
            AND t.usd_amount > hp.highprice_cutoff
        LEFT JOIN 
        wf_filter wf 
            ON wf.buyer = t.buyer 
            AND wf.seller = t.seller 
)

SELECT 
*, 
CASE WHEN cardinality(wash_filters) > 0 THEN true ELSE false END as any_filter 
FROM 
filtered_trades