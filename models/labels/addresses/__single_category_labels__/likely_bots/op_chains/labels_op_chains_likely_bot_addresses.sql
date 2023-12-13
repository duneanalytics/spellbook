{{
    config(
        alias = 'op_chains_likely_bot_addresses',
        materialized ='table',
        partition_by = ['blockchain'],
        post_hook='{{ expose_spells(\'["optimism","base","zora"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

{% set op_chains = all_op_chains() %} --macro: all_op_chains.sql

-- This could/should become a spell with some kind of modular logic approach so that we can plug in new detection logic over time (i.e. many of X method, or Y project's contracts)
-- the core of this logic is on transaction frequency and sender concentration The "sender concentration" piece will get tested by mutlisigs / smart contract wallets.

WITH sender_transfer_rates AS (

    {% for chain in op_chains %}
    -- For each transaction sender, get their hourly transaction data
    SELECT  '{{chain}}' as blockchain
            ,"from" AS sender, DATE_TRUNC('hour',block_time) AS hr
            , MIN(block_time) AS min_block_time
            , MAX(block_time) AS max_block_time
            , COUNT(*) AS hr_txs
            , SUM(CASE WHEN to IN (SELECT address FROM {{ ref('labels_op_chains_likely_bot_contracts') }} WHERE name != 'chain ops bot' AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS bot_concentration_txs
            
            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ source('erc20_' + chain,'evt_transfer') }} r WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number) THEN 1 ELSE 0 END) AS num_erc20_tfer_txs
            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('nft_' + chain + '_transfers') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number) THEN 1 ELSE 0 END) AS num_nft_tfer_txs

            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ source('erc20_' + chain,'evt_transfer') }} r WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number) THEN 1 
                    WHEN EXISTS (SELECT 1 FROM {{ ref('nft_' + chain + '_transfers') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND blockchain = '{{chain}}') THEN 1
                ELSE 0 END) AS num_token_tfer_txs
    
            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('dex_trades') }} r WHERE t.hash = r.tx_hash AND t.block_time = r.block_time AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_dex_trade_txs
            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('perpetual_trades') }} r WHERE t.hash = r.tx_hash AND t.block_time = r.block_time AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_perp_trade_txs
            , SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('nft_trades') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_nft_trade_txs
            
            FROM {{ source( chain ,'transactions') }} t

        GROUP BY 1,2,3

        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

)

, first_pass_throughput_filter AS
(
    -- Filter down this list a bit to help with later mappings
    SELECT 
        blockchain
        ,sender, DATE_TRUNC('week',hr) AS wk, SUM(hr_txs) AS wk_txs, MAX(hr_txs) AS max_hr_txs, SUM(bot_concentration_txs) AS bot_concentration_txs,
        cast(COUNT(*) as double) /cast(7.0*24.0 as double) AS pct_weekly_hours_active,
        MIN(min_block_time) AS min_block_time,
        MAX(max_block_time) AS max_block_time,
        
        SUM(num_erc20_tfer_txs) AS num_erc20_tfer_txs,
        SUM(num_nft_tfer_txs) AS num_nft_tfer_txs,
        SUM(num_token_tfer_txs) AS num_token_tfer_txs,
        SUM(num_dex_trade_txs) AS num_dex_trade_txs,
        SUM(num_perp_trade_txs) AS num_perp_trade_txs,
        SUM(num_nft_trade_txs) AS num_nft_trade_txs
        FROM sender_transfer_rates e
        GROUP BY 1,2,3
        HAVING MAX(hr_txs) >= 20 --had some high-ish frequency - gte 20 txs per hour at least once
        OR
        SUM(bot_concentration_txs) >= 0 --had a tx to a bot address

)


,  bot_addresses AS (
SELECT *,
    cast(num_erc20_tfer_txs as double) / cast( num_txs as double) AS pct_erc20_tfer_txs,
    cast(num_nft_tfer_txs as double) / cast( num_txs as double) AS pct_nft_tfer_txs,
    cast(num_token_tfer_txs as double) / cast( num_txs as double) AS pct_token_tfer_txs,
    cast(num_dex_trade_txs as double) / cast( num_txs as double) AS pct_dex_trade_txs,
    cast(num_perp_trade_txs as double) / cast( num_txs as double) AS pct_perp_trade_txs, -- perpetual.trades has some dunesql incompatability
    cast(num_nft_trade_txs as double) / cast( num_txs as double) AS pct_nft_trade_txs
    
FROM (
        SELECT
             blockchain
            , sender, MAX(wk_txs) AS max_wk_txs, MAX(max_hr_txs) AS max_hr_txs, AVG(wk_txs) AS avg_wk_txs
            ,MIN(min_block_time) AS min_block_time
            ,MAX(max_block_time) AS max_block_time
            ,MAX(pct_weekly_hours_active) AS max_pct_weekly_hours_active
            ,AVG(pct_weekly_hours_active) AS avg_pct_weekly_hours_active
            ,SUM(wk_txs) AS num_txs
            ,SUM(bot_concentration_txs) AS bot_concentration_txs
            
            ,cast(SUM(bot_concentration_txs) as double) / cast(SUM(wk_txs) as double) AS pct_bot_concentration_txs
            ,( cast( date_DIFF('second', MIN(min_block_time), MAX(max_block_time)) as double) / (60.0*60.0) ) AS txs_per_hour

            ,SUM(num_erc20_tfer_txs) AS num_erc20_tfer_txs
            ,SUM(num_nft_tfer_txs) AS num_nft_tfer_txs
            ,SUM(num_token_tfer_txs) AS num_token_tfer_txs
            ,SUM(num_dex_trade_txs) AS num_dex_trade_txs
            ,SUM(num_perp_trade_txs) AS num_perp_trade_txs
            ,SUM(num_nft_trade_txs) AS num_nft_trade_txs
            
        FROM first_pass_throughput_filter f
            GROUP BY 1,2
            -- various cases to detect bots
            HAVING (MAX(wk_txs) >= 2000 AND MAX(max_hr_txs) >= 100) --frequency (gt 2k txs in one week and gt 100 txs in one hour)
                OR (MAX(wk_txs) >= 4000 AND MAX(max_hr_txs) >= 50) --frequency (gt 4k txs in one week and gt 50 txs in one hour)
                OR AVG(wk_txs) >= 1000 --frequency (avg 1k txs per week)
                OR 
                    (
                    cast(COUNT(*) as double) / 
                        ( cast( date_DIFF('second', MIN(min_block_time), MAX(max_block_time)) as double) / (60.0*60.0) ) >= 25 
                    AND SUM(wk_txs) >= 100
                    ) --frequency gt 25 txs per hour
                OR AVG(pct_weekly_hours_active) > 0.5 -- aliveness: transacting at least 50% of hours per week
                OR MAX(pct_weekly_hours_active) > 0.95 -- aliveness: at peack, transacted at least 95% of hours in a week
                OR (cast(SUM(bot_concentration_txs) as double) / cast(SUM(wk_txs) as double) > 0.5) --at least half txs go to bots
        ) ff

)

select
  blockchain,
  address,
  name,
  category,
  'msilb7' AS contributor,
  'query' AS source,
  timestamp '2023-03-11' as created_at,
  now() as updated_at,
  'likely_bot_addresses' as model_name,
  'persona' as label_type

  FROM (
    -- show ther entire list as one label
    SELECT
    blockchain,
    sender AS address,
    'likely bots' AS category,
    'likely bot addresses' AS name

    from bot_addresses

  UNION ALL
    -- create another set of labels, cohorted by "type"
    SELECT
    blockchain,
    sender AS address,
    'likely bot types' AS category,
    CASE
      WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trade bot address' 
      WHEN pct_nft_trade_txs >= 0.5 THEN 'nft trade bot address' 
      WHEN pct_perp_trade_txs >= 0.5 THEN 'perp trade bot address' 
      WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot address' 
      WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot address' 
      WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot address' 
    ELSE 'non-token bot address'
    END AS name

  from bot_addresses

    ) a