{{
    config(
        alias = 'op_chains_likely_bot_contracts',
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
WITH first_contracts AS (
SELECT *,
    cast(num_erc20_tfer_txs as double) / cast( num_txs as double) AS pct_erc20_tfer_txs,
    cast(num_nft_tfer_txs as double) / cast( num_txs as double) AS pct_nft_tfer_txs,
    cast(num_token_tfer_txs as double) / cast( num_txs as double) AS pct_token_tfer_txs,
    cast(num_dex_trade_txs as double) / cast( num_txs as double) AS pct_dex_trade_txs,
    cast(num_perp_trade_txs as double) / cast( num_txs as double) AS pct_perp_trade_txs,
    cast(num_nft_trade_txs as double) / cast( num_txs as double) AS pct_nft_trade_txs

FROM (
    {% for chain in op_chains %}
        SELECT 
            '{{chain}}' as blockchain,
            to AS contract, 
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ source('erc20_' + chain,'evt_transfer') }} r WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number) THEN 1 ELSE 0 END) AS num_erc20_tfer_txs,
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('nft_' + chain + '_transfers') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number) THEN 1 ELSE 0 END) AS num_nft_tfer_txs,
            
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ source('erc20_' + chain,'evt_transfer') }} r WHERE t.hash = r.evt_tx_hash AND t.block_number = r.evt_block_number) THEN 1 
                    WHEN EXISTS (SELECT 1 FROM {{ ref('nft_' + chain + '_transfers') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number ) THEN 1
                ELSE 0 END) AS num_token_tfer_txs,
                
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('dex_trades') }} r WHERE t.hash = r.tx_hash AND t.block_time = r.block_time AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_dex_trade_txs,
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('perpetual_trades') }} r WHERE t.hash = r.tx_hash AND t.block_time = r.block_time AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_perp_trade_txs,
            SUM(CASE WHEN EXISTS (SELECT 1 FROM {{ ref('nft_trades') }} r WHERE t.hash = r.tx_hash AND t.block_number = r.block_number AND r.block_month = DATE_TRUNC('month',r.block_time) AND blockchain = '{{chain}}') THEN 1 ELSE 0 END) AS num_nft_trade_txs,
        COUNT(*) AS num_txs, COUNT(DISTINCT "from") AS num_senders, COUNT(*)/COUNT(DISTINCT "from") AS txs_per_sender,
        
        cast(cast(COUNT(*) as double)/cast(COUNT(DISTINCT "from") as double) as double) / 
            ( cast( date_DIFF('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0) )  
            AS txs_per_addr_per_hour,
            
        cast(COUNT(*) as double) / 
            ( cast( date_DIFF('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0) ) 
            AS txs_per_hour

        -- SUM( CASE WHEN substring(data from 1 for 10) = mode(substring(data from 1 for 10) THEN 1 ELSE 0 END) ) AS method_dupe
        FROM {{ source(chain ,'transactions') }} t
        GROUP BY 1,2
        
        -- search for various potential bot indicators
        HAVING
        COUNT(*) >= 100 --prefilter, req 100 txs
        AND 
        (
            -- early bots: > 25 txs / hour per address
            (
            cast(cast(COUNT(*) as double)/cast(COUNT(DISTINCT "from") as double) as double) / 
                ( cast( date_DIFF('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0) ) >= 25 
            )
            OR
            -- established bots: less than 30 senders & > 2.5k txs & > 0.5 txs / hr (to make sure we don't accidently catch active multisigs)
                (COUNT(*) >= 2500 AND COUNT(DISTINCT "from") <=30
                AND cast(COUNT(*) as double) / 
                    ( cast( date_DIFF('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0) ) >= 0.5
                )
                OR 
            -- wider distribution bots: > 2.5k txs and > 1k txs per sender & > 0.5 txs / hr (to make sure we don't accidently catch active multisigs)
                (
                COUNT(*) >= 2500 AND cast(COUNT(*) as double)/cast(COUNT(DISTINCT "from") as double) >= 1000
                AND cast(COUNT(*) as double) / 
                ( cast( date_DIFF('second', MIN(block_time), MAX(block_time)) as double) / (60.0*60.0) ) >= 0.5
                )
        )

        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ) a
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
  'likely_bot_contracts' as model_name,
  'persona' as label_type

  FROM (

    SELECT
    blockchain,
    contract AS address,
    'likely bots' AS category,
    'likely bot contracts' AS name

    from first_contracts

  UNION ALL

    SELECT
    blockchain,
    contract AS address,
    'likely bot types' AS category,
    CASE
      WHEN pct_dex_trade_txs >= 0.5 THEN 'dex trade bot contract' 
      WHEN pct_nft_trade_txs >= 0.5 THEN 'nft trade bot contract' 
      WHEN pct_perp_trade_txs >= 0.5 THEN 'perp trade bot contract' 
      WHEN pct_erc20_tfer_txs >= 0.5 THEN 'erc20 transfer bot contract' 
      WHEN pct_nft_tfer_txs >= 0.5 THEN 'nft transfer bot contract' 
      WHEN pct_token_tfer_txs >= 0.5 THEN 'other token transfer bot contract' 
    ELSE 'non-token bot contract'
    END AS name

  from first_contracts

    ) a