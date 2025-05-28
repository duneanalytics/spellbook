{{
  config(
    schema = 'stablecoins_solana',
    alias = 'p2p_usdc',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'from_owner', 'to_owner', 'block_time'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

--to get all the transctions that involved transfer of usdc 
WITH usdc_tx_ids AS (
  SELECT DISTINCT tx_id
  FROM {{ ref('tokens_solana_transfers') }}
  WHERE token_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
    AND outer_executing_account = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' --token program only
    AND action = 'transfer'
    AND from_owner != to_owner
    AND amount_usd >= 0.5
),

--to get the full picture of each transaction involving the transfer of usdc
all_tx_id AS (
  SELECT *
  FROM {{ ref('tokens_solana_transfers') }} t
  WHERE tx_id IN (SELECT tx_id FROM usdc_tx_ids)
),

--advance filtering - set rules , then remove transaction that doesnt meet the criteria (to get p2p transfers data)
pure_p2p_tx_ids AS (
  SELECT tx_id
  FROM all_tx_id
  GROUP BY tx_id
  HAVING 
    -- All transfers must be USDC
    MIN(CASE WHEN token_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' THEN 1 ELSE 0 END) = 1
    
    -- All actions must be transfers
    AND MIN(CASE WHEN action = 'transfer' THEN 1 ELSE 0 END) = 1
    
    -- All programs must be token program
    AND MIN(CASE WHEN outer_executing_account = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN 1 ELSE 0 END) = 1
    
    -- All transfers must be between different owners (p2p)
    AND MIN(CASE WHEN from_owner != to_owner THEN 1 ELSE 0 END) = 1
)

SELECT 
  'solana' as blockchain,
  t.tx_id as tx_hash,
  t.block_time,
  t.block_slot,
  t.from_owner,
  t.to_owner,
  t.amount_raw,
  t.amount,
  t.amount_usd,
  t.token_mint_address,
  t.tx_signer,
  CASE WHEN t.tx_signer = t.from_owner THEN true ELSE false END as is_sender_initiated
FROM all_tx_id t
JOIN pure_p2p_tx_ids p ON t.tx_id = p.tx_id
WHERE t.from_owner != t.to_owner
  AND t.token_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC token address
  -- Filter out DEX trades
  AND NOT EXISTS (
    SELECT 1
    FROM {{ ref('dex_solana_trades') }} dt
    WHERE dt.tx_id = t.tx_id
  )