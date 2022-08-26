-- -- test to see if # trades in raw = # trades in processed.
WITH
  raw_swaps as (
    WITH
      pairs_created as (
        SELECT
          _nft as nftcontractaddress,
          _initialNFTIDs as nft_ids,
          _fee as initialfee,
          _assetRecipient as asset_recip,
          output_pair as pair_address,
          call_block_time as block_time
        FROM
            {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }}
        WHERE
          call_success
          AND _nft = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'
      ),
      fil_swaps as (
        SELECT
          *
        FROM
          (
            SELECT
              contract_address,
              call_tx_hash,
              call_trace_address,
              call_block_time,
              call_block_number,
              call_success,
              tokenRecipient as call_from,
              'Sell' as trade_category
            FROM 
                {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapNFTsForToken') }}
                join pairs_created pc ON contract_address = pc.pair_address
            where
              call_block_time >= '2022-07-10'
              AND call_block_time <= '2022-08-10'
              AND call_success = true
            UNION ALL
            SELECT
              contract_address,
              call_tx_hash,
              call_trace_address,
              call_block_time,
              call_block_number,
              call_success,
              nftRecipient as call_from,
              'Buy' as trade_category
            FROM
                {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForAnyNFTs') }}
              join pairs_created pc ON contract_address = pc.pair_address
            where
              call_block_time >= '2022-07-10'
              AND call_block_time <= '2022-08-10'
              AND call_success = true
            UNION ALL
            SELECT
              contract_address,
              call_tx_hash,
              call_trace_address,
              call_block_time,
              call_block_number,
              call_success,
              nftRecipient as call_from,
              'Buy' as trade_category
            FROM
                {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForSpecificNFTs') }}
              join pairs_created pc ON contract_address = pc.pair_address
            where
              call_block_time >= '2022-07-10'
              AND call_block_time <= '2022-08-10'
              AND call_success = true
          ) s
      )
    SELECT
      COUNT(distinct call_tx_hash) as num_txs
    FROM
      fil_swaps
  ),
  abstractions_swaps as (
    SELECT
      COUNT(distinct tx_hash) as num_txs
    FROM
        {{ ref('nft_trades') }} nft
    WHERE
      blockchain = 'ethereum'
      AND project = 'sudoswap'
      and version = 'v1'
      AND block_time >= '2022-07-10'
      AND block_time <= '2022-08-10'
      AND nft_contract_address = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'
  ),
  test as (
    SELECT
      (
        SELECT
          num_txs
        FROM
          abstractions_swaps
      ) - (
        SELECT
          num_txs
        FROM
          raw_swaps
      ) as txs_mismatch
  )
SELECT
  *
FROM
  test
WHERE
  txs_mismatch > 0