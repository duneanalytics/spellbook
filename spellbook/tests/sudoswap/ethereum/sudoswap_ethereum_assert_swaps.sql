-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH 
    raw_swaps as (
        with fil_swaps as (
                SELECT 
                    * 
                FROM (
                    SELECT 
                        contract_address
                        , call_tx_hash
                        , call_trace_address
                        , call_block_time
                        , call_block_number
                        , call_success
                        , tokenRecipient as call_from
                        , 'Sell' as trade_category 
                    FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapNFTsForToken') }} s1
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'
                    AND call_success = true
                    AND contract_address = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'

                    UNION ALL
                    SELECT 
                        contract_address
                        , call_tx_hash
                        , call_trace_address
                        , call_block_time
                        , call_block_number
                        , call_success
                        , nftRecipient as call_from
                        , 'Buy' as trade_category 
                    FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForAnyNFTs') }} s2
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'
                    AND call_success = true
                    AND contract_address = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'

                    UNION ALL
                    SELECT
                        contract_address
                        , call_tx_hash
                        , call_trace_address
                        , call_block_time
                        , call_block_number
                        , call_success
                        , nftRecipient as call_from
                        , 'Buy' as trade_category 
                    FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForSpecificNFTs') }} s3
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'
                    AND call_success = true
                    AND contract_address = '0xef1a89cbfabe59397ffda11fc5df293e9bc5db90'
                ) s
            )

        SELECT 
            COUNT(*) as num_trades
            , COUNT(distinct call_tx_hash) as num_txs
        FROM fil_swaps
    ),

    abstractions_swaps as
    (
        SELECT
            COUNT(*) as num_trades
            COUNT(distinct tx_hash) as num_txs
        FROM {{ ref('nft_trades') }} nft
        WHERE blockchain = 'ethereum' AND project = 'sudoswap' and version = 'v1'
        AND block_time >= '2022-07-10' AND block_time <= '2022-08-10'
    )

SELECT 
    (SELECT num_trades FROM abstractions_swaps) - (SELECT num_trades FROM raw_swaps) as trades_mismatch
    , (SELECT num_txs FROM abstractions_swaps) - (SELECT num_txs FROM raw_swaps) as txs_mismatch
FROM 1
WHERE (SELECT num_trades FROM abstractions_swaps) - (SELECT num_trades FROM raw_swaps) > 0
    OR (SELECT num_txs FROM abstractions_swaps) - (SELECT num_txs FROM raw_swaps) > 0