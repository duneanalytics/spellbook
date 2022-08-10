-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH 
    raw_swaps as (
        WITH
            pairs_created as (
                SELECT 
                    _nft as nftcontractaddress
                    , _initialNFTIDs as nft_ids
                    , _fee as initialfee
                    , _assetRecipient as asset_recip
                    , output_pair as pair_address
                    , call_block_time as block_time
                FROM {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }}
                WHERE call_success
                AND _nft = lower('0xeF1a89cbfAbE59397FfdA11Fc5DF293E9bC5Db90') --here just so the test runs faster
            ),

            swaps as (
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
                    join pairs_created pc ON s1.contract_address = pc.pair_address
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'

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
                    join pairs_created pc ON s2.contract_address = pc.pair_address
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'

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
                    join pairs_created pc ON s3.contract_address = pc.pair_address
                    where call_block_time >= '2022-07-10' AND call_block_time <= '2022-08-10'
                ) s
            )

        SELECT 
            COUNT(*) as num_trades
            , COUNT(distinct call_tx_hash) as num_txs
        FROM swaps
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