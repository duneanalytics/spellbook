{{ config(
        alias ='wash_trades',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

SELECT
    nftt.blockchain
    , nftt.project
    , nftt.version
    , nftt.nft_contract_address
    , nftt.token_id
    , nftt.token_standard
    , nftt.trade_category
    , nftt.buyer
    , nftt.seller
    , nftt.project_contract_address
    , nftt.aggregator_name
    , nftt.aggregator_address
    , nftt.tx_from
    , nftt.tx_to
    , nftt.block_time
    , nftt.block_date
    , nftt.block_number
    , nftt.tx_hash
    , nftt.unique_trade_id
    , filter_funding_buyer.first_funded_by AS buyer_first_funded_by
    , filter_funding_seller.first_funded_by AS seller_first_funded_by
    , count(1)
    -- , CASE WHEN COUNT(distinct filter_baf.block_number) > 0
    --     THEN true
    --     ELSE false 
    --     END AS back_and_forth_trade
    -- , CASE WHEN COUNT(distinct filter_bought_3x.block_number) > 2
    --     THEN true
    --     ELSE false
    --     END AS bought_it_three_times_within_a_week
    -- , CASE WHEN 
    --     (
    --         filter_funding_buyer.first_funded_by = filter_funding_seller.first_funded_by
    --         AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM labels.bridges)
    --         AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM labels.cex)
    --     )
    --     OR filter_funding_buyer.first_funded_by = nftt.seller
    --     OR filter_funding_seller.first_funded_by = nftt.buyer
    --     THEN true
    --     ELSE false
    --     END AS funded_by_same_wallet
    -- , CASE WHEN COUNT(filter_baf.block_number) > 0
    --     OR COUNT(filter_bought_3x.block_number) > 2
    --     OR
    --     (
    --         filter_funding_buyer.first_funded_by = filter_funding_seller.first_funded_by
    --         AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM labels.bridges)
    --         AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM labels.cex)
    --     )
    --     OR filter_funding_buyer.first_funded_by = nftt.seller
    --     OR filter_funding_seller.first_funded_by = nftt.buyer
    --     THEN true
    --     ELSE false
    --     END AS is_wash_trade
FROM nft.trades nftt
LEFT JOIN nft.trades filter_baf
    ON filter_baf.seller=nftt.buyer
    AND filter_baf.buyer=nftt.seller
    AND filter_baf.nft_contract_address=nftt.nft_contract_address
    AND filter_baf.token_id=nftt.token_id
    AND (filter_baf.block_time BETWEEN nftt.block_time - interval '1 week' AND nftt.block_time + interval '1 week')
    AND filter_baf.block_time >= date_trunc("day", NOW() - interval '2 weeks')
LEFT JOIN nft.trades filter_bought_3x
    ON filter_bought_3x.nft_contract_address=nftt.nft_contract_address
    AND filter_bought_3x.token_id=nftt.token_id
    AND filter_bought_3x.buyer=nftt.buyer
    AND filter_bought_3x.token_standard IN ('erc721', 'erc20')
    AND (filter_bought_3x.block_time BETWEEN nftt.block_time - interval '1 week' AND nftt.block_time + interval '1 week')
    AND filter_bought_3x.block_time >= date_trunc("day", NOW() - interval '2 weeks')
LEFT JOIN dbt_jeff_addresses_ethereum.first_funded_by filter_funding_buyer
    ON filter_funding_buyer.address=nftt.buyer
LEFT JOIN dbt_jeff_addresses_ethereum.first_funded_by filter_funding_seller
    ON filter_funding_seller.address=nftt.seller
WHERE nftt.blockchain='ethereum'
    AND nftt.block_time >= date_trunc("day", NOW() - interval '1 week')
GROUP BY
    nftt.blockchain
    , nftt.project
    , nftt.version
    , nftt.nft_contract_address
    , nftt.token_id
    , nftt.token_standard
    , nftt.trade_category
    , nftt.buyer
    , nftt.seller
    , nftt.project_contract_address
    , nftt.aggregator_name
    , nftt.aggregator_address
    , nftt.tx_from
    , nftt.tx_to
    , nftt.block_time
    , nftt.block_number
    , nftt.tx_hash
    , nftt.unique_trade_id
    , filter_funding_buyer.first_funded_by
    , filter_funding_seller.first_funded_by
 order by count(1) desc
 ;