{{
    config(
        
        schema = 'blur_ethereum',
        alias = 'blend_events',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blur",
                                \'["hildobby"]\') }}'
    )
}}

WITH offer_taken AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Loan' AS event
    , collection
    , tokenId AS token_id
    , loanAmount/1e18 as amount
    , rate/1e4 as apy_rate
    , auctionDuration AS auction_duration
    , lender
    , borrower
    , offerHash AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    , contract_address
    FROM {{ source('blur_ethereum', 'Blend_evt_LoanOfferTaken') }}
    )

, all_events AS (
    SELECT block_time
    , block_number
    , lien_id
    , event
    , collection
    , amount
    , apy_rate
    , auction_duration
    , lender
    , borrower
    , offer_hash
    , tx_hash
    , evt_index
    FROM offer_taken
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Refinance' AS event
    , collection
    , newAmount/1e18 as amount
    , newRate/1e4 as apy_rate
    , NULL AS auction_duration
    , newLender AS lender
    , NULL AS borrower
    , NULL AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    FROM {{ source('blur_ethereum', 'Blend_evt_Refinance') }}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Seize' AS event
    , collection
    , 0 AS amount
    , 0 AS apy_rate
    , NULL AS auction_duration
    , NULL AS lender
    , NULL AS borrower
    , NULL AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    FROM {{ source('blur_ethereum', 'Blend_evt_Seize') }}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Repay' AS event
    , collection
    , 0 AS amount
    , 0 AS apy_rate
    , NULL AS auction_duration
    , NULL AS lender
    , NULL AS borrower
    , NULL AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    FROM {{ source('blur_ethereum', 'Blend_evt_Repay') }}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Start Auction' AS event
    , collection
    , 0 AS amount
    , 0 AS apy_rate
    , NULL AS auction_duration
    , NULL AS lender
    , NULL AS borrower
    , NULL AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    FROM {{ source('blur_ethereum', 'Blend_evt_StartAuction') }}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , lienId AS lien_id
    , 'Sell' AS event
    , collection
    , 0 AS amount
    , 0 AS apy_rate
    , NULL AS auction_duration
    , NULL AS lender
    , NULL AS borrower
    , NULL AS offer_hash
    , evt_tx_hash AS tx_hash
    , evt_index
    FROM {{ source('blur_ethereum', 'Blend_evt_BuyLocked') }}
    )

SELECT a.block_time
, a.block_number
, lien_id
, a.event
, a.collection AS nft_contract_address
, tok.name AS nft_collection_name
, token_id
, a.amount AS amount_eth
, a.amount*pu.price AS amount_usd
, a.apy_rate
, a.auction_duration
, CASE WHEN a.event IN ('Loan', 'Refinance') THEN a.lender
    ELSE LAG(a.lender, 1) IGNORE NULLS OVER (PARTITION BY lien_id ORDER BY a.block_time, a.evt_index)
    END AS lender
, CASE WHEN a.event = 'Loan' THEN a.borrower
    ELSE LAG(a.borrower, 1) IGNORE NULLS OVER (PARTITION BY lien_id ORDER BY a.block_time, a.evt_index)
    END AS borrower
, a.offer_hash
, a.tx_hash
, txs.index AS tx_index
, a.evt_index
FROM all_events a
INNER JOIN (SELECT distinct lien_id, token_id FROM offer_taken) USING (lien_id)
LEFT JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=a.block_number
    AND txs.hash=a.tx_hash
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON tok.contract_address=a.collection
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='ethereum'
    AND pu.contract_address=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    AND pu.minute=date_trunc('minute', a.block_time)