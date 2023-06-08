{{
    config(
        schema = 'blend_ethereum',
        alias='active_loans',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        unique_key = ['lien_id'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blend",
                                \'["hildobby"]\') }}'
    )
}}

SELECT evt_block_time AS block_time
, evt_block_number AS block_number
, 'started' AS evt_type
, collection AS nft_smart_contract
, tokenId AS token_id
, CAST(loanAmount AS double) AS loan_amount_raw
, CAST(loanAmount AS double)/1e18 AS loan_amount_original
, pu.price*CAST(loanAmount AS double)/1e18 AS loan_amount_usd
, rate/1e6 AS rate
, borrower
, lender
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, offerHash AS offer_hash
, lienId AS lien_id
, auctionDuration AS auction_duration
FROM blur_ethereum.Blend_evt_LoanOfferTaken