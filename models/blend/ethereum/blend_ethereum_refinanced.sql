{{
    config(
        schema = 'blend_ethereum',
        alias='refinanced',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        unique_key = ['tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blend",
                                \'["hildobby"]\') }}'
    )
}}

WITH loan_ids AS (
    SELECT distinct lien_id 
    , token_id
    FROM blur_ethereum.Blend_evt_LoanOfferTaken
    )

SELECT distinct r.evt_block_time AS block_time
, r.evt_block_number AS block_number
, 'refinanced' AS evt_type
, r.collection AS nft_smart_contract
, l.token_id
, CAST(r.newAmount AS double) AS loan_amount_raw
, CAST(r.newAmount AS double)/1e18 AS loan_amount_original
, r.newRate AS rate
, NULL AS borrower
, r.newLender AS lender
, r.evt_tx_hash AS tx_hash
, r.evt_index
, r.contract_address
, NULL AS offer_hash
, r.lienId AS lien_id
, NULL AS auction_duration
FROM blur_ethereum.Blend_evt_Refinance r
INNER JOIN loan_ids l ON r.lienId=l.lien_id