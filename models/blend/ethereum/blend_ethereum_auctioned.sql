{{
    config(
        schema = 'blend_ethereum',
        alias='auctioned',
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
, 'auctioned' AS evt_type
, r.collection AS nft_smart_contract
, l.token_id
, NULL AS loan_amount_raw
, NULL AS loan_amount_original
, NULL AS rate
, NULL AS borrower
, NULL AS lender
, r.evt_tx_hash AS tx_hash
, r.evt_index
, r.contract_address
, NULL AS offer_hash
, r.lienId AS lien_id
, NULL AS auction_duration
FROM blur_ethereum.Blend_evt_StartAuction r
INNER JOIN loan_ids l ON r.lienId=l.lien_id