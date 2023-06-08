{{
    config(
        schema = 'blend_ethereum',
        alias='buy_locked',
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

SELECT bl.evt_block_time AS block_time
, bl.evt_block_number AS block_number
, 'buylocked' AS evt_type
, bl.collection AS nft_smart_contract
, l.token_id
, NULL AS loan_amount_raw
, NULL AS loan_amount_original
, NULL AS rate
, bl.buyer AS borrower
, NULL AS lender
, bl.evt_tx_hash AS tx_hash
, bl.evt_index
, bl.contract_address
, NULL AS offer_hash
, bl.lienId AS lien_id
, NULL AS auction_duration
FROM blur_ethereum.Blend_evt_BuyLocked bl
INNER JOIN loan_ids l ON bl.lienId=l.lien_id