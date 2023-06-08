{{
    config(
        schema = 'blend_ethereum',
        alias='seized',
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

SELECT s.evt_block_time AS block_time
, s.evt_block_number AS block_number
, 'seize' AS evt_type
, s.collection AS nft_smart_contract
--, l.token_id AS token_id
, l.token_id
, NULL AS loan_amount_raw
, NULL AS loan_amount_original
, NULL AS rate
, NULL AS borrower
, NULL AS lender
, s.evt_tx_hash AS tx_hash
, s.evt_index
, s.contract_address
, NULL AS offer_hash
, s.lienId AS lien_id
, NULL AS auction_duration
FROM blur_ethereum.Blend_evt_Seize s
INNER JOIN loan_ids l ON s.lienId=l.lien_id