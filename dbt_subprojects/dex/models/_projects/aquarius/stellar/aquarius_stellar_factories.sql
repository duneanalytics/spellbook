{{ config(
    schema = 'aquarius_stellar'
    , alias = 'factories'
    , materialized = 'view'
    , tags = ['static']
    )
}}

-- Canonical Aquarius AMM contracts on Stellar.
-- https://docs.aqua.network/developers/reference/addresses-and-networks
-- ci-stamp: 1

SELECT
    blockchain
    , project
    , version
    , contract_id
    , contract_name
FROM (
    VALUES
        ('stellar', 'aquarius', '1', 'CBQDHNBFBZYE4MKPWBSJOPIYLW4SFSXAXUTSXJN76GNKYVYPCKWC6QUK', 'router')
        , ('stellar', 'aquarius', '1', 'CA4Q2T6FRAFYJYSMDJV7F6B7RL5PS6QS2UOZHBMCT2KSMGQRAAKP2MKO', 'provider_fee_factory')
) AS t(blockchain, project, version, contract_id, contract_name)
