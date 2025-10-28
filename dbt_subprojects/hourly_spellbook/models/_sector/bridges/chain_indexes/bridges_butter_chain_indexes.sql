{{ config(
    schema = 'bridges',
    alias = 'butter_chain_indexes',
    materialized = 'view',
    )
}}

SELECT id, blockchain
    FROM (VALUES
    (1, 'ethereum')
    , (127, 'polygon')
    , (56, 'bnb')
    ) AS x (id, blockchain)