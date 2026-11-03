{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'factories'
    , materialized = 'view'
    , tags = ['static']
    )
}}

-- Canonical SushiSwap V3 contracts on Stellar.
-- https://docs.sushi.com/sdk/reference/stellar/config
-- ci-stamp: 2

SELECT
    blockchain
    , project
    , version
    , contract_id
    , contract_name
FROM (
    VALUES
        ('stellar', 'sushiswap', '3', 'CD3KRKGDRVWPXVB3VXLUMQKMX6XZ6Q2H334IVZD4XXNAMKSRVQL5GLYF', 'factory')
        , ('stellar', 'sushiswap', '3', 'CARTUL5AWDZYBSN7HUUJZSKCAKCIAKM7M54Z76G6KRYCK4XPR3OHUQZ4', 'position_manager')
        , ('stellar', 'sushiswap', '3', 'CASKWJSINHFW7BF7RUOA4E2FP6B2TYRKFX2UOPWLCPOOPUR6UU3G2RWC', 'tick_lens')
        , ('stellar', 'sushiswap', '3', 'CDMIM23WOUL5CZBKX3GOA3V5R5AMVIMTCP52KCDQORWELAPLJ27WZCHL', 'quoter')
) AS t(blockchain, project, version, contract_id, contract_name)
