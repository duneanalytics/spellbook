{{
    config(
        schema = 'tokens_avalanche_c'
        ,alias = 'erc20'
        ,tags=['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
    (0xb44a9b6905af7c801311e8f4e76932ee959c663c, 'ANY', 18)
    , (0xbec243c995409e6520d7c41e404da5deba4b209b, 'SNX.e', 18)
    , (0xccc9b2c9d96c33cecc064ddd444b132eff56e232, 'bWETH', 18)
    , (0x609268b9c47c7be0a8d77ae93c31d2bf6859d175, 'LONG', 18)
    , (0x3b9e3b5c616a1a038fdc190758bbe9bab6c7a857, 'UNCX', 18)
    , (0x81ccdd9e44c518caee2f720c43cd0853032a1779, 'bWBTC', 8)
    , (0xaaae58986b24e422740c8f22b3efb80bcbd68159, 'xPHAR', 18)
    , (0x4036f3d9c45a20f44f0b8b85dd6ca33005ff9654, 'ROOBEE', 18)
    , (0xa6772f1efedef231a3d92851470bd73316ddeaa9, 'sBLIGHT', 9)
    , (0x4a4f77d74cf5fd4ea4ab71ba79988c055a5c27b2, 'LESS', 18)
    , (0x6807ed4369d9399847f306d7d835538915fa749d, 'bDAI', 18)
    , (0x913c61ec3573e5e4ee6488552535fb1be84ff2ac, 'XAV', 18)
    , (0x420fca0121dc28039145009570975747295f2329, 'COQ', 18)
    , (0x298c5c64eba94b8dd425582e4266a882db6d9848, 'SaAVAXb', 18)
    , (0x9767203e89dcd34851240b3919d4900d3e5069f1, 'A4', 6)
) AS temp_table (contract_address, symbol, decimals)