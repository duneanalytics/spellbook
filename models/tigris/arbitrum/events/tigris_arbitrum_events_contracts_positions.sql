{{ config(
    
        alias = 'events_contracts_positions'
        )
}}

WITH 

hardcoded_positions as ( -- harcoding the position contracts since there's no event logged to get the position contract of the trading contracts
        SELECT 
            trading_contract, 
            positions_contract,
            trading_contract_version,
            blockchain
        FROM (
        VALUES 
        -- v1 trading contracts 
            (0xe17a2829f0c23c02e662c616081dcad18dcbb7e4, 0xb75bf135a05665855377a689d39203750cba6c30, 'v1.2', 'arbitrum'),
            (0xDDe031307c185aB3FA1b51874f4EE57841B20292, 0xb75bf135a05665855377a689d39203750cba6c30, 'v1.3', 'arbitrum'),
            (0x0CC23BF1761C85e010D257F02fd638d4E4221579, 0xb75bf135a05665855377a689d39203750cba6c30, 'v1.4', 'arbitrum'),
            (0x6c5Da3f6A1f1B41feE2aA4a86b935272663b4957, 0xb75bf135a05665855377a689d39203750cba6c30, 'v1.5', 'arbitrum'),
        -- v2 trading contracts 
            (0x62F0a3f138E762d08Eff0651857eF3e51CEE6742, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.1', 'arbitrum'),
            (0xcF6d276dd9F4203ae56ba62DED3F5d1120243eaA, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.2', 'arbitrum'),
            (0x2B6026d7b69f0fa4e703D965Bb0FEF0Fa838fEad, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.3', 'arbitrum'),
            (0x40cde4820Ec2270511D36dB418a14A4AFf16276b, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.4', 'arbitrum'),
            (0x399214eE22bF068ff207adA462EC45046468B766, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.5', 'arbitrum'),
            (0xd89B4B1C8918150f137cc68E83E3876F9e309aB9, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.6', 'arbitrum'),
        -- v2 options contracts 
            (0xc6d1ba6363ffe4fdda9ffbea8d91974de9775331, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.1', 'arbitrum'),
            (0x98125e58bc966894167c536652d7648f6beebf05, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.2', 'arbitrum'),
            (0x8895b0b946b3d5bcd7d1e9e31dcfaeb51644922a, 0x09D74999e5315044956ad15D5F2Aeb8d393E85eD, 'v2.3', 'arbitrum')
        ) as temp_table (trading_contract, positions_contract, trading_contract_version, blockchain)
)

SELECT * FROM hardcoded_positions