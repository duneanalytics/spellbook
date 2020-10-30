CREATE OR REPLACE VIEW makermcd.view_collateral_addresses AS

-- Addresses are JOIN addresses from here https://changelog.makerdao.com/releases/mainnet/1.1.3/contracts.json

WITH maker(adr,project, details) AS (VALUES 
        ('\x2f0b23f53734252bda2277357e97e1517d6b042a'::bytea, 'MakerDAO', 'WETH A address'::text),
        ('\x08638eF1A205bE6762A8b935F5da9b700Cf7322c'::bytea, 'MakerDAO', 'WETH B address'::text),
        ('\x3d0b1912b66114d4096f48a8cee3a56c231772ca'::bytea, 'MakerDAO', 'BAT address'::text),
        ('\xA191e578a6736167326d05c119CE0c90849E84B7'::bytea, 'MakerDAO', 'USDC A address'::text),
        ('\x2600004fd1585f7270756DDc88aD9cfA10dD0428'::bytea, 'MakerDAO', 'USDC B address'::text),
        ('\x4454aF7C8bb9463203b66C816220D41ED7837f44'::bytea, 'MakerDAO', 'TUSD address'::text),
        ('\xc7e8Cd72BDEe38865b4F5615956eF47ce1a7e5D0'::bytea, 'MakerDAO', 'ZRX address'::text),
        ('\x475F1a89C1ED844A08E8f6C50A00228b5E59E4A9'::bytea, 'MakerDAO', 'KNC address'::text),
        ('\xA6EA3b9C04b8a38Ff5e224E7c3D6937ca44C0ef9'::bytea, 'MakerDAO', 'Mana address'::text),
        ('\x0Ac6A1D74E84C2dF9063bDDc31699FF2a2BB22A2'::bytea, 'MakerDAO', 'USDT address'::text),
        ('\x7e62B7E279DFC78DEB656E34D6a435cC08a44666'::bytea, 'MakerDAO', 'PAXUSD address'::text),
        ('\xBEa7cDfB4b49EC154Ae1c0D731E4DC773A3265aA'::bytea, 'MakerDAO', 'COMP address'::text),
        ('\x6C186404A7A238D3d6027C0299D1822c1cf5d8f1'::bytea, 'MakerDAO', 'LRC address'::text),
        ('\xdFccAf8fDbD2F4805C174f856a317765B49E4a50'::bytea, 'MakerDAO', 'LINK address'::text),
        ('\xBF72Da2Bd84c5170618Fbe5914B0ECA9638d5eb5'::bytea, 'MakerDAO', 'WBTC address'::text)
        )

SELECT adr, project, details FROM maker
