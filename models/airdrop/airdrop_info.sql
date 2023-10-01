{{ config(
        tags = ['dunesql', 'static'],
        schema='airdrop',
        alias = alias('info'),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]\',
                                    "sector",
                                    "dex",
                                    \'["hildobby"]\') }}')
}}

SELECT project, name, eligible_addresses, airdropped_total, circulating_supply_on_drop, max_supply, token_issued_by_protocol, x_username, CAST(airdrop_start AS TIMESTAMP) AS airdrop_start, CAST(airdrop_end AS TIMESTAMP) AS airdrop_end
FROM (VALUES
    ('notional_finance', 'Notional Finance', NULL, NULL, NULL, NULL, TRUE, 'NotionalFinance', '2021-11-01 15:54', '2023-07-09 14:04')
    , ('giveth', 'Giveth', NULL, NULL, NULL, NULL, TRUE, 'Giveth', '2021-12-24 12:18', '2022-12-30 00:05')
    , ('ethereum_name_service', 'Ethereum Name Service', 137689, 25000000, 30093058, 100000000, TRUE, 'ensdomains', '2021-11-09', '2022-07-31')
    , ('forefront', 'Forefront', NULL, NULL, NULL, NULL, TRUE, 'forefront__', '2021-03-31 10:09', '2021-04-15 07:40')
    , ('optimism', 'Optimism', 248699, 214748365, 214748365, 4294967296, TRUE, 'optimismFND', '2022-05-31 14:39', '2023-07-11 21:41')
    , ('arbitrum', 'Arbitrum', 625143, 1275000000, 4853500000, NULL, TRUE, 'arbitrum', '2023-03-23 13:01', NULL)
    , ('forta_network', 'Forta Network', NULL, NULL, NULL, NULL, TRUE, 'FortaNetwork', '2022-06-14 22:15', '2022-09-15 11:00')
    , ('uniswap', 'Uniswap', 252803, 150000000, 150000000, 1000000000, TRUE, 'Uniswap', '2020-09-16 21:30', NULL)
    , ('cow_protocol', 'CoW Protocol', NULL, NULL, NULL, NULL, TRUE, 'CoWSwap', '2022-02-11 20:38', '2022-03-25 06:01')
    , ('value_defi', 'Value DeFi', NULL, NULL, NULL, NULL, TRUE, 'value_defi', '2021-01-19 11:40', '2021-09-25 13:00')
    , ('genie', 'Genie', 15823, 54800000, 693000000, 1000000000, FALSE, 'geniexyz', '2022-11-23 16:26', NULL)
    , ('dappradar', 'DappRadar', NULL, NULL, NULL, NULL, TRUE, 'DappRadar', '2021-12-14 19:09', '2022-03-19 01:45')
    , ('thales', 'Thales', NULL, NULL, NULL, NULL, TRUE, 'thalesmarket', '2021-09-15 07:36', '2022-02-01 10:35')
    , ('shapeshift', 'ShapeShift', NULL, NULL, NULL, NULL, TRUE, 'ShapeShift', '2021-07-08 21:11', '2021-10-21 13:15')
    , ('gearbox', 'Gearbox Protocol', NULL, NULL, NULL, NULL, TRUE, 'GearboxProtocol', '2022-04-05 18:23', '2022-07-21 04:00')
    , ('gas_dao', 'Gas DAO', NULL, NULL, NULL, NULL, TRUE, '0xGasDAO', '2021-12-29 03:39', NULL)
    , ('tokenfy', 'Tokenfy', NULL, NULL, NULL, NULL, TRUE, 'tokenfy_', '2022-01-21 18:58', '2022-02-04 19:00')
    , ('apecoin', 'ApeCoin', 15757, 150000000, 277500000, 1000000000, TRUE, 'apecoin', '2022-03-17 12:13', '2022-06-15 12:00')
    , ('sudoswap', 'Sudoswap', NULL, 26920000, 42000000, 60000000, TRUE, 'sudoswap', '2023-01-30 18:56', '2023-05-25 00:42')
    , ('hop_protocol', 'Hop Protocol', 145329, 80000000, 693000000, 1000000000, TRUE, 'HopProtocol', '2022-06-09 18:26', NULL)
    , ('paraswap', 'Paraswap', 20000, 150000000, NULL, 2000000000, TRUE, 'paraswap', '2021-11-15 11:52', NULL)
    , ('alchemydao', 'AlchemyDAO', NULL, NULL, NULL, NULL, TRUE, 'AlchemyPlatform','2021-03-28 12:50', '2021-04-18 11:00')
    , ('snowswap', 'SnowSwap', NULL, NULL, NULL, NULL, TRUE, 'snow_swap', '2020-10-14 02:20', '2020-10-30 13:00')
    , ('botto', 'Botto', NULL, NULL, NULL, NULL, TRUE, 'bottoproject', '2021-10-07 15:51', '2022-01-08 21:02')
    , ('galxe', 'Galxe', NULL, NULL, NULL, NULL, TRUE, 'Galxe', '2022-05-05 12:00', '2022-06-04 11:00')
    , ('blur', 'Blur', NULL, 360000000, 360000000, 3000000000, TRUE, 'blur_io', '2023-02-14 18:19', '2023-04-23 17:11')
    , ('dydx', 'DYDX', NULL, NULL, NULL, NULL, TRUE, 'dYdX', '2021-09-08 05:24', NULL)
    , ('pangolin', 'Pangolin', NULL, NULL, NULL, NULL, TRUE, 'pangolindex', '2021-02-09 17:00', '2021-03-10')
    , ('looksrare', 'LooksRare', 185223, 120000000, 137000000, 1000000000, TRUE, 'LooksRare', '2022-01-10 08:48', '2022-03-18 08:10')
    , ('ampleforth', 'Ampleforth', NULL, 15000000, NULL, NULL, TRUE, 'AmpleforthOrg', '2021-04-20 01:25', '2022-04-16')
    , ('x2y2', 'X2Y2', 861417, 120000000, 135000000, 1000000000, TRUE, 'the_x2y2', '2022-02-15 04:26', '2022-03-31')
    , ('ellipsis_finance', 'Ellipsis Finance', NULL, NULL, NULL, NULL, TRUE, 'Ellipsisfi', '2021-03-24 21:41', '2022-03-31 13:00')
    , ('tornado_cash', 'Tornado Cash', NULL, NULL, NULL, NULL, TRUE, 'TornadoCash', '2020-12-18 03:00', '2021-12-13')
    , ('benddao', 'BendDAO', NULL, NULL, NULL, NULL, TRUE, 'BendDAO', '2022-03-21 04:54', '2022-06-18 15:04')
    , ('component', 'Component', NULL, NULL, NULL, NULL, TRUE, 'componentx', '2021-04-27 13:12', '2021-07-28 10:00')
    , ('safe', 'Safe', NULL, NULL, NULL, NULL, TRUE, 'safe', '2022-09-28 07:00', NULL)
    , ('ribbon', 'Ribbon', NULL, NULL, NULL, NULL, TRUE, 'ribbonfinance', '2021-05-24 15:00', NULL)
    , ('oneinch', '1inch Protocol', NULL, NULL, NULL, NULL, TRUE, '1inch', '2020-12-25 00:08', NULL)
    , ('gitcoin', 'Gitcoin', NULL, NULL, NULL, NULL, TRUE, 'gitcoin', '2021-05-24 20:40', '2021-06-23 15:08')
    , ('paladin', 'Paladin', NULL, NULL, NULL, NULL, TRUE, 'Paladin_vote', '2021-11-03 20:08', '2022-02-01 13:51')
    , ('pooltogether', 'PoolTogether', NULL, NULL, NULL, NULL, TRUE, 'PoolTogether_', '2021-02-17 02:18', NULL)
    , ('diva', 'Diva', NULL, 106000000, NULL, 1000000000, TRUE, 'divastaking', NULL, NULL)
    , ('arkham', 'Arkham', NULL, NULL, NULL, NULL, TRUE, 'ArkhamIntel', '2023-06-27 19:00', NULL)
    ) AS temp_table (project, name, eligible_addresses, circulating_supply_on_drop, max_supply, airdropped_total, token_issued_by_protocol, x_username, airdrop_start, airdrop_end)