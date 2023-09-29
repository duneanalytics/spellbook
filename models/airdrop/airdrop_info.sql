{{ config(
        tags = ['dunesql', 'static'],
        schema='airdrop',
        alias = alias('info'),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]\',
                                    "sector",
                                    "dex",
                                    \'["hildobby"]\') }}')
}}

SELECT project, name, eligible_addresses, airdropped_total, circulating_supply_on_drop, max_supply, token_issued_by_protocol, x_username
FROM (VALUES
    ('notional_finance', 'Notional Finance', NULL, NULL, NULL, NULL, NULL, TRUE, 'NotionalFinance')
    , ('giveth', 'Giveth', NULL, NULL, NULL, NULL, NULL, TRUE, 'Giveth')
    , ('ethereum_name_service', 'Ethereum Name Service', TRUE, NULL, NULL, NULL, NULL, TRUE, 'ensdomains')
    , ('forefront', 'Forefront', NULL, NULL, NULL, NULL, NULL, TRUE, 'forefront__')
    , ('optimism', 'Optimism', TRUE, 248699, 214748365, 214748365, 4294967296, TRUE, 'optimismFND')
    , ('arbitrum', 'Arbitrum', TRUE, NULL, 1275000000, 4853500000, NULL, TRUE, 'arbitrum')
    , ('forta_network', 'Forta Network', NULL, NULL, NULL, NULL, NULL, TRUE, 'FortaNetwork')
    , ('uniswap', 'Uniswap', TRUE, NULL, NULL, NULL, NULL, TRUE, 'Uniswap')
    , ('cow_protocol', 'CoW Protocol', NULL, NULL, NULL, NULL, NULL, TRUE, 'CoWSwap')
    , ('value_defi', 'Value DeFi', NULL, NULL, NULL, NULL, NULL, TRUE, 'value_defi')
    , ('genie', 'Genie', NULL, NULL, NULL, NULL, NULL, FALSE, 'geniexyz')
    , ('dappradar', 'DappRadar', NULL, NULL, NULL, NULL, NULL, TRUE, 'DappRadar')
    , ('thales', 'Thales', NULL, NULL, NULL, NULL, NULL, TRUE, 'thalesmarket')
    , ('shapeshift', 'ShapeShift', NULL, NULL, NULL, NULL, NULL, TRUE, 'ShapeShift')
    , ('gearbox', 'Gearbox Protocol', NULL, NULL, NULL, NULL, NULL, TRUE, 'GearboxProtocol')
    , ('gas_dao', 'Gas DAO', NULL, NULL, NULL, NULL, NULL, TRUE, '0xGasDAO')
    , ('tokenfy', 'Tokenfy', NULL, NULL, NULL, NULL, NULL, TRUE, 'tokenfy_')
    , ('apecoin', 'ApeCoin', NULL, NULL, NULL, NULL, NULL, TRUE, 'apecoin')
    , ('sudoswap', 'Sudoswap', NULL, NULL, NULL, NULL, NULL, TRUE, 'sudoswap')
    , ('hop_protocol', 'Hop Protocol', NULL, NULL, NULL, NULL, NULL, TRUE, 'HopProtocol')
    , ('paraswap', 'Paraswap', NULL, NULL, NULL, NULL, NULL, TRUE, 'paraswap')
    , ('alchemydao', 'AlchemyDAO', NULL, NULL, NULL, NULL, NULL, TRUE, 'AlchemyPlatform')
    , ('snowswap', 'SnowSwap', NULL, NULL, NULL, NULL, TRUE, NULL, 'snow_swap')
    , ('botto', 'Botto', NULL, NULL, NULL, NULL, NULL, TRUE, 'bottoproject')
    , ('galxe', 'Galxe', NULL, NULL, NULL, NULL, NULL, TRUE, 'Galxe')
    , ('blur', 'Blur', NULL, NULL, NULL, NULL, NULL, TRUE, 'blur_io')
    , ('dydx', 'DYDX', NULL, NULL, NULL, NULL, NULL, TRUE, 'dYdX')
    , ('pangolin', 'Pangolin', NULL, NULL, NULL, NULL, NULL, TRUE, 'pangolindex')
    , ('looksrare', 'LooksRare', TRUE, 185223, 120000000, 137000000, 1000000000, TRUE, 'LooksRare')
    , ('ampleforth', 'Ampleforth', NULL, NULL, NULL, NULL, NULL, TRUE, 'AmpleforthOrg')
    , ('x2y2', 'X2Y2', TRUE, NULL, NULL, NULL, NULL, TRUE, 'the_x2y2')
    , ('ellipsis_finance', 'Ellipsis Finance', NULL, NULL, NULL, NULL, NULL, TRUE, 'Ellipsisfi')
    , ('tornado_cash', 'Tornado Cash', NULL, NULL, NULL, NULL, NULL, TRUE, 'TornadoCash')
    , ('benddao', 'BendDAO', NULL, NULL, NULL, NULL, NULL, TRUE, 'BendDAO')
    , ('component', 'Component', NULL, NULL, NULL, NULL, NULL, TRUE, 'componentx')
    , ('safe', 'Safe', NULL, NULL, NULL, NULL, NULL, TRUE, 'safe')
    , ('ribbon', 'Ribbon', NULL, NULL, NULL, NULL, NULL, TRUE, 'ribbonfinance')
    , ('oneinch', '1inch Protocol', NULL, NULL, NULL, NULL, NULL, TRUE, '1inch')
    , ('gitcoin', 'Gitcoin', NULL, NULL, NULL, NULL, NULL, TRUE, 'gitcoin')
    , ('paladin', 'Paladin', NULL, NULL, NULL, NULL, NULL, TRUE, 'Paladin_vote')
    , ('pooltogether', 'PoolTogether', NULL, NULL, NULL, NULL, NULL, TRUE, 'PoolTogether_')
    ) AS temp_table (project, name, eligible_addresses, circulating_supply_on_drop, max_supply, airdropped_total, token_issued_by_protocol, x_username)