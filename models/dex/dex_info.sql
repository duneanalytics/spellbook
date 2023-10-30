{{ config(
        tags = [ 'static'],
        schema='dex',
        alias = 'info',
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base"]\',
                                    "sector",
                                    "dex",
                                    \'["hildobby"]\') }}')
}}

SELECT project, name, marketplace_type, x_username
FROM (VALUES
    	('uniswap', 'Uniswap', 'Direct', 'Uniswap')
    , ('pancakeswap', 'PancakeSwap', 'Direct', 'PancakeSwap')
    , ('curve', 'Curve', 'Direct', 'CurveFinance')
    , ('sushiswap', 'SushiSwap', 'Direct', 'SushiSwap')
    , ('mdex', 'Mdex', 'Direct', 'Mdextech')
    , ('dodo', 'DODO', 'Direct & Aggregator', 'BreederDodo') -- To Fix, should be just dodo
    , ('DODO X', 'DODO', 'Direct & Aggregator', 'BreederDodo')
    , ('balancer', 'Balancer', 'Direct & FLashloans', 'Balancer')
    , ('trader_joe', 'Trader Joe', 'Direct', 'traderjoe_xyz')
    , ('spookyswap', 'SpookySwap', 'Direct', 'SpookySwap')
    , ('spookswap', 'SpookySwap', 'Direct', 'SpookySwap')  -- To remove, should be just spookyswap
    , ('biswap', 'Biswap', 'Direct', 'Biswap_Dex')
    , ('bancor_network', 'Bancor', 'Direct', 'Bancor') -- To Fix, should be just bancor
    , ('bancor', 'Bancor', 'Direct', 'Bancor')
    , ('ellipsis_finance', 'Ellipsis', 'Direct', 'Ellipsisfi')
    , ('babyswap', 'BabySwap', 'Direct', 'babyswap_bsc')
    , ('apeswap', 'ApeSwap', 'Direct', 'ape_swap')
    , ('shibaswap', 'ShibaSwap', 'Direct', 'ShibaSwapDEX')
    , ('spiritswap', 'SpiritSwap', 'Direct', 'Spirit_Swap')
    , ('kyberswap', 'KyberSwap', 'Direct', 'KyberNetwork')
    , ('beethoven_x', 'Beethoven X', 'Direct', 'beethoven_x')
    , ('platypus_finance', 'Platypus', 'Direct', 'Platypusdefi')
    , ('hashflow', 'hashflow', 'Direct', 'hashflow')
    , ('velodrome', 'Velodrome', 'Direct', 'VelodromeFi')
    , ('aerodrome', 'Aerodrome', 'Direct', 'aerodromefi')
    , ('quickswap', 'QuickSwap', 'Direct', 'QuickswapDEX')
    , ('mstable', 'mStable', 'Direct', 'mstable_')
    , ('airswap', 'AirSwap', 'Direct', 'airswap')
    , ('wombat', 'Wombat Exchange', 'Direct', 'WombatExchange')
    , ('defiswap', 'DeFi Swap', 'Direct', 'defi_swap')
    , ('gmx', 'GMX', 'Direct', 'GMX_IO')
    , ('clipper', 'Clipper', 'Direct', 'Clipper_DEX')
    , ('woofi', 'WOOFi', 'Direct', '_WOOFi')
    , ('maverick', 'Maverick Protocol', 'Direct', 'mavprotocol')
    , ('thena', 'THENA', 'Direct', 'ThenaFi_')
    , ('synthetix', 'Synthetix', 'Direct', 'synthetix_io')
    , ('fraxswap', 'Frax Finance', 'Direct', 'fraxfinance')
    , ('dfx', 'DFX Finance', 'Direct', 'DFXFinance')
    , ('zigzag', 'ZigZag Exchange', 'Direct', 'ZigZagExchange')
    , ('integral', 'Integral', 'Direct', 'IntegralHQ')
    , ('wigoswap', 'Wigo Ecosystem', 'Direct', 'WigoSwap')
    , ('equalizer_exchange', 'Equalizer', 'Direct', 'Equalizer0x')
    , ('zipswap', 'ZipSwap', 'Direct', 'Zip_swap')
    , ('iziswap', 'iZUMi Finance', 'Direct', 'izumi_Finance')
    , ('spartacus_exchange', 'Spartacus', 'Direct', 'Spartacus_Fi')
    , ('swapr', 'swapr', 'Direct', 'swapr_finance')
    , ('rubicon', 'Rubicon', 'Direct', 'rubicondefi')
    , ('arbswap', 'Arbswap', 'Direct', 'ArbswapOfficial')
    , ('verse_dex', 'Verse', 'Direct', 'VerseEcosystem')
    , ('honeyswap', 'Honeyswap', 'Direct', 'Honeyswap')
    , ('glacier', 'Glacier Finance', 'Direct', 'Glacier_Fi')
    , ('nomiswap', 'Nomiswap DEX', 'Direct', 'Nomiswap')
    , ('onepunchswap', 'OnePunchSwap', 'Direct', 'OnePunchSwap')
    , ('camelot', 'Camelot', 'Direct', 'CamelotDEX')
    , ('saddle', 'Saddle', 'Direct', 'saddlefinance')
    , ('oneinch', '1inch', 'Aggregator', '1inch')
    , ('1inch', '1inch', 'Aggregator', '1inch') -- To Fix, should be just oneinch
    , ('1inch Limit Order Protocol', '1inch Limit Order Protocol', 'Aggregator', '1inch') -- To Fix, should be just oneinch or maybe oneinch_limit_order_protocol
    , ('zeroex', '0x', 'Aggregator', '0xProject')
    , ('0x API', '0x API', 'Aggregator', '0xProject') -- To Fix, should be just zeroex
    , ('paraswap', 'ParaSwap', 'Aggregator', 'paraswap')
    , ('cow_protocol', 'CoW Swap', 'Aggregator', 'CoWSwap')
    , ('CoW Protocol', 'CoW Swap', 'Aggregator', 'CoWSwap') -- To Fix, should be just cow_protocol
    , ('openocean', 'OpenOcean', 'Aggregator', 'OpenOceanGlobal')
    , ('lifi', 'LI.FI', 'Aggregator', 'lifiprotocol')
    , ('yield_yak', 'Yield Yak', 'Aggregator', 'yieldyak_')
    , ('bebop', 'Bebop', 'Aggregator', 'bebop_dex')
    , ('euler', 'Euler', 'Flashloans', 'eulerfinance')
    , ('synapse', 'Synapse', 'Flashloans', 'SynapseProtocol')
    , ('maker', 'Maker', 'Flashloans', 'MakerDAO')
    , ('fiatdao', 'Fiat DAO', 'Flashloans', 'fiatdao')
    , ('dydx', 'dYdX', 'Flashloans', 'dYdX')
    , ('aave', 'Aave', 'Flashloans', 'AaveAave')
    , ('openxswap', 'OpenXSwap', 'Direct', 'OpenXSwap')
    , ('carbon_defi', 'Carbon DeFi', 'Direct', 'CarbonDeFixyz')
    , ('opx_finance', 'OPX', 'Direct', 'opxfinance')
    , ('ubeswap', 'Ubeswap', 'Direct', 'ubeswap')
    , ('mauve', 'Mauve', 'Direct', 'mauve_org')
    , ('xchange', 'X7 Finance', 'Direct', 'X7_Finance')
    ) AS temp_table (project, name, marketplace_type, x_username)