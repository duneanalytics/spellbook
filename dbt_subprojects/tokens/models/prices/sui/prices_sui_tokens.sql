{% set blockchain = 'sui' %}

{{ config(
   schema = 'prices_' + blockchain,
   alias = 'tokens',
   materialized = 'table',
   file_format = 'delta',
   tags = ['static']
   )
}}

SELECT
   token_id
   , '{{ blockchain }}' as blockchain
   , symbol
   , contract_address
   , decimals
FROM
(
   VALUES
   ('deep-deepbook-protocol', 'DEEP', '0xdeeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270::deep::DEEP', 6)
   , ('usdc-usd-coin', 'USDC', '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC', 6)
   , ('wal-walrus', 'WAL', '0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL', 9)
   , ('btc-bitcoin', 'XBTC', '0x876a4b7bce8aeaef60464c11f4026903e9afacab79b9b142686158aa86560b50::xbtc::XBTC', 8)
   , ('wbtc-wrapped-bitcoin', 'suiWBTC', '0xaafb102dd0902f5055cadecd687fb5b71ca82ef0e0285d90afde828ec58ca96b::btc::BTC', 8)
   , ('ns-suins-token', 'NS', '0x5145494a5f5100e645e4b0aa950fa6b68f614e8c59e17bc5ded3495123a79178::ns::NS', 6)
   , ('usdt-tether', 'wUSDT', '0x375f70cf2ae4c00bf37117d0c85a2c71545e6ee05c4a5c7d282cd66a4504b068::usdt::USDT', 6)
   , ('fdusd-first-digital-usd', 'FDUSD', '0xf16e6b723f242ec745dfd7634ad072c42d5c1d9ac9d62a39c381303eaa57693a::fdusd::FDUSD', 6)
   , ('ausd-agora-dollar', 'AUSD', '0x2053d08c1e2bd02791056171aab0fd12bd7cd7efad2ab8f6b9c8902f14df2ff2::ausd::AUSD', 6)
   , ('buck-bucket-protocol-buck-stablecoin', 'BUCK', '0xce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::buck::BUCK', 9)
   , ('cetus-cetus-protocol', 'CETUS', '0x06864a6f921804860930db6ddbe2e16acdf8504495ea7481637a1c8b9a8fe54b::cetus::CETUS', 9)
   , ('blue-bluefin', 'BLUE', '0xe1b45a0e641b9955a20aa0ad1c1f4ad86aad8afb07296d4085e349a50e90bdca::blue::BLUE', 9)
   , ('ika-ika', 'IKA', '0x7262fb2f7a3a14c888c438a3cd9b912469a58cf60f367352c46584262e8299aa::ika::IKA', 9)
   , ('send-suilend', 'SEND', '0xb45fcfcc2cc07ce0702cc2d229621e046c906ef14d9b25e8e4d25f6e8763fef7::send::SEND', 6)
   , ('lbtc-lombard-staked-btc', 'LBTC', '0x3e8e9423d80e1774a7ca128fccd8bf5f1f7753be658c5e645929037f7c819040::lbtc::LBTC', 8)
   , ('stbtc-lorenzo-stbtc', 'stBTC', '0x5f496ed5d9d045c5b788dc1bb85f54100f2ede11e46f6a232c29daada4c5bdb6::coin::COIN', 8)
   , ('tbtc-tbtc', 'TBTC', '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC', 8)
   , ('afsui-aftermath-staked-sui', 'afSUI', '0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI', 9)
) as temp (token_id, symbol, contract_address, decimals)

