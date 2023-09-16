{{config(
        tags = ['static', 'dunesql'],
        alias = alias('addresses'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby", "soispoke", "web3_data"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    ('ethereum', 0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be, 'Binance', 'Binance 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd551234ae421e3bcba99a0da6d736074f22192ff, 'Binance', 'Binance 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x564286362092d8e7936f0549571a803b203aaced, 'Binance', 'Binance 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0681d8db095565fe8a346fa0277bffde9c0edbbf, 'Binance', 'Binance 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfe9e8709d3215310075d67e3ed32a380ccf451c8, 'Binance', 'Binance 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67, 'Binance', 'Binance 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xbe0eb53f46cd790cd13851d5eff43d12404d33e8, 'Binance', 'Binance 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf977814e90da44bfa03b6295a0616a897441acec, 'Binance', 'Binance 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x001866ae5b3de6caa5a51543fd9fb64f524f5478, 'Binance', 'Binance 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x85b931a32a0725be14285b66f1a22178c672d69b, 'Binance', 'Binance 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x708396f17127c42383e3b9014072679b2f60b82f, 'Binance', 'Binance 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe0f0cfde7ee664943906f17f7f14342e76a5cec7, 'Binance', 'Binance 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8f22f2063d253846b53609231ed80fa571bc0c8f, 'Binance', 'Binance 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x28c6c06298d514db089934071355e5743bf21d60, 'Binance', 'Binance 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x21a31ee1afc51d94c2efccaa2092ad1028285549, 'Binance', 'Binance 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdfd5293d8e347dfe59e90efd55b2956a1343963d, 'Binance', 'Binance 16', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x56eddb7aa87536c09ccc2793473599fd21a8b17f, 'Binance', 'Binance 17', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9696f59e4d72e237be84ffd425dcad154bf96976, 'Binance', 'Binance 18', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4d9ff50ef4da947364bb9650892b2554e7be5e2b, 'Binance', 'Binance 19', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4976a4a02f38326660d17bf34b431dc6e2eb2327, 'Binance', 'Binance 20', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd88b55467f58af508dbfdc597e8ebd2ad2de49b3, 'Binance', 'Binance 21', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7dfe9a368b6cf0c0309b763bb8d16da326e8f46e, 'Binance', 'Binance 22', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x345d8e3a1f62ee6b1d483890976fd66168e390f2, 'Binance', 'Binance 23', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc3c8e0a39769e2308869f7461364ca48155d1d9e, 'Binance', 'Binance 24', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2e581a5ae722207aa59acd3939771e7c7052dd3d, 'Binance', 'Binance 25', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x44592b81c05b4c35efb8424eb9d62538b949ebbf, 'Binance', 'Binance 26', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa344c7ada83113b3b56941f6e85bf2eb425949f3, 'Binance', 'Binance 27', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x5a52e96bacdabb82fd05763e25335261b270efcb, 'Binance', 'Binance 28', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x06a0048079ec6571cd1b537418869cde6191d42d, 'Binance', 'Binance 29', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x892e9e24aea3f27f4c6e9360e312cce93cc98ebe, 'Binance', 'Binance 30', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x00799bbc833d5b168f0410312d2a8fd9e0e3079c, 'Binance', 'Binance 31', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x141fef8cd8397a390afe94846c8bd6f4ab981c48, 'Binance', 'Binance 32', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x50d669f43b484166680ecc3670e4766cdb0945ce, 'Binance', 'Binance 33', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2f7e209e0f5f645c7612d7610193fe268f118b28, 'Binance', 'Binance 34', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd9d93951896b4ef97d251334ef2a0e39f6f6d7d7, 'Binance', 'Binance 35', 'hildobby', date '2022-08-28')
    --, ('ethereum', 0xf35a6bd6e0459a4b53a27862c51a2a7292b383d1, 'Binance', 'Binance 36', 'soispoke', date '2022-11-14')
    , ('ethereum', 0x19184ab45c40c2920b0e0e31413b9434abd243ed, 'Binance', 'Binance 39', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x294b9b133ca7bc8ed2cdd03ba661a4c6d3a834d9, 'Binance', 'Binance 41', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5d7f34372fa8708e09689d400a613eee67f75543, 'Binance', 'Binance 42', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8b99f3660622e21f2910ecca7fbe51d654a1517d, 'Binance', 'Binance Charity', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4, 'Binance', 'Binance JEX', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc365c3315cf926351ccaf13fa7d19c8c4058c8e1, 'Binance', 'Binance Pool', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4fabb145d64652a948d72533023f6e7a623c7c53, 'Binance', 'Binance USD', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2f47a1c2db4a3b78cda44eade915c3b19107ddcc, 'Binance', 'Binance Eth2 Depositor', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xbdd75a97c29294ff805fb2fee65abd99492b32a8, 'Binance', 'Binance Eth2 Depositor 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xb3f923eabaf178fc1bd8e13902fc5c61d3ddef5b, 'Binance', 'Wintermute Binance Deposit', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x47ac0fb4F2d84898e4d9e7b4dab3c24507a6d503, 'Binance', 'Binance: Stablecoin Proof of Assets', 'soispoke', date '2022-11-14')
    , ('ethereum', 0x9be89d2a4cd102d8fecc6bf9da793be995c22541, 'Binance', 'Binance: ETH and ERC20 tokens Proof of Assets', 'soispoke', date '2022-11-14')
    , ('ethereum', 0x7884f51dc1410387371ce61747cb6264e1daee0b, 'Binance', 'Binance: DOT Proof of Assets', 'soispoke', date '2022-11-14')
    , ('ethereum', 0xff0a024b66739357c4ed231fb3dbc0c8c22749f5, 'Binance', 'Binance: WRX Proof of Assets', 'soispoke', date '2022-11-14')
    , ('ethereum', 0x61189da79177950a7272c88c6058b96d4bcd6be2, 'Binance US', 'Binance US 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x34ea4138580435b5a521e460035edb19df1938c1, 'Binance US', 'Binance US 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xf60c2ea62edbfe808163751dd0d8693dcb30019c, 'Binance US', 'Binance US 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x9430801ebaf509ad49202aabc5f5bc6fd8a3daf8, 'Binance', 'Binance Deposit Funder', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x8f80c66c70cbc52009babb04c1cadf9b40109289, 'Binance', 'Binance Deposit Funder 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x15ece0d7de25436bcfcf3d62a9085ddc7838aee9, 'Binance', 'Binance Deposit Funder 3', 'hildobby', date '2023-08-31')
    -- Bybit, Source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('ethereum', 0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4, 'Bybit', 'Bybit 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xa7a93fd0a276fc1c0197a5b5623ed117786eed06, 'Bybit', 'Bybit 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe1ab8c08294f8ee707d4efa458eab8bbeeb09215, 'Bybit', 'Bybit 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xee5b5b923ffce93a870b3104b7ca09c3db80047a, 'Bybit', 'Bybit 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xf89d7b9c864f589bbf53a82105107622b35eaa40, 'Bybit', 'Bybit 5', 'hildobby', date '2023-04-06')
    -- Derebit, Source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/deribit_address.txt
    , ('ethereum', 0x77021d475e36b3ab1921a0e3a8380f069d3263de, 'Derebit', 'Derebit 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x5f397b62502e255f68382791947d54c4b2d37f09, 'Derebit', 'Derebit 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xcfee6efec3471874022e205f4894733c42cbbf64, 'Derebit', 'Derebit 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x2eed6a08fb89a5cd111efa33f8dca46cfdbe370f, 'Derebit', 'Derebit 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x6b378be3c9642ccf25b1a27facb8ace24ac34a12, 'Derebit', 'Derebit 5', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xa7e15ef7c01b58ebe5ef74aa73625ae4b11fe754, 'Derebit', 'Derebit 6', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x062448f804191128d71fc72e10a1d13bd7308e7e, 'Derebit', 'Derebit 7', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xa0f6121319a34f24653fb82addc8dd268af5b9e1, 'Derebit', 'Derebit 8', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x904cc2b2694ffa78f04708d6f7de205108213126, 'Derebit', 'Derebit 9', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x63f41034871535cee49996cc47719891fe03dff9, 'Derebit', 'Derebit 10', 'hildobby', date '2023-04-06')
    -- FTX, Source: https://etherscan.io/accounts/label/ftx
    , ('ethereum', 0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2, 'FTX', 'FTX 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc098b2a3aa256d2140208c3de6543aaef5cd3a94, 'FTX', 'FTX 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x25eaff5b179f209cf186b1cdcbfa463a69df4c45, 'FTX', 'FTX (formerly Blockfolio)', 'hildobby', date '2022-08-28')
    -- FTX US, Source: https://etherscan.io/accounts/label/ftx
    , ('ethereum', 0x7abe0ce388281d2acf297cb089caef3819b13448, 'FTX US', 'FTX US', 'hildobby', date '2022-08-28')
    -- Coinbase, Source: https://etherscan.io/accounts/label/coinbase
    , ('ethereum', 0x71660c4005ba85c37ccec55d0c4493e66fe775d3, 'Coinbase', 'Coinbase 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x503828976d22510aad0201ac7ec88293211d23da, 'Coinbase', 'Coinbase 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740, 'Coinbase', 'Coinbase 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3cd751e6b0078be393132286c442345e5dc49699, 'Coinbase', 'Coinbase 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511, 'Coinbase', 'Coinbase 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xeb2629a2734e272bcc07bda959863f316f4bd4cf, 'Coinbase', 'Coinbase 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd688aea8f7d450909ade10c47faa95707b0682d9, 'Coinbase', 'Coinbase 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x02466e547bfdab679fc49e96bbfc62b9747d997c, 'Coinbase', 'Coinbase 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6b76f8b1e9e59913bfe758821887311ba1805cab, 'Coinbase', 'Coinbase 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43, 'Coinbase', 'Coinbase 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x77696bb39917c91a0c3908d577d5e322095425ca, 'Coinbase', 'Coinbase 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7c195d981abfdc3ddecd2ca0fed0958430488e34, 'Coinbase', 'Coinbase 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x95a9bd206ae52c4ba8eecfc93d18eacdd41c88cc, 'Coinbase', 'Coinbase 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb739d0895772dbb71a89a3754a160269068f0d45, 'Coinbase', 'Coinbase 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa090e606e30bd747d4e6245a1517ebe430f0057e, 'Coinbase', 'Coinbase Miscellaneous', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf6874c88757721a02f47592140905c4336dfbc61, 'Coinbase', 'Coinbase Commerce', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x881d4032abe4188e2237efcd27ab435e81fc6bb1, 'Coinbase', 'Coinbase Commerce 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6c8dd0e9cc58c07429e065178d88444b60e60b80, 'Coinbase', 'Coinbase Commerce Fee', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xbc8ec259e3026ae0d87bc442d034d6882ce4a35c, 'Coinbase', 'Coinbase Commerce Fee 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x02d24cab4f2c3bf6e6eb07ea07e45f96baccffe7, 'Coinbase', 'Coinbase Commerce Fee 3', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xce352e98934499be70f641353f16a47d9e1e3abd, 'Coinbase', 'Coinbase Commerce Fee 4', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x90e18a6920985dbacc3d76cf27a3f2131923c720, 'Coinbase', 'Coinbase Commerce Fee 5', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x4b23d52eff7c67f5992c2ab6d3f69b13a6a33561, 'Coinbase', 'Coinbase Commerce Fee 6', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xbe3c68821d585cf1552214897a1c091014b1eb0a, 'Coinbase', 'Coinbase Account Blacklister', 'hildobby', date '2023-08-31')
    -- OKX, Source: https://etherscan.io/accounts/label/okx
    , ('ethereum', 0x6cc5f688a315f3dc28a7781717a9a798a59fda7b, 'OKX', 'OKX', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x236f9f97e0e62388479bf9e5ba4889e46b0273c3, 'OKX', 'OKX 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa7efae728d2936e78bda97dc267687568dd593f3, 'OKX', 'OKX 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2c8fbb630289363ac80705a1a61273f76fd5a161, 'OKX', 'OKX 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x59fae149a8f8ec74d5bc038f8b76d25b136b9573, 'OKX', 'OKX 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x98ec059dc3adfbdd63429454aeb0c990fba4a128, 'OKX', 'OKX 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5041ed759dd4afc3a72b8192c143f72f4724081a, 'OKX', 'OKX 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xcba38020cd7b6f51df6afaf507685add148f6ab6, 'OKX', 'OKX 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x461249076b88189f8ac9418de28b365859e46bfd, 'OKX', 'OKX 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc5451b523d5fffe1351337a221688a62806ad91a, 'OKX', 'OKX 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x42436286a9c8d63aafc2eebbca193064d68068f2, 'OKX', 'OKX 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x69a722f0b5da3af02b4a205d6f0c285f4ed8f396, 'OKX', 'OKX 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc708a1c712ba26dc618f972ad7a187f76c8596fd, 'OKX', 'OKX 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6fb624b48d9299674022a23d92515e76ba880113, 'OKX', 'OKX 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf59869753f41db720127ceb8dbb8afaf89030de4, 'OKX', 'OKX 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x65a0947ba5175359bb457d3b34491edf4cbf7997, 'OKX', 'OKX 16', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4d19c0a5357bc48be0017095d3c871d9afc3f21d, 'OKX', 'OKX 17', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5c52cc7c96bde8594e5b77d5b76d042cb5fae5f2, 'OKX', 'OKX 18', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe9172daf64b05b26eb18f07ac8d6d723acb48f99, 'OKX', 'OKX 19', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7eb6c83ab7d8d9b8618c0ed973cbef71d1921ef2, 'OKX', 'OKX 20', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3b3ae790df4f312e745d270119c6052904fb6790, 'OKX', 'OKX DEX Aggregation Router', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xc5a93444cc4da6efb9e6fc6e5d3cb55a53b52396, 'OKX', 'OKX Deposit Funder', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x89fd00b8d2dcee0f40d8699970115bb861241a54, 'OKX', 'OKX Deposit Funder 3', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xaad8ad7dfa05bc354e011890dd61636842c2cb96, 'OKX', 'OKX Deposit Funder 4', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x24c654f6b143dc5cae3c02fbb527ca63aa555dbc, 'OKX', 'OKX Deposit Supplier', 'hildobby', date '2023-08-31')
    -- Huobi, Source: https://etherscan.io/accounts/label/huobi
    , ('ethereum', 0xab5c66752a9e8167967685f1450532fb96d5d24f, 'Huobi', 'Huobi 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b, 'Huobi', 'Huobi 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfdb16996831753d5331ff813c29a93c76834a0ad, 'Huobi', 'Huobi 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xeee28d484628d41a82d01e21d12e2e78d69920da, 'Huobi', 'Huobi 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5c985e89dde482efe97ea9f1950ad149eb73829b, 'Huobi', 'Huobi 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdc76cd25977e0a5ae17155770273ad58648900d3, 'Huobi', 'Huobi 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xadb2b42f6bd96f5c65920b9ac88619dce4166f94, 'Huobi', 'Huobi 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa8660c8ffd6d578f657b72c0c811284aef0b735e, 'Huobi', 'Huobi 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1062a747393198f70f71ec65a582423dba7e5ab3, 'Huobi', 'Huobi 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe93381fb4c4f14bda253907b18fad305d799241a, 'Huobi', 'Huobi 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfa4b5be3f2f84f56703c42eb22142744e95a2c58, 'Huobi', 'Huobi 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x46705dfff24256421a05d056c29e81bdc09723b8, 'Huobi', 'Huobi 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x32598293906b5b17c27d657db3ad2c9b3f3e4265, 'Huobi', 'Huobi 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5861b8446a2f6e19a067874c133f04c578928727, 'Huobi', 'Huobi 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x926fc576b7facf6ae2d08ee2d4734c134a743988, 'Huobi', 'Huobi 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xeec606a66edb6f497662ea31b5eb1610da87ab5f, 'Huobi', 'Huobi 16', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7ef35bb398e0416b81b019fea395219b65c52164, 'Huobi', 'Huobi 17', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x229b5c097f9b35009ca1321ad2034d4b3d5070f6, 'Huobi', 'Huobi 18', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd8a83b72377476d0a66683cde20a8aad0b628713, 'Huobi', 'Huobi 19', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325, 'Huobi', 'Huobi 20', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x30741289523c2e4d2a62c7d6722686d14e723851, 'Huobi', 'Huobi 21', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380, 'Huobi', 'Huobi 22', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94, 'Huobi', 'Huobi 23', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5, 'Huobi', 'Huobi 24', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e, 'Huobi', 'Huobi 25', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x034f854b44d28e26386c1bc37ff9b20c6380b00d, 'Huobi', 'Huobi 26', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404, 'Huobi', 'Huobi 27', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0c6c34cdd915845376fb5407e0895196c9dd4eec, 'Huobi', 'Huobi 28', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x794d28ac31bcb136294761a556b68d2634094153, 'Huobi', 'Huobi 29', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x34189c75cbb13bdb4f5953cda6c3045cfca84a9e, 'Huobi', 'Huobi 30', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9, 'Huobi', 'Huobi 31', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4d77a1144dc74f26838b69391a6d3b1e403d0990, 'Huobi', 'Huobi 32', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x28ffe35688ffffd0659aee2e34778b0ae4e193ad, 'Huobi', 'Huobi 33', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xcac725bef4f114f728cbcfd744a731c2a463c3fc, 'Huobi', 'Huobi 34', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x73f8fc2e74302eb2efda125a326655acf0dc2d1b, 'Huobi', 'Huobi 35', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0a98fb70939162725ae66e626fe4b52cff62c2e5, 'Huobi', 'Huobi 36', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf66852bc122fd40bfecc63cd48217e88bda12109, 'Huobi', 'Huobi 37', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x49517ca7b7a50f592886d4c74175f4c07d460a70, 'Huobi', 'Huobi 38', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x58c2cb4a6bee98c309215d0d2a38d7f8aa71211c, 'Huobi', 'Huobi 39', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x39d9f4640b98189540a9c0edcfa95c5e657706aa, 'Huobi', 'Huobi 40', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1d1e10e8c66b67692f4c002c0cb334de5d485e41, 'Huobi', 'Huobi Old Address 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1b93129f05cc2e840135aab154223c75097b69bf, 'Huobi', 'Huobi Old Address 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xeb6d43fe241fb2320b5a3c9be9cdfd4dd8226451, 'Huobi', 'Huobi Old Address 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x956e0dbecc0e873d34a5e39b25f364b2ca036730, 'Huobi', 'Huobi Old Address 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6f50c6bff08ec925232937b204b0ae23c488402a, 'Huobi', 'Huobi Old Address 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdf95de30cdff4381b69f9e4fa8dddce31a0128df, 'Huobi', 'Huobi Old Address 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x25c6459e5c5b01694f6453e8961420ccd1edf3b1, 'Huobi', 'Huobi Old Address 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x04645af26b54bd85dc02ac65054e87362a72cb22, 'Huobi', 'Huobi Old Address 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb2a48f542dc56b89b24c04076cbe565b3dc58e7b, 'Huobi', 'Huobi Old Address 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xea0cfef143182d7b9208fbfeda9d172c2aced972, 'Huobi', 'Huobi Old Address 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0c92efa186074ba716d0e2156a6ffabd579f8035, 'Huobi', 'Huobi Old Address 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x91dfa9d9e062a50d2f351bfba0d35a9604993dac, 'Huobi', 'Huobi Old Address 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8e8bc99b79488c276d6f3ca11901e9abd77efea4, 'Huobi', 'Huobi Old Address 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb9a4873d8d2c22e56b8574e8605644d08e047549, 'Huobi', 'Huobi Old Address 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x170af0a02339743687afd3dc8d48cffd1f660728, 'Huobi', 'Huobi Old Address 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf775a9a0ad44807bc15936df0ee68902af1a0eee, 'Huobi', 'Huobi Old Address 16', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x75a83599de596cbc91a1821ffa618c40e22ac8ca, 'Huobi', 'Huobi Old Address 17', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x48ab9f29795dfb44b36587c50da4b30c0e84d3ed, 'Huobi', 'Huobi Old Address 18', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x90f49e24a9554126f591d28174e157ca267194ba, 'Huobi', 'Huobi Old Address 19', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe3314bbf3334228b257779e28228cfb86fa4261b, 'Huobi', 'Huobi Old Address 20', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6edb9d6547befc3397801c94bb8c97d2e8087e2f, 'Huobi', 'Huobi Old Address 21', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8aabba0077f1565df73e9d15dd3784a2b0033dad, 'Huobi', 'Huobi Old Address 22', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd3a2f775e973c1671f2047e620448b8662dcd3ca, 'Huobi', 'Huobi Old Address 23', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1c515eaa87568c850043a89c2d2c2e8187adb056, 'Huobi', 'Huobi Old Address 24', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x60b45f993223dcb8bdf05e3391f7630e5a51d787, 'Huobi', 'Huobi Old Address 25', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa23d7dd4b8a1060344caf18a29b42350852af481, 'Huobi', 'Huobi Old Address 26', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9eebb2815dba2166d8287afa9a2c89336ba9deaa, 'Huobi', 'Huobi Old Address 27', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd10e08325c0e95d59c607a693483680fe5b755b3, 'Huobi', 'Huobi Old Address 28', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc837f51a0efa33f8eca03570e3d01a4b2cf97ffd, 'Huobi', 'Huobi Old Address 29', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf7a8af16acb302351d7ea26ffc380575b741724c, 'Huobi', 'Huobi Old Address 30', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x636b76ae213358b9867591299e5c62b8d014e372, 'Huobi', 'Huobi Old Address 31', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9a755332d874c893111207b0b220ce2615cd036f, 'Huobi', 'Huobi Old Address 32', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xecd8b3877d8e7cd0739de18a5b545bc0b3538566, 'Huobi', 'Huobi Old Address 33', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xef54f559b5e3b55b783c7bc59850f83514b6149c, 'Huobi', 'Huobi Old Address 34', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9d6d492bd500da5b33cf95a5d610a73360fcaaa0, 'Huobi', 'Huobi Mining Pool', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfd54078badd5653571726c3370afb127351a6f26, 'Huobi', 'Huobi Deposit Funder', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x18916e1a2933cb349145a280473a5de8eb6630cb, 'Huobi', 'Huobi Deposit Funder 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xdb0e89a9b003a28a4055ef772e345e8089987bfd, 'Huobi', 'Huobi Deposit Funder 3', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xf0458aaaf6d49192d3b4711960635d5fa2114e71, 'Huobi', 'Huobi Deposit Funder 4', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x07ef60deca209ea0f3f3f08c1ad21a6db5ef9d33, 'Huobi', 'Huobi Deposit Funder 5', 'hildobby', date '2023-08-31')
    -- https://twitter.com/ArkhamIntel/status/1696931898117800322?s=20
    , ('ethereum', 0x40b38765696e3d5d8d9d834d8aad4bb6e418e489, 'Robinhood', 'Robinhood', 'hildobby', date '2023-08-31')
    -- Gate.io, Source: https://etherscan.io/accounts/label/gate-io
    , ('ethereum', 0x0d0707963952f2fba59dd06f2b425ace40b492fe, 'Gate.io', 'Gate.io 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7793cd85c11a924478d358d49b05b37e91b5810f, 'Gate.io', 'Gate.io 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c, 'Gate.io', 'Gate.io 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x234ee9e35f8e9749a002fc42970d570db716453b, 'Gate.io', 'Gate.io 4', 'web3_data', date '2023-02-07')
    , ('ethereum', 0xc882b111a75c0c657fc507c04fbfcd2cc984f071, 'Gate.io', 'Gate.io 5', 'web3_data', date '2023-02-07')
    , ('ethereum', 0x6596da8b65995d5feacff8c2936f0b7a2051b0d0, 'Gate.io', 'Gate.io: Deposit Funder', 'web3_data', date '2023-02-07')
    , ('ethereum', 0xd793281182a0e3e023116004778f45c29fc14f19, 'Gate.io', 'Gate.io: Contract', 'web3_data', date '2023-02-07')
    -- Kraken, Source: https://etherscan.io/accounts/label/kraken
    , ('ethereum', 0x2910543af39aba0cd09dbb2d50200b3e800a63d2, 'Kraken', 'Kraken 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13, 'Kraken', 'Kraken 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe853c56864a2ebe4576a807d26fdc4a0ada51919, 'Kraken', 'Kraken 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0, 'Kraken', 'Kraken 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfa52274dd61e1643d2205169732f29114bc240b3, 'Kraken', 'Kraken 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x53d284357ec70ce289d6d64134dfac8e511c8a3d, 'Kraken', 'Kraken 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40, 'Kraken', 'Kraken 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc6bed363b30df7f35b601a5547fe56cd31ec63da, 'Kraken', 'Kraken 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x29728d0efd284d85187362faa2d4d76c2cfc2612, 'Kraken', 'Kraken 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xae2d4617c862309a3d75a0ffb358c7a5009c673f, 'Kraken', 'Kraken 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x43984d578803891dfa9706bdeee6078d80cfc79e, 'Kraken', 'Kraken 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x66c57bf505a85a74609d2c83e94aabb26d691e1f, 'Kraken', 'Kraken 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xda9dfa130df4de4673b89022ee50ff26f6ea73cf, 'Kraken', 'Kraken 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa83b11093c858c86321fbc4c20fe82cdbd58e09e, 'Kraken', 'Kraken 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe9f7ecae3a53d2a67105292894676b00d1fab785, 'Kraken', 'Kraken Hot Wallet', 'hildobby', date '2022-08-28')
    -- Bitfinex, Source: https://etherscan.io/accounts/label/bitfinex
    , ('ethereum', 0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec, 'Bitfinex', 'Bitfinex: Hot Wallet', 'soispoke', date '2022-10-19')
    , ('ethereum', 0x1151314c646ce4e0efd76d1af4760ae66a9fe30f, 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x742d35cc6634c0532925a3b844bc454e4438f44e, 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x876eabf441b2ee5b5b0554fd502a8e0600950cfa, 'Bitfinex', 'Bitfinex 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdcd0272462140d0a3ced6c4bf970c7641f08cd2c, 'Bitfinex', 'Bitfinex 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4fdd5eb2fb260149a3903859043e962ab89d8ed4, 'Bitfinex', 'Bitfinex 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1b29dd8ff0eb3240238bf97cafd6edea05d5ba82, 'Bitfinex', 'Bitfinex 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x30a2ebf10f34c6c4874b0bdd5740690fd2f3b70c, 'Bitfinex', 'Bitfinex 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3f7e77b627676763997344a1ad71acb765fc8ac5, 'Bitfinex', 'Bitfinex 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x59448fe20378357f206880c58068f095ae63d5a5, 'Bitfinex', 'Bitfinex 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x36a85757645e8e8aec062a1dee289c7d615901ca, 'Bitfinex', 'Bitfinex 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc56fefd1028b0534bfadcdb580d3519b5586246e, 'Bitfinex', 'Bitfinex 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0b73f67a49273fc4b9a65dbd25d7d0918e734e63, 'Bitfinex', 'Bitfinex 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x482f02e8bc15b5eabc52c6497b425b3ca3c821e8, 'Bitfinex', 'Bitfinex 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1b8766d041567eed306940c587e21c06ab968663, 'Bitfinex', 'Bitfinex 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5a710a3cdf2af218740384c52a10852d8870626a, 'Bitfinex', 'Bitfinex 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x28140cb1ac771d4add91ee23788e50249c10263d, 'Bitfinex', 'Bitfinex 16', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x53b36141490c419fa27ecabfeb8be1ecadc82431, 'Bitfinex', 'Bitfinex 17', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0cd76cd43992c665fdc2d8ac91b935ca3165e782, 'Bitfinex', 'Bitfinex 18', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe92d1a43df510f82c66382592a047d288f85226f, 'Bitfinex', 'Bitfinex 19', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8103683202aa8da10536036edef04cdd865c225e, 'Bitfinex', 'Bitfinex 20', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e, 'Bitfinex', 'Bitfinex MultiSig 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828, 'Bitfinex', 'Bitfinex MultiSig 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc61b9bb3a7a0767e3179713f3a5c7a9aedce193c, 'Bitfinex', 'Bitfinex MultiSig 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xcafb10ee663f465f9d10588ac44ed20ed608c11e, 'Bitfinex', 'Bitfinex Old Address 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7180eb39a6264938fdb3effd7341c4727c382153, 'Bitfinex', 'Bitfinex Old Address 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5754284f345afc66a98fbb0a0afe71e0f007b949, 'Bitfinex', 'Tether Treasury', 'hildobby', date '2022-08-28')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('ethereum', 0x0639556f03714a74a5feeaf5736a4a64ff70d206, 'Bitget', 'Bitget 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689, 'Bitget', 'Bitget 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x1ae3739e17d8500f2b2d80086ed092596a116e0b, 'Bitget', 'Bitget 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x2bf7494111a59bd51f731dcd4873d7d71f8feeec, 'Bitget', 'Bitget 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x31a36512d4903635b7dd6828a934c3915a5809be, 'Bitget', 'Bitget 5', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x461f6dcdd5be42d41fe71611154279d87c06b406, 'Bitget', 'Bitget 6', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x5bdf85216ec1e38d6458c870992a69e38e03f7ef, 'Bitget', 'Bitget 7', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x9e00816f61a709fa124d36664cd7b6f14c13ee05, 'Bitget', 'Bitget 8', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xdfe4b89cf009bffa33d9bca1f19694fc2d4d943d, 'Bitget', 'Bitget 9', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe2b406ec9227143a8830229eeb3eb6e24b5c60be, 'Bitget', 'Bitget 10', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe6a421f24d330967a3af2f4cdb5c34067e7e4d75, 'Bitget', 'Bitget 11', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe80623a9d41f2f05780d9cd9cea0f797fd53062a, 'Bitget', 'Bitget 12', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xf646d9b7d20babe204a89235774248ba18086dae, 'Bitget', 'Bitget 13', 'hildobby', date '2023-04-06')
    -- KuCoin, Source: https://etherscan.io/accounts/label/kucoin
    , ('ethereum', 0x2b5634c42055806a59e9107ed44d43c426e58258, 'KuCoin', 'KuCoin 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x689c56aef474df92d44a1b70850f808488f9769c, 'KuCoin', 'KuCoin 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa1d8d972560c2f8144af871db508f0b0b10a3fbf, 'KuCoin', 'KuCoin 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4ad64983349c49defe8d7a4686202d24b25d0ce8, 'KuCoin', 'KuCoin 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1692e170361cefd1eb7240ec13d048fd9af6d667, 'KuCoin', 'KuCoin 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd6216fc19db775df9774a6e33526131da7d19a2c, 'KuCoin', 'KuCoin 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe59cd29be3be4461d79c0881d238cbe87d64595a, 'KuCoin', 'KuCoin 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x899b5d52671830f567bf43a14684eb14e1f945fe, 'KuCoin', 'KuCoin 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91, 'KuCoin', 'KuCoin 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xcad621da75a66c7a8f4ff86d30a2bf981bfc8fdd, 'KuCoin', 'KuCoin 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xec30d02f10353f8efc9601371f56e808751f396f, 'KuCoin', 'KuCoin 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x738cf6903e6c4e699d1c2dd9ab8b67fcdb3121ea, 'KuCoin', 'Kucoin 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd89350284c7732163765b23338f2ff27449e0bf5, 'KuCoin', 'KuCoin 13', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x88bd4d3e2997371bceefe8d9386c6b5b4de60346, 'KuCoin', 'KuCoin 14', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb8e6d31e7b212b2b7250ee9c26c56cebbfbe6b23, 'KuCoin', 'KuCoin 15', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x061f7937b7b2bc7596539959804f86538b6368dc, 'KuCoin', 'KuCoin Deposit Funder', 'hildobby', date '2023-08-31')
    -- Crypto.com, Source: https://etherscan.io/accounts/label/crypto-com
    , ('ethereum', 0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'Crypto.com', 'Crypto.com 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x46340b20830761efd32832a74d7169b29feb9758, 'Crypto.com', 'Crypto.com 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x72a53cdbbcc1b9efa39c834a540550e23463aacb, 'Crypto.com', 'Crypto.com 3', 'soispoke', date '2022-11-14')
    , ('ethereum', 0x7758e507850da48cd47df1fb5f875c23e3340c50, 'Crypto.com', 'Crypto.com 4', 'soispoke', date '2022-11-14')
    , ('ethereum', 0xcffad3200574698b78f32232aa9d63eabd290703, 'Crypto.com', 'Crypto.com 5', 'soispoke', date '2022-11-14')
    -- Gemini, Source: https://etherscan.io/accounts/label/gemini
    , ('ethereum', 0xd24400ae8bfebb18ca49be86258a3c749cf46853, 'Gemini', 'Gemini 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8, 'Gemini', 'Gemini 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea, 'Gemini', 'Gemini 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5f65f7b609678448494de4c87521cdf6cef1e932, 'Gemini', 'Gemini 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb302bfe9c246c6e150af70b1caaa5e3df60dac05, 'Gemini', 'Gemini 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8d6f396d210d385033b348bcae9e4f9ea4e045bd, 'Gemini', 'Gemini 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd69b0089d9ca950640f5dc9931a41a5965f00303, 'Gemini', 'Gemini 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x183b1Ffb0Aa9213b9335AdFAd82E47bfb02f8d24, 'Gemini', 'Gemini 8', 'hildobby', date '2023-09-05')
    , ('ethereum', 0xdd51f01d9fc0fd084c1a4737bbfa5becb6ced9bc, 'Gemini', 'Gemini Deployer', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x4c2f150fc90fed3d8281114c2349f1906cde5346, 'Gemini', 'Gemini Deployer 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x07ee55aa48bb72dcc6e9d78256648910de513eca, 'Gemini', 'Gemini Contract 1', 'hildobby', date '2023-08-31')
    -- BitMart, Source: https://etherscan.io/accounts/label/bitmart
    , ('ethereum', 0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1, 'BitMart', 'BitMart 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7, 'BitMart', 'BitMart 2', 'hildobby', date '2022-08-28')
    -- LATOKEN, Source: https://etherscan.io/accounts/label/latoken
    , ('ethereum', 0x0861fca546225fbf8806986d211c8398f7457734, 'LAToken', 'LAToken 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7891b20c690605f4e370d6944c8a5dbfac5a451c, 'LAToken', 'LAToken 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1b6c1a0e20af81b922cb454c3e52408496ee7201, 'LAToken', 'LAToken 3', 'hildobby', date '2022-08-28')
    -- Upbit, Source: https://etherscan.io/accounts/label/upbit
    , ('ethereum', 0x390de26d772d2e2005c6d1d24afc902bae37a4bb, 'Upbit', 'Upbit 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xba826fec90cefdf6706858e5fbafcb27a290fbe0, 'Upbit', 'Upbit 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5e032243d507c743b061ef021e2ec7fcc6d3ab89, 'Upbit', 'Upbit 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc9cf0ec93d764f5c9571fd12f764bae7fc87c84e, 'Upbit', 'Upbit Cold Wallet', 'hildobby', date '2022-08-28')
    -- Bittrex, Source: https://etherscan.io/accounts/label/bittrex
    , ('ethereum', 0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98, 'Bittrex', 'Bittrex 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3, 'Bittrex', 'Bittrex 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x66f820a414680b5bcda5eeca5dea238543f42054, 'Bittrex', 'Bittrex 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa3c1e324ca1ce40db73ed6026c4a177f099b5770, 'Bittrex', 'Bittrex Controller', 'hildobby', date '2023-08-31')
    -- Bithumb, Source: https://etherscan.io/accounts/label/bithumb
    , ('ethereum', 0x88d34944cf554e9cccf4a24292d891f620e9c94f, 'Bithumb', 'Bithumb 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3052cd6bf951449a984fe4b5a38b46aef9455c8e, 'Bithumb', 'Bithumb 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2140efd7ba31169c69dfff6cdc66c542f0211825, 'Bithumb', 'Bithumb 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa0ff1e0f30b5dda2dc01e7e828290bc72b71e57d, 'Bithumb', 'Bithumb 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc1da8f69e4881efe341600620268934ef01a3e63, 'Bithumb', 'Bithumb 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb4460b75254ce0563bb68ec219208344c7ea838c, 'Bithumb', 'Bithumb 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x15878e87c685f866edfaf454be6dc06fa517b35b, 'Bithumb', 'Bithumb 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x31d03f07178bcd74f9099afebd23b0ae30184ab5, 'Bithumb', 'Bithumb 8', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xed48dc0628789c2956b1e41726d062a86ec45bff, 'Bithumb', 'Bithumb 9', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x186549a4ae594fc1f70ba4cffdac714b405be3f9, 'Bithumb', 'Bithumb 10', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd273bd546b11bd60214a2f9d71f22a088aafe31b, 'Bithumb', 'Bithumb 11', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x558553d54183a8542f7832742e7b4ba9c33aa1e6, 'Bithumb', 'Bithumb 12', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xbb5a0408fa54287b9074a2f47ab54c855e95ef82, 'Bithumb', 'Bithumb Old Address 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5521a68d4f8253fc44bfb1490249369b3e299a4a, 'Bithumb', 'Bithumb Old Address 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8fa8af91c675452200e49b4683a33ca2e1a34e42, 'Bithumb', 'Bithumb Old Address 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3b83cd1a8e516b6eb9f1af992e9354b15a6f9672, 'Bithumb', 'Bithumb Old Address 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x03599a2429871e6be1b154fb9c24691f9d301865, 'Bithumb', 'Bithumb Deposit Funder', 'hildobby', date '2023-08-31')
    -- Bitstamp, Source: https://etherscan.io/accounts/label/bitstamp
    , ('ethereum', 0x00bdb5699745f5b860228c8f939abf1b9ae374ed, 'Bitstamp', 'Bitstamp 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x1522900b6dafac587d499a862861c0869be6e428, 'Bitstamp', 'Bitstamp 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72, 'Bitstamp', 'Bitstamp 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x059799f2261d37b829c2850cee67b5b975432271, 'Bitstamp', 'Bitstamp 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4c766def136f59f6494f0969b1355882080cf8e0, 'Bitstamp', 'Bitstamp 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f, 'Bitstamp', 'Bitstamp 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfca70e67b3f93f679992cd36323eeb5a5370c8e4, 'Bitstamp', 'Bitstamp Old Address 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x964771f6df31eea2d927fa71d7bd78e81bcdce05, 'Bitstamp', 'Bitstamp Contract 1', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x538d72ded42a76a30f730292da939e0577f22f57, 'Bitstamp', 'Bitstamp Deployer', 'hildobby', date '2023-08-31')
    -- BitMEX, Source: https://etherscan.io/accounts/label/bitmex
    , ('ethereum', 0xeea81c4416d71cef071224611359f6f99a4c4294, 'BitMEX', 'BitMEX 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfb8131c260749c7835a08ccbdb64728de432858e, 'BitMEX', 'BitMEX 2', 'hildobby', date '2022-08-28')
    -- HitBTC, Source: https://etherscan.io/accounts/label/hitbtc
    , ('ethereum', 0x9c67e141c0472115aa1b98bd0088418be68fd249, 'HitBTC', 'HitBTC 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x59a5208b32e627891c389ebafc644145224006e8, 'HitBTC', 'HitBTC 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa12431d0b9db640034b0cdfceef9cce161e62be4, 'HitBTC', 'HitBTC 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfdeda15e2922c5ed41fc1fdf36da2fb2623666b3, 'HitBTC', 'HitBTC Deposit Funder', 'hildobby', date '2023-08-31')
    , ('ethereum', 0xad68942a95fdd56594aa5cf862b358790e37834c, 'HitBTC', 'HitBTC Deposit Funder 2', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x0113a6b755fbad36b4249fd63002e2035e401143, 'HitBTC', 'HitBTC Deposit Funder 3', 'hildobby', date '2023-08-31')
    -- Luno
    , ('ethereum', 0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0, 'Luno', 'Luno 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0
    , ('ethereum', 0x416299aade6443e6f6e8ab67126e65a7f606eef5, 'Luno', 'Luno 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x416299aade6443e6f6e8ab67126e65a7f606eef5
    -- Poloniex, Source: https://etherscan.io/accounts/label/poloniex
    , ('ethereum', 0x32be343b94f860124dc4fee278fdcbd38c102d88, 'Poloniex', 'Poloniex', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef, 'Poloniex', 'Poloniex 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb794f5ea0ba39494ce839613fffba74279579268, 'Poloniex', 'Poloniex 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xa910f92acdaf488fa6ef02174fb86208ad7722ba, 'Poloniex', 'Poloniex 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b, 'Poloniex', 'Poloniex BAT', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec, 'Poloniex', 'Poloniex BNT', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1, 'Poloniex', 'Poloniex CVC', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437, 'Poloniex', 'Poloniex FOAM', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df, 'Poloniex', 'Poloniex GNO', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0536806df512d6cdde913cf95c9886f65b1d3462, 'Poloniex', 'Poloniex GNT', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb, 'Poloniex', 'Poloniex KNC', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21, 'Poloniex', 'Poloniex LOOM', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb, 'Poloniex', 'Poloniex MANA', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0, 'Poloniex', 'Poloniex NXC', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x36b01066b7fa4a0fdb2968ea0256c848e9135674, 'Poloniex', 'Poloniex OMG', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5, 'Poloniex', 'Poloniex REP', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6795cf8eb25585eadc356ae32ac6641016c550f2, 'Poloniex', 'Poloniex SNT', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfbf2173154f7625713be22e0504404ebfe021eae, 'Poloniex', 'Poloniex STORJ', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x6f803466bcd17f44fa18975bf7c509ba64bf3825, 'Poloniex', 'Poloniex USDC', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xead6be34ce315940264519f250d8160f369fa5cd, 'Poloniex', 'Poloniex ZRX', 'hildobby', date '2022-08-28')
    -- WhiteBIT, Source: https://etherscan.io/address/0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3
    , ('ethereum', 0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3, 'WhiteBIT', 'WhiteBIT 1', 'hildobby', date '2022-08-28')
    -- DigiFinex, Source: https://etherscan.io/accounts/label/digifinex
    , ('ethereum', 0xe17ee7b3c676701c66b395a35f0df4c2276a344e, 'DigiFinex', 'DigiFinex 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3b73d7e1266e02a68185f5221a6718db04df6301, 'DigiFinex', 'DigiFinex Deployer 1', 'hildobby', date '2023-08-31')
    , ('ethereum', 0x1b930c43526b09191a74175eaa47f2a650aeb73d, 'DigiFinex', 'DigiFinex Deployer 2', 'hildobby', date '2023-08-31')
    -- MEXC
    , ('ethereum', 0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88, 'MEXC', 'MEXC 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88
    , ('ethereum', 0x0211f3cedbef3143223d3acf0e589747933e8527, 'MEXC', 'MEXC 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x0211f3cedbef3143223d3acf0e589747933e8527
    , ('ethereum', 0x3cc936b795a188f0e246cbb2d74c5bd190aecf18, 'MEXC', 'MEXC 3', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x3cc936b795a188f0e246cbb2d74c5bd190aecf18
    -- Yobit
    , ('ethereum', 0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3, 'Yobit', 'Yobit 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3
    -- Paribu, Source: https://etherscan.io/accounts/label/paribu
    , ('ethereum', 0xbd8ef191caa1571e8ad4619ae894e07a75de0c35, 'Paribu', 'Paribu 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2bb97b6cf6ffe53576032c11711d59bd056830ee, 'Paribu', 'Paribu 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfb90501083a3b6af766c8da35d3dde01eb0d2a68, 'Paribu', 'Paribu 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xabc74170f3cb8ab352820c39cc1d1e05ce9e41d3, 'Paribu', 'Paribu 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9acbb72cf67103a30333a32cd203459c6a9c3311, 'Paribu', 'Paribu 5', 'hildobby', date '2022-08-28')
    -- RenrenBit
    , ('ethereum', 0x28c9386ebab8d52ead4a327e6423316435b2d4fc, 'RenrenBit', 'RenrenBit', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x28c9386ebab8d52ead4a327e6423316435b2d4fc
    -- Exmo
    , ('ethereum', 0x1fd6267f0d86f62d88172b998390afee2a1f54b6, 'Exmo', 'Exmo 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x1fd6267f0d86f62d88172b998390afee2a1f54b6
    , ('ethereum', 0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32, 'Exmo', 'Exmo 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32
    -- Remitano, Source: https://etherscan.io/accounts/label/remitano
    , ('ethereum', 0xb8cf411b956b3f9013c1d0ac8c909b086218207c, 'Remitano', 'Remitano 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x2819c144d5946404c0516b6f817a960db37d4929, 'Remitano', 'Remitano 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8365efb25d0822aaf15ee1d314147b6a7831c403, 'Remitano', 'Remitano Deposit Funder', 'hildobby', date '2023-08-31')
    -- WEX Exchange 
    , ('ethereum', 0xb3aaaae47070264f3595c5032ee94b620a583a39, 'WEX Exchange', 'WEX Exchange 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xb3aaaae47070264f3595c5032ee94b620a583a39
    -- Peatio
    , ('ethereum', 0xd4dcd2459bb78d7a645aa7e196857d421b10d93f, 'Peatio', 'Peatio 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xd4dcd2459bb78d7a645aa7e196857d421b10d93f
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('ethereum', 0x274f3c32c90517975e29dfc209a23f315c1e5fc7, 'Hotbit', 'Hotbit 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x8533a0bd9310eb63e7cc8e1116c18a3d67b1976a, 'Hotbit', 'Hotbit 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x562680a4dc50ed2f14d75bf31f494cfe0b8d10a1, 'Hotbit', 'Hotbit 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb18fbfe3d34fdc227eb4508cde437412b6233121, 'Hotbit', 'Hotbit 4', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1, 'Hotbit', 'Hotbit 5', 'hildobby', date '2023-04-07')
    -- CoinEx
    , ('ethereum', 0xb9ee1e551f538a464e8f8c41e9904498505b49b0, 'CoinEx', 'CoinEx 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xb9ee1e551f538a464e8f8c41e9904498505b49b0
    , ('ethereum', 0x33ddd548fe3a082d753e5fe721a26e1ab43e3598, 'CoinEx', 'CoinEx 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x33ddd548fe3a082d753e5fe721a26e1ab43e3598
    , ('ethereum', 0x601a63c50448477310fedb826ed0295499baf623, 'CoinEx', 'CoinEx 3', 'sankinyue', date '2023-09-13') -- https://www.coinex.com/en/reserve-proof
    , ('ethereum', 0xd782e53a49d564f5fce4ba99555dd25d16d02a75, 'CoinEx', 'CoinEx 4', 'sankinyue', date '2023-09-13') -- https://www.coinex.com/en/reserve-proof
    , ('ethereum', 0x90f86774e792e91cf81b2ff9f341efca649343a6, 'CoinEx', 'CoinEx 5', 'sankinyue', date '2023-09-13') -- https://www.coinex.com/en/reserve-proof
    , ('ethereum', 0x53eb3ea47643e87e8f25dd997a37b3b5260e7336, 'CoinEx', 'CoinEx 6', 'sankinyue', date '2023-09-13') -- https://www.coinex.com/en/reserve-proof
    , ('ethereum', 0x5cf44f2cb65af7d56b30719312ecd13151a0470b, 'CoinEx', 'CoinEx Deposit Funder', 'sankinyue', date '2023-09-13') -- https://etherscan.io/address/0x5cf44f2cb65af7d56b30719312ecd13151a0470b
    , ('ethereum', 0x1e450c2a1870a52606edd37ac0bf593dca9c1c3f, 'CoinEx', 'CoinEx Deposit Funder 2', 'sankinyue', date '2023-09-13') -- https://etherscan.io/address/0x1e450c2a1870a52606edd37ac0bf593dca9c1c3f
    , ('ethereum', 0xf54635836862aad6e255e9b4fe49275fa5047e5d, 'CoinEx', 'CoinEx Multisig', 'sankinyue', date '2023-09-13') -- https://etherscan.io/address/0xf54635836862aad6e255e9b4fe49275fa5047e5d
    -- CoinExchange
    , ('ethereum', 0x4b01721f0244e7c5b5f63c20942850e447f5a5ee, 'CoinExchange', 'CoinExchange 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x4b01721f0244e7c5b5f63c20942850e447f5a5ee
    -- AscendEX (formerly BitMax), Source: https://etherscan.io/accounts/label/ascendex
    , ('ethereum', 0x03bdf69b1322d623836afbd27679a1c0afa067e9, 'AscendEX', 'AscendEX 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4b1a99467a284cc690e3237bc69105956816f762, 'AscendEX', 'AscendEX 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd, 'AscendEX', 'AscendEX 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x09344477fdc71748216a7b8bbe7f2013b893def8, 'AscendEX', 'AscendEX Deposit Funder', 'hildobby', date '2023-08-31')
    -- Liquid, Source: https://etherscan.io/accounts/label/liquid
    , ('ethereum', 0xedbb72e6b3cf66a792bff7faac5ea769fe810517, 'Liquid', 'Liquid 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdf4b6fb700c428476bd3c02e6fa83e110741145b, 'Liquid', 'Liquid 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdb2cad4f306b47c9b35541988c7656f1bb092e15, 'Liquid', 'Liquid 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9cc2dce817093ceea82bb67a4cf43131fa354c06, 'Liquid', 'Liquid 4', 'hildobby', date '2022-08-28')
    -- Tidex, Source: https://etherscan.io/accounts/label/tidex
    , ('ethereum', 0x3613ef1125a078ef96ffc898c4ec28d73c5b8c52, 'Tidex', 'Tidex 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0a73573cf2903d2d8305b1ecb9e9730186a312ae, 'Tidex', 'Tidex 2', 'hildobby', date '2022-08-28')
    -- OTCBTC
    , ('ethereum', 0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8, 'OTCBTC', 'OTCBTC 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8
    -- ShapeShift, Source: https://etherscan.io/accounts/label/shapeshift 
    , ('ethereum', 0x120a270bbc009644e35f0bb6ab13f95b8199c4ad, 'ShapeShift', 'ShapeShift 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x9e6316f44baeeee5d41a1070516cc5fa47baf227, 'ShapeShift', 'ShapeShift 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413, 'ShapeShift', 'ShapeShift 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x563b377a956c80d77a7c613a9343699ad6123911, 'ShapeShift', 'ShapeShift 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd3273eba07248020bf98a8b560ec1576a612102f, 'ShapeShift', 'ShapeShift 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x3b0bc51ab9de1e5b7b6e34e5b960285805c41736, 'ShapeShift', 'ShapeShift 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xeed16856d551569d134530ee3967ec79995e2051, 'ShapeShift', 'ShapeShift 7', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb36efd48c9912bd9fd58b67b65f7438f6364a256, 'ShapeShift', 'ShapeShift Binance Deposit', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xda1e5d4cc9873963f788562354b55a772253b92f, 'ShapeShift', 'ShapeShift Bitfinex Deposit', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe9319eba87af7c2fc1f55ccde9d10ea8efbd592d, 'ShapeShift', 'ShapeShift Bittrex Deposit', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xe8ed915e208b28c617d20f3f8ca8e11455933adf, 'ShapeShift', 'ShapeShift Poloniex Deposit', 'hildobby', date '2022-08-28')
    -- TopBTC, Source: https://etherscan.io/accounts/label/topbtc
    , ('ethereum', 0xb2cc3cdd53fc9a1aeaf3a68edeba2736238ddc5d, 'TopBTC', 'TopBTC 1', 'hildobby', date '2022-08-28')
    -- Trade.io, Source: https://etherscan.io/accounts/label/trade-io
    , ('ethereum', 0x1119aaefb02bf12b84d28a5d8ea48ec3c90ef1db, 'Trade.io', 'Trade.io 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x58f75ddacffb183a30f69fe58a67a0d0985fce0f, 'Trade.io', 'Trade.io Wallet 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x5a2fad810f990c4535ada938400b6b67ef7646af, 'Trade.io', 'Trade.io Wallet 2', 'hildobby', date '2022-08-28')
    -- Uex
    , ('ethereum', 0x2f1233ec3a4930fd95874291db7da9e90dfb2f03, 'Uex', 'Uex 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x2f1233ec3a4930fd95874291db7da9e90dfb2f03
    -- Uphold
    , ('ethereum', 0x340d693ed55d7ba167d184ea76ea2fd092a35bdc, 'Uphold', 'Uphold 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x340d693ed55d7ba167d184ea76ea2fd092a35bdc
    -- Kuna.io
    , ('ethereum', 0xea81ce54a0afa10a027f65503bd52fba83d745b8, 'Kuna.io', 'Kuna.io 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xea81ce54a0afa10a027f65503bd52fba83d745b8
    , ('ethereum', 0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d, 'Kuna.io', 'Kuna.io 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d
    -- Bit-Z
    , ('ethereum', 0x4b729cf402cfcffd057e254924b32241aedc1795, 'Bit-Z', 'Bit-Z 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x4b729cf402cfcffd057e254924b32241aedc1795
    -- Bitzlato 
    , ('ethereum', 0x00cdc153aa8894d08207719fe921fff964f28ba3, 'Bitzlato', 'Bitzlato 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x00cdc153aa8894d08207719fe921fff964f28ba3
    -- Cobinhood, Source: https://etherscan.io/accounts/label/cobinhood
    , ('ethereum', 0x8958618332df62af93053cb9c535e26462c959b0, 'Cobinhood', 'Cobinhood 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xb726da4fbdc3e4dbda97bb20998cf899b0e727e0, 'Cobinhood', 'Cobinhood 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0bb9fc3ba7bcf6e5d6f6fc15123ff8d5f96cee00, 'Cobinhood', 'Cobinhood MultiSig', 'hildobby', date '2022-08-28')
    -- Cashierest
    , ('ethereum', 0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3, 'Cashierest', 'Cashierest 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3
    -- Bibox
    , ('ethereum', 0xf73c3c65bde10bf26c2e1763104e609a41702efe, 'Bibox', 'Bibox 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xf73c3c65bde10bf26c2e1763104e609a41702efe
    -- Coinhako, Source: https://etherscan.io/accounts/label/coinhako
    , ('ethereum', 0xd4bddf5e3d0435d7a6214a0b949c7bb58621f37c, 'Coinhako', 'Coinhako 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf2d4766ad705e3a5c9ba5b0436b473085f82f82f, 'Coinhako', 'Coinhako Hot Wallet', 'hildobby', date '2022-08-28')
    -- Bitberry
    , ('ethereum', 0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a, 'Bitberry', 'Bitberry 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a
    -- BigONE
    , ('ethereum', 0xa30d8157911ef23c46c0eb71889efe6a648a41f7, 'BigONE', 'BigONE 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xa30d8157911ef23c46c0eb71889efe6a648a41f7
    -- Allbit, Source: https://etherscan.io/accounts/label/allbit
    , ('ethereum', 0xdc1882f350b42ac9a23508996254b1915c78b204, 'Allbit', 'Allbit 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xff6b1cdfd2d3e37977d7938aa06b6d89d6675e27, 'Allbit', 'Allbit 2', 'hildobby', date '2022-08-28')
    -- COSS, Source: https://etherscan.io/accounts/label/coss-io
    , ('ethereum', 0x0d6b5a54f940bf3d52e438cab785981aaefdf40c, 'COSS', 'COSS 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd1560b3984b7481cd9a8f40435a53c860187174d, 'COSS', 'COSS Old Hot Wallet', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x43f07efe28e092a0fe4ec5b5662022b461ffac80, 'COSS', 'COSS Hot Wallet', 'hildobby', date '2022-08-28')
    -- ABCC, Source: https://etherscan.io/accounts/label/abcc
    , ('ethereum', 0x05f51aab068caa6ab7eeb672f88c180f67f17ec7, 'ABCC', 'ABCC 1', 'hildobby', date '2022-08-28')
    -- ATAIX
    , ('ethereum', 0x4df5f3610e2471095a130d7d934d551f3dde01ed, 'ATAIX', 'ATAIX 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x4df5f3610e2471095a130d7d934d551f3dde01ed
    -- Beaxy
    , ('ethereum', 0xadb72986ead16bdbc99208086bd431c1aa38938e, 'Beaxy', 'Beaxy 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xadb72986ead16bdbc99208086bd431c1aa38938e
    -- Bgogo, Source: https://etherscan.io/accounts/label/bgogo
    , ('ethereum', 0x7a10ec7d68a048bdae36a70e93532d31423170fa, 'Bgogo', 'Bgogo 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xce1bf8e51f8b39e51c6184e059786d1c0eaf360f, 'Bgogo', 'Bgogo 2', 'hildobby', date '2022-08-28')
    -- Bilaxy
    , ('ethereum', 0xf7793d27a1b76cdf14db7c83e82c772cf7c92910, 'Bilaxy', 'Bilaxy 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xf7793d27a1b76cdf14db7c83e82c772cf7c92910
    , ('ethereum', 0xcce8d59affdd93be338fc77fa0a298c2cb65da59, 'Bilaxy', 'Bilaxy 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xcce8d59affdd93be338fc77fa0a298c2cb65da59
    -- Bity
    , ('ethereum', 0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9, 'Bity', 'Bity', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9
    -- BW 
    , ('ethereum', 0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1, 'BW', 'BW Old Address', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1
    , ('ethereum', 0x73957709695e73fd175582105c44743cf0fb6f2f, 'BW', 'BW 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x73957709695e73fd175582105c44743cf0fb6f2f
    -- bitFlyer
    , ('ethereum', 0x111cff45948819988857bbf1966a0399e0d1141e, 'bitFlyer', 'bitFlyer 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x111cff45948819988857bbf1966a0399e0d1141e
    -- Coinone
    , ('ethereum', 0x167a9333bf582556f35bd4d16a7e80e191aa6476, 'Coinone', 'Coinone 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x167a9333bf582556f35bd4d16a7e80e191aa6476
    -- Bitkub
    , ('ethereum', 0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33, 'Bitkub', 'Bitkub Hot Wallet 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33
    , ('ethereum', 0x3d1d8a1d418220fd53c18744d44c182c46f47468, 'Bitkub', 'Bitkub Hot Wallet 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x3d1d8a1d418220fd53c18744d44c182c46f47468
    , ('ethereum', 0x59e0cda5922efba00a57794faf09bf6252d64126, 'Bitkub', 'Bitkub Hot Wallet 3', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x59e0cda5922efba00a57794faf09bf6252d64126
    , ('ethereum', 0x1579b5f6582c7a04f5ffeec683c13008c4b0a520, 'Bitkub', 'Bitkub Hot Wallet 4', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x1579b5f6582c7a04f5ffeec683c13008c4b0a520
    -- Indodax
    , ('ethereum', 0x51836a753e344257b361519e948ffcaf5fb8d521, 'Indodax', 'Indodax 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x51836a753e344257b361519e948ffcaf5fb8d521
    , ('ethereum', 0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719, 'Indodax', 'Indodax 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719
    -- MaiCoin
    , ('ethereum', 0x477b8d5ef7c2c42db84deb555419cd817c336b6f, 'MaiCoin', 'MaiCoin 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x477b8d5ef7c2c42db84deb555419cd817c336b6f
    -- Bitfront
    , ('ethereum', 0xdf5021a4c1401f1125cd347e394d977630e17cf7, 'Bitfront', 'Bitfront 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xdf5021a4c1401f1125cd347e394d977630e17cf7
    -- Bit2C
    , ('ethereum', 0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a, 'Bit2C', 'Bit2C 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a
    -- FixedFloat
    , ('ethereum', 0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f, 'FixedFloat', 'FixedFloat 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f
    -- Bitrue, Source: 
    , ('ethereum', 0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21, 'Bitrue', 'Bitrue 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21
    -- CoinMetro, Source: https://etherscan.io/accounts/label/coinmetro
    , ('ethereum', 0xa270f3ad1a7a82e6a3157f12a900f1e25bc4fbfd, 'CoinMetro', 'CoinMetro 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x7c1c73bf60feb40cbcf0f12324200238ee23bb54, 'CoinMetro', 'CoinMetro MultiSig', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xbac7c449689a2d3c51c386d8e657338c41ab3030, 'CoinMetro', 'CoinMetro Treasury', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xf3e35734b7413f87c2054a16ce04230d803e4dc3, 'CoinMetro', 'CoinMetro Vault Aug 2020', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xfad672dc92c2d2db0aa093331bd1098e30249ab8, 'CoinMetro', 'CoinMetro Vault Feb 2020', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x165fe6a10812faa49515522d685a27c6bf12dba9, 'CoinMetro', 'CoinMetro Vault Feb 2021', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xdd06b66c76d9c6fdc41935a7b32566c646325005, 'CoinMetro', 'CoinMetro XCM Utility Vault', 'hildobby', date '2022-08-28')
    -- BlockTrades
    , ('ethereum', 0x007174732705604bbbf77038332dc52fd5a5000c, 'BlockTrades', 'BlockTrades 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x007174732705604bbbf77038332dc52fd5a5000c
    -- Catex
    , ('ethereum', 0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f, 'Catex', 'Catex', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f
    -- Mercatox
    , ('ethereum', 0xe03c23519e18d64f144d2800e30e81b0065c48b5, 'Mercatox', 'Mercatox 1', 'hildobby', date '2022-08-28') --https://etherscan.io/address/0xe03c23519e18d64f144d2800e30e81b0065c48b5
    -- Sparrow 
    , ('ethereum', 0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5, 'Sparrow', 'Sparrow 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5
    , ('ethereum', 0xe855283086fbee485aecf2084345a91424c23954, 'Sparrow', 'Sparrow 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xe855283086fbee485aecf2084345a91424c23954
    -- DMEX
    , ('ethereum', 0x2101e480e22c953b37b9d0fe6551c1354fe705e6, 'DMEX', 'DMEX 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x2101e480e22c953b37b9d0fe6551c1354fe705e6
    -- BitBlinx
    , ('ethereum', 0x5d375281582791a38e0348915fa9cbc6139e9c2a, 'BitBlinx', 'BitBlinx', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x5d375281582791a38e0348915fa9cbc6139e9c2a
    -- OMGFIN
    , ('ethereum', 0x03e3ff995863828554282e80870b489cc31dc8bc, 'OMGFIN', 'OMGFIN', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x03e3ff995863828554282e80870b489cc31dc8bc
    -- CREX24, Source: https://etherscan.io/accounts/label/crex24
    , ('ethereum', 0x521db06bf657ed1d6c98553a70319a8ddbac75a3, 'CREX24', 'CREX24 1', 'hildobby', date '2022-08-28')
    -- Panda
    , ('ethereum', 0xcacc694840ecebadd9b4c419e5b7f1d73fedf999, 'Panda Exchange', 'Panda Exchange Hot Wallet 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xcacc694840ecebadd9b4c419e5b7f1d73fedf999
    , ('ethereum', 0xb709d82f0706476457ae6bad7c3534fbf424382c, 'Panda Exchange', 'Panda Exchange Hot Wallet 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xb709d82f0706476457ae6bad7c3534fbf424382c
    -- IDAX
    , ('ethereum', 0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7, 'IDAX', 'IDAX', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7
    -- FlataExchange
    , ('ethereum', 0x14301566b9669b672878d86ff0b1d18dd58054e9, 'FlataExchange', 'FlataExchange', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x14301566b9669b672878d86ff0b1d18dd58054e9
    -- XT.com Exchange
    , ('ethereum', 0xefda0cb780a8564903285ed25df3cc024f3b2982, 'XT.com Exchange', 'XT.com Exchange 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xefda0cb780a8564903285ed25df3cc024f3b2982
    -- BitBase
    , ('ethereum', 0x0d8824ca76e627e9cc8227faa3b3993986ce9e48, 'BitBase', 'BitBase 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x0d8824ca76e627e9cc8227faa3b3993986ce9e48
    , ('ethereum', 0x6dcd15a0dbefd0700063a4445382d3506391a41a, 'BitBase', 'BitBase 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6dcd15a0dbefd0700063a4445382d3506391a41a
    -- KickEX
    , ('ethereum', 0x352bdabe484499e4c25c3536cc3eda1edbc5ad29, 'KickEX', 'KickEX 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x352bdabe484499e4c25c3536cc3eda1edbc5ad29
    , ('ethereum', 0xaf4ff15c9809e246111802f04a6acc7160992fef, 'KickEX', 'KickEX Hot Wallet 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xaf4ff15c9809e246111802f04a6acc7160992fef
    , ('ethereum', 0xc153121042832ac11587ebe361b8dc3ccd90e9e4, 'KickEX', 'KickEX Cold Wallet', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xc153121042832ac11587ebe361b8dc3ccd90e9e4
    -- Coinbene
    , ('ethereum', 0x9539e0b14021a43cde41d9d45dc34969be9c7cb0, 'Coinbene', 'Coinbene 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x9539e0b14021a43cde41d9d45dc34969be9c7cb0
    , ('ethereum', 0x33683b94334eebc9bd3ea85ddbda4a86fb461405, 'Coinbene', 'Coinbene Cold Wallet 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x33683b94334eebc9bd3ea85ddbda4a86fb461405
    -- QuantaEx
    , ('ethereum', 0xd344539efe31f8b6de983a0cab4fb721fc69c547, 'QuantaEx', 'QuantaEx 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xd344539efe31f8b6de983a0cab4fb721fc69c547
    , ('ethereum', 0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf, 'QuantaEx', 'QuantaEx 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf
    , ('ethereum', 0x2a048d9a8ffdd239f063b09854976c3049ae659c, 'QuantaEx', 'QuantaEx 3', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x2a048d9a8ffdd239f063b09854976c3049ae659c
    -- Yunbi, Source: https://etherscan.io/accounts/label/yunbi
    , ('ethereum', 0xd94c9ff168dc6aebf9b6cc86deff54f3fb0afc33, 'Yunbi', 'Yunbi 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x42da8a05cb7ed9a43572b5ba1b8f82a0a6e263dc, 'Yunbi', 'Yunbi 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x700f6912e5753e91ea3fae877a2374a2db1245d7, 'Yunbi', 'Yunbi 3', 'hildobby', date '2022-08-28')
    -- CoinW
    , ('ethereum', 0x8705ccfd8a6df3785217c307cbebf9b793310b94, 'CoinW', 'CoinW 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x8705ccfd8a6df3785217c307cbebf9b793310b94
    , ('ethereum', 0xcb243bf48fb443082fae7db47ec96cb120cd6801, 'CoinW', 'CoinW 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xcb243bf48fb443082fae7db47ec96cb120cd6801
    , ('ethereum', 0x429bf8ec3330e02401d72beade86000d9a2e19eb, 'CoinW', 'CoinW 3', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x429bf8ec3330e02401d72beade86000d9a2e19eb
    , ('ethereum', 0x6f31d347457962c9811ff953742870ef5a755de3, 'CoinW', 'CoinW 4', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6f31d347457962c9811ff953742870ef5a755de3
    -- Cryptopia
    , ('ethereum', 0x5baeac0a0417a05733884852aa068b706967e790, 'Cryptopia', 'Cryptopia 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x5baeac0a0417a05733884852aa068b706967e790
    , ('ethereum', 0x2984581ece53a4390d1f568673cf693139c97049, 'Cryptopia', 'Cryptopia 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x2984581ece53a4390d1f568673cf693139c97049
    -- CoinDhan
    , ('ethereum', 0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9, 'CoinDhan', 'CoinDhan 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9
    -- BIKI
    , ('ethereum', 0x6eff3372fa352b239bb24ff91b423a572347000d, 'BIKI', 'BIKI 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6eff3372fa352b239bb24ff91b423a572347000d
    , ('ethereum', 0x6efb20f61b80f6a7ebe7a107bace58288a51fb34, 'BIKI', 'BIKI Old Address', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x6efb20f61b80f6a7ebe7a107bace58288a51fb34
    -- Liqui
    , ('ethereum', 0x8271b2e8cbe29396e9563229030c89679b9470db, 'Liqui', 'Liqui 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x8271b2e8cbe29396e9563229030c89679b9470db
    , ('ethereum', 0x5e575279bf9f4acf0a130c186861454247394c06, 'Liqui', 'Liqui 2', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x5e575279bf9f4acf0a130c186861454247394c06
    -- Hoo.com, Source: https://etherscan.io/accounts/label/hoo-com
    , ('ethereum', 0x980a4732c8855ffc8112e6746bd62095b4c2228f, 'Hoo.com', 'Hoo.com 1', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xd0ec209ad2134899148bec8aef905a6e9997456a, 'Hoo.com', 'Hoo.com 2', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x993b7fcba51d8f75c2dfaec0d17b6649ee0c9068, 'Hoo.com', 'Hoo.com 3', 'hildobby', date '2022-08-28')
    , ('ethereum', 0xec293b9c56f06c8f71392269313d7e2da681d9ac, 'Hoo.com', 'Hoo.com 4', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x0093e5f2a850268c0ca3093c7ea53731296487eb, 'Hoo.com', 'Hoo.com 5', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x008932be50098089c6a075d35f4b5182ee549f8a, 'Hoo.com', 'Hoo.com 6', 'hildobby', date '2022-08-28')
    , ('ethereum', 0x4d4ffb448194504242267585f0ea6f9de6a96de3, 'Hoo.com', 'Hoo.com Deposit Funder', 'hildobby', date '2023-08-31')
    -- Beldex
    , ('ethereum', 0x258b7b9a1ba92f47f5f4f5e733293477620a82cb, 'Beldex', 'Beldex 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x258b7b9a1ba92f47f5f4f5e733293477620a82cb
    -- SouthXchange
    , ('ethereum', 0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4, 'SouthXchange', 'SouthXchange 1', 'hildobby', date '2022-08-28') -- https://etherscan.io/address/0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4
    -- CamboChanger, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x4dc98c79a52968a6c20ce9a7a08d5e8d1c2d5605, 'CamboChanger', 'CamboChanger 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x88988d6ef12d7084e34814b9edafa01ae0d05082, 'CamboChanger', 'CamboChanger 2', 'hildobby', date '2023-04-06')
    -- SimpleSwap, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xbb3fd383d1c5540e52ef0a7bcb9433375793aeaf, 'SimpleSwap', 'SimpleSwap 1', 'hildobby', date '2023-04-06')
    -- Alcumex Exchange, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x2ddd202174a72514ed522e77972b461b03155525, 'Alcumex Exchange', 'Alcumex Exchange 1', 'hildobby', date '2023-04-06')
    -- APROBIT, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xaecbe94703df39b49ac440feb177c7f1f782c064, 'APROBIT', 'APROBIT 1', 'hildobby', date '2023-04-06')
    -- Artis Turba Exchange, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xf0c80fb9fb22bef8269cb6feb9a51130288a671f, 'Artis Turba Exchange', 'Artis Turba Exchange 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x94597850916a49b3b152ee374e97260b99249f5b, 'Artis Turba Exchange', 'Artis Turba Exchange 2', 'hildobby', date '2023-04-06')
    -- ArzPaya.com, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x82a403c14483931b2ff6e4440c8373ccfee698b8, 'ArzPaya.com', 'ArzPaya.com 1', 'hildobby', date '2023-04-06')
    -- Azbit, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x92dbd8e0a46edd62aa42d1f7902d0e496bddc15a, 'Azbit', 'Azbit 1', 'hildobby', date '2023-04-06')
    -- Bidesk, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x0bb5de248dbbd31ee6c402c3c4a70293024acf74, 'Bidesk', 'Bidesk 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xed5cdb0d02152046e6f234ad578613831b9184d4, 'Bidesk', 'Bidesk 3', 'hildobby', date '2023-04-06')
    -- Bitbee, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x2b49ce21ad2004cfb3d0b51b2e8ec0406d632513, 'Bitbee', 'Bitbee 1', 'hildobby', date '2023-04-06')
    -- BiteBTC, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x28ebe764b8f9a853509840645216d3c2c0fd774b, 'BiteBTC', 'BiteBTC 1', 'hildobby', date '2023-04-06')
    -- Bitexlive, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x7217d64f77041ce320c356d1a2185bcb89798a0a, 'Bitexlive', 'Bitexlive 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x57a47cfe647306a406118b6cf36459a1756823d0, 'Bitexlive', 'Bitexlive 2', 'hildobby', date '2023-04-06')
    -- BitKeep, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x603d022611bfe6a101dcdab207d96c527f1d4d8e, 'BitKeep', 'BitKeep 1', 'hildobby', date '2023-04-06')
    -- BITStorage, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x1b8a38ea02ceda9440e00c1aeba26ee2dc570423, 'BITStorage', 'BITStorage 1', 'hildobby', date '2023-04-06')
    -- BitUN.io, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xaa90b4aae74cee41e004bc45e45a427406c4dcae, 'BitUN.io', 'BitUN.io 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xf8d04a720520d0bcbc722b1d21ca194aa22699f2, 'BitUN.io', 'BitUN.io 2', 'hildobby', date '2023-04-06')
    -- Bololex.com, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xdf8752caa319668006580ddf48db25a23728b926, 'Bololex.com', 'Bololex.com 1', 'hildobby', date '2023-04-06')
    -- BTC-Alpha Exchange, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x1c00d840ccaa67c494109f46e55cfeb2d8562f5c, 'BTC-Alpha Exchange', 'BTC-Alpha Exchange 1', 'hildobby', date '2023-04-06')
    -- C2CX, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xd7c866d0d536937bf9123e02f7c052446588189f, 'C2CX', 'C2CX 1', 'hildobby', date '2023-04-06')
    -- ChainX, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xfd648cc72f1b4e71cbdda7a0a91fe34d32abd656, 'ChainX', 'ChainX 1', 'hildobby', date '2023-04-06')
    -- Changelly, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x96fc4553a00c117c5b0bed950dd625d1c16dc894, 'Changelly', 'Changelly 1', 'hildobby', date '2023-04-06')
    -- Coindelta, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xb6ba1931e4e74fd080587688f6db10e830f810d5, 'Coindelta', 'Coindelta 1', 'hildobby', date '2023-04-06')
    -- Coinswitch, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xd0808da05cc71a9f308d330bc9c5c81bbc26fc59, 'Coinswitch', 'Coinswitch 1', 'hildobby', date '2023-04-06')
    -- Eidoo, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xf1c525a488a848b58b95d79da48c21ce434290f7, 'Eidoo', 'Eidoo 1', 'hildobby', date '2023-04-06')
    -- Eigen Fx, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x608f94df1c1d89ea13e5984d7bf107df137a6541, 'Eigen Fx', 'Eigen Fx 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xeb9ebf2c624ebee42e0853da6443ddc6c8020de7, 'Eigen Fx', 'Eigen Fx 2', 'hildobby', date '2023-04-06')
    -- Eterbase, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x8d76166c22658a144c0211d87abf152e6a2d9d95, 'Eterbase', 'Eterbase 1', 'hildobby', date '2023-04-06')
    -- Exchange A, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xd3808c5d48903be1490989f3fce2a2b3890e8eb6, 'Exchange A', 'Exchange A 1', 'hildobby', date '2023-04-06')
    -- Faa.st, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x94fe3ad91dacba8ec4b82f56ff7c122181f1535d, 'Faa.st', 'Faa.st 1', 'hildobby', date '2023-04-06')
    -- FCoin, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x915d7915f2b469bb654a7d903a5d4417cb8ea7df, 'FCoin', 'FCoin 1', 'hildobby', date '2023-04-06')
    -- Flybit, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x91e18ee76483fa2ec5cfe2959df46673c2565be0, 'Flybit', 'Flybit 1', 'hildobby', date '2023-04-06')
    -- Folgory Exchange, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x0021845f4c2604c58f9ba5b7bff58d16a2ab372c, 'Folgory Exchange', 'Folgory Exchange 1', 'hildobby', date '2023-04-06')
    -- GBX, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x9f5ca0012b9b72e8f3db57092a6f26bf4f13dc69, 'GBX', 'GBX 1', 'hildobby', date '2023-04-06')
    -- GGBTC.com, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x9fb01a2584aac5aae3fab1ed25f86c5269b32999, 'GGBTC.com', 'GGBTC.com 1', 'hildobby', date '2023-04-06')
    -- IndoEx LTD, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xb1a34309af7f29b4195a6b589737f86e14597ddc, 'IndoEx LTD', 'IndoEx LTD 1', 'hildobby', date '2023-04-06')
    -- Kryptono, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xe8a0e282e6a3e8023465accd47fae39dd5db010b, 'Kryptono', 'Kryptono 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x629a7144235259336ea2694167f3c8b856edd7dc, 'Kryptono', 'Kryptono 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x30b71d015f60e2f959743038ce0aaec9b4c1ea44, 'Kryptono', 'Kryptono 3', 'hildobby', date '2023-04-06')
    -- Livecoin.net, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x243bec9256c9a3469da22103891465b47583d9f1, 'Livecoin.net', 'Livecoin.net 1', 'hildobby', date '2023-04-06')
    -- MinedTrade.com, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xac338d9faac562df26d702880c796e1024e2698a, 'MinedTrade.com', 'MinedTrade.com 1', 'hildobby', date '2023-04-06')
    -- NEXBIT Pro, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xae7006588d03bd15d6954e3084a7e644596bc251, 'NEXBIT Pro', 'NEXBIT Pro 1', 'hildobby', date '2023-04-06')
    -- Streamity, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x9bf25700727d10a857099d1033ce2cc493c3b61a, 'Streamity', 'Streamity 1', 'hildobby', date '2023-04-06')
    -- Switchain, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xa96b536eef496e21f5432fd258b6f78cf3673f74, 'Switchain', 'Switchain 1', 'hildobby', date '2023-04-06')
    -- TAGZ, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xea3a46bd1dbd0620d80037f70d0bf7c7dc5a837c, 'TAGZ', 'TAGZ 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xed8204345a0cf4639d2db61a4877128fe5cf7599, 'TAGZ', 'TAGZ 2', 'hildobby', date '2023-04-06')
    -- Tokocrypto, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x0068eb681ec52dbd9944517d785727310b494575, 'Tokocrypto', 'Tokocrypto 1', 'hildobby', date '2023-04-06')
    -- Vinex, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0xb436c96c6de1f50a160ed307317c275424dbe4f2, 'Vinex', 'Vinex 1', 'hildobby', date '2023-04-06')
    -- Wintermute, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x0e5069514a3dd613350bab01b58fd850058e5ca4, 'Wintermute', 'Wintermute 1', 'hildobby', date '2023-04-06')
    -- ZB.com, source: https://github.com/dawsbot/evm-labels/blob/master/src/mainnet/exchange/all.csv
    , ('ethereum', 0x60d0cc2ae15859f69bf74dadb8ae3bd58434976b, 'ZB.com', 'ZB.com 1', 'hildobby', date '2023-04-06')
    -- Korbit, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/korbit/index.js
    , ('ethereum', 0x0c01089aedc45ab0f43467cceca6b4d3e4170bea, 'Korbit', 'Korbit 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x2864de013415b6c2c7a96333183b20f0f9cc7532, 'Korbit', 'Korbit 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x8550e644d74536f1df38b17d5f69aa1bfe28ae86, 'Korbit', 'Korbit 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xd03be958e6b8da2d28ac8231a2291d6e4f0a7ea7, 'Korbit', 'Korbit 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xd6e0F7dA4480b3AD7A2C8b31bc5a19325355CA15, 'Korbit', 'Korbit 5', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe5d7ccc5fc3b3216c4dff3a59442f1d83038468c, 'Korbit', 'Korbit 6', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xe83a48cae4d7120e8ba1c2e0409568ffba532e87, 'Korbit', 'Korbit 7', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xf0bc8FdDB1F358cEf470D63F96aE65B1D7914953, 'Korbit', 'Korbit 8', 'hildobby', date '2023-04-06')
    -- Swissborg, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/swissborg/index.js
    , ('ethereum', 0x5770815b0c2a09a43c9e5aecb7e2f3886075b605, 'Swissborg', 'Swissborg 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x94596096320a6b4eab43556ad1ed8c4c3d51c9aa, 'Swissborg', 'Swissborg 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x5cedc1923c33b253aedf24bf038eee6cbbb68a6a, 'Swissborg', 'Swissborg 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x42b86a269fb3d5368d880c519badaba77ec00130, 'Swissborg', 'Swissborg 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x87cbc48075d7aa1760ac71c41e8bc289b6a31f56, 'Swissborg', 'Swissborg 5', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xcde4c1b984f3f02f997ecff9980b06316de2577d, 'Swissborg', 'Swissborg 6', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x6cf9aa65ebad7028536e353393630e2340ca6049, 'Swissborg', 'Swissborg 7', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x7153d2ef9f14a6b1bb2ed822745f65e58d836c3f, 'Swissborg', 'Swissborg 8', 'hildobby', date '2023-04-06')
    -- Firi, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/fire/index.js
    , ('ethereum', 0x66a0be112efe2cc3bc2f09fa2acaaf9f593b0265, 'Firi', 'Firi 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xa6f617f873684ed062c9df281145250b3e4ee2d2, 'Firi', 'Firi 2', 'hildobby', date '2023-04-06')
    -- Phemex, source: https://phemex.com/proof-of-reserves
    , ('ethereum', 0xf7d13c7dbec85ff86ee815f6dcbb3dedac78ca49, 'Phemex', 'Phemex 1', 'hildobby', date '2023-04-06')
    -- Cake DeFi, source: https://cakedefi.com/transparency
    , ('ethereum', 0x94fa70d079d76279e1815ce403e9b985bccc82ac, 'Cake DeFi', 'Cake DeFi 1', 'hildobby', date '2023-04-06')
    -- MaskEX, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/maskex/index.js
    , ('ethereum', 0x09b1806df13062b5f653beda6998972cabcf7009, 'MaskEX', 'MaskEX 1', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x0c78fd926a8fc9cfc682bdc6b411942d9c7edb7a, 'MaskEX', 'MaskEX 2', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x0feabb61f67e859811aafce83a5ab780f8c53c0a, 'MaskEX', 'MaskEX 3', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x33fe5557e90a872a065f2acfd973847e33fc4532, 'MaskEX', 'MaskEX 4', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x6f531cf07f2d659dcfb371b1a7f4c0157a168332, 'MaskEX', 'MaskEX 5', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x71467da4c0b0db4e889da703e6ff1cd740f1f74a, 'MaskEX', 'MaskEX 6', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x7ac724cac6e4ddc24c102b1006f41bc8a6a5c1c5, 'MaskEX', 'MaskEX 7', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x7e0616656934a09373b1e1114de2c20a77513d16, 'MaskEX', 'MaskEX 8', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x80b62f0ea7a89bbc4df4c95e2ad363e5c153b80e, 'MaskEX', 'MaskEX 9', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x823c8e533657b0004b5ab8553d84502ba2e571f7, 'MaskEX', 'MaskEX 10', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x833f3b6faa717079fb3a1030f6207c57b1c591bd, 'MaskEX', 'MaskEX 11', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x9f1bb5349d481065561a84cbd7f84982fd533359, 'MaskEX', 'MaskEX 12', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xA310b3eecA53B9C115af529faF92Bb5ca4B41494, 'MaskEX', 'MaskEX 13', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xa4E71851A8c8eaeFeb20A994159F4A443E46059b, 'MaskEX', 'MaskEX 14', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xbe921ea3bd0c879a8688b7fabe6b3c8a471df90d, 'MaskEX', 'MaskEX 15', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xc3edbb9c181016cef5d76491f835930e9c8c4d2c, 'MaskEX', 'MaskEX 16', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xdd9c649edb7ff80c6c9d238344260184a4f94b88, 'MaskEX', 'MaskEX 17', 'hildobby', date '2023-04-06')
    , ('ethereum', 0xfb65377800a7282cf81baf0f335fbc6f8ff36776, 'MaskEX', 'MaskEX 18', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x0ce7eefb9f862aa0374ee7bbc4d8a0fc2c651517, 'MaskEX', 'MaskEX 19', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x0ce92d3a15908b53371ff1afcae800f28142250c, 'MaskEX', 'MaskEX 20', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x95ad8841376058a000F489196F05ecf176bEB8ac, 'MaskEX', 'MaskEX 21', 'hildobby', date '2023-04-06')
    , ('ethereum', 0x0B3c7bcE764E6f1B52443e30fcb4f34997A0674c, 'MaskEX', 'MaskEX 22', 'hildobby', date '2023-04-06')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('ethereum', 0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df, 'WOO Network', 'WOO Network 1', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4, 'WOO Network', 'WOO Network 2', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9, 'WOO Network', 'WOO Network 3', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xea319fd75766f5180018f8e760f51c3d3c457496, 'WOO Network', 'WOO Network 4', 'hildobby', date '2023-04-07')
    -- Coinsquare, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/coinsquare/index.js
    , ('ethereum', 0x02fdc44Bf226E49DCecA4775Afaef3360e9C4EE9, 'Coinsquare', 'Coinsquare 1', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x0fcFF154753e337983613889b69dd85Fe8a1a145, 'Coinsquare', 'Coinsquare 2', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x14AA1AD09664c33679aE5689d93085B8F7c84bd3, 'Coinsquare', 'Coinsquare 3', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x3858A27eeCB5f1144473E35A293cb1B2bda6DfF4, 'Coinsquare', 'Coinsquare 4', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x476B067CbFF8ACB805038E9dAEF5D51c7612d593, 'Coinsquare', 'Coinsquare 5', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x48a0B5f7DE8789a3962918C6DF4A766c0c8857B0, 'Coinsquare', 'Coinsquare 6', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x56E89a4b2E3924c336d52CE0ad98fF23E1a51627, 'Coinsquare', 'Coinsquare 7', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x6A73f209d25CC9c089170cc5b54962e0c7614E0c, 'Coinsquare', 'Coinsquare 8', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x6d712f120bD65aD54a5F56670976788a044Cb987, 'Coinsquare', 'Coinsquare 9', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x7061d86A274B398a1fB7Cdb74B3abBc7601e105f, 'Coinsquare', 'Coinsquare 10', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x7ee87dd5BB9924Cb85CA2916Bd4E04299D3A8EcC, 'Coinsquare', 'Coinsquare 11', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x82Be7cFeF05B70c4AF47F8fd70F636201121341b, 'Coinsquare', 'Coinsquare 12', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x8623c08A4B880799CF65E75137ec9759DB336637, 'Coinsquare', 'Coinsquare 13', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x89813b57AE92e74Fb808eb7639d3A0050c9b3D7D, 'Coinsquare', 'Coinsquare 14', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x8e080C5d233F2A14A37d024c0382bF0585146993, 'Coinsquare', 'Coinsquare 15', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x910695E5C7c14499B554fb132A9710988a42fC38, 'Coinsquare', 'Coinsquare 16', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x9C6D4A1922Eed56Ee9de148c5BA9b1b477FEcBb6, 'Coinsquare', 'Coinsquare 17', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xC4d75abAb14Ef006d5Ac9fe901a8ed616C4e2627, 'Coinsquare', 'Coinsquare 18', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xD381347EE757F53aE4B3b6822DAeC3E2A14B2005, 'Coinsquare', 'Coinsquare 19', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xD5B2C371808018ee131ad387877C4d58e08e7A06, 'Coinsquare', 'Coinsquare 20', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xd093F2Ee92cf32B4D3EBefd965447415074DD6c8, 'Coinsquare', 'Coinsquare 21', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xf9c91937737cCaFE9bBb662b1917B54F9606Ca13, 'Coinsquare', 'Coinsquare 22', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xfac596Facd1901458C1C6347397a6e5D0769736c, 'Coinsquare', 'Coinsquare 23', 'hildobby', date '2023-04-07')
    -- CoinDCX, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/coindcx/index.js
    , ('ethereum', 0x2e5129e77c928D96b5A70c0effB97Ee6e95D77b6, 'CoinDCX', 'CoinDCX 1', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x3698cc7F524BAde1a05e02910538F436a3E94384, 'CoinDCX', 'CoinDCX 2', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x37b6bD5fECE5b88B6E8e825196bcc868a2FeEd51, 'CoinDCX', 'CoinDCX 3', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x38f76d1C8fcC854fb4d2416dDAeC8Df41Ab60867, 'CoinDCX', 'CoinDCX 4', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x4D24EecEcb86041F47bca41265319e9f06aE2Fcb, 'CoinDCX', 'CoinDCX 5', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x660e3Bd3bcDa11538fa331282666F1d001b87A42, 'CoinDCX', 'CoinDCX 6', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x763104507945B6b7f21Ee68b92048A53F7debF18, 'CoinDCX', 'CoinDCX 7', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x78bba2389c2cEEb6f94C70eD133712E3B3e2C4D0, 'CoinDCX', 'CoinDCX 8', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x881f982575a3EcBEA6fe133ddB0951303215d130, 'CoinDCX', 'CoinDCX 9', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x892787C947fdd1CF6C525C6107d80265D3D7EBb4, 'CoinDCX', 'CoinDCX 10', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x8c7Efd5B04331EFC618e8006f19019A3Dc88973e, 'CoinDCX', 'CoinDCX 11', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xA15B94629727152c952a6979d899F71426cE7976, 'CoinDCX', 'CoinDCX 12', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xA4FE2F90a8991A410c825C983CbB6A92d03607fc, 'CoinDCX', 'CoinDCX 13', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xA916a54af7553BAe6172e510D067826Bd204d0dD, 'CoinDCX', 'CoinDCX 14', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xAA8bC1fc0FCfdcA5b7E5D35e5AC13800850d90C7, 'CoinDCX', 'CoinDCX 15', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xCCFA6f3b01c7bf07B033A9d496Fdf22F0cdF5293, 'CoinDCX', 'CoinDCX 16', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xEF0Fc6322b2b5b02f0Db68f8eA74819560124b2d, 'CoinDCX', 'CoinDCX 17', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xF25d1D2507ce1f956F5BAb45aD2341e3c0DB6d3C, 'CoinDCX', 'CoinDCX 18', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xb188a49Da0836c289dcB4Fa0E856647a33DE537F, 'CoinDCX', 'CoinDCX 19', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xb6DFCF39503dddDe140105954a819e944CE543A7, 'CoinDCX', 'CoinDCX 20', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xb79421720b92180487f71F13c5D5D8B9ecA27BF1, 'CoinDCX', 'CoinDCX 21', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xb85E9868a0E8492353Db5C3022e6F96fc62F2306, 'CoinDCX', 'CoinDCX 22', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xe298dC1c377e4511f32Afd2362726c4F3A644356, 'CoinDCX', 'CoinDCX 23', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xf809c975eFAD2Bc33E21B5972DB765A6230E956A, 'CoinDCX', 'CoinDCX 24', 'hildobby', date '2023-04-07')
    -- NBX, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/nbx/index.js
    , ('ethereum', 0x29af949c3D218C1133bD16257ed029E92deFb168, 'NBX', 'NBX 1', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x8Cad96fB23924Ebc37b8CdAFa8400AD856fE4a2C, 'NBX', 'NBX 2', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xAeB81c391Ac427B6443310fF1cB73a21E071e5ad, 'NBX', 'NBX 3', 'hildobby', date '2023-04-07')
    -- BitVenus, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/bitvenus/index.js
    , ('ethereum', 0xe43c53c466a282773f204df0b0a58fb6f6a88633, 'BitVenus', 'BitVenus 1', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x2b097741854eedeb9e5c3ef9d221fb403d8d8609, 'BitVenus', 'BitVenus 2', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x686b9202a36c09ce8aba8b49ae5f75707edec5fe, 'BitVenus', 'BitVenus 3', 'hildobby', date '2023-04-07')
    , ('ethereum', 0xef7a2610a7c9cfb2537d68916b6a87fea8acfec3, 'BitVenus', 'BitVenus 4', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x5631aa1fc1868703a962e2fd713dc02cad07c1db, 'BitVenus', 'BitVenus 5', 'hildobby', date '2023-04-07')
    , ('ethereum', 0x4785e47ae7061632c2782384da28b9f68a5647a3, 'BitVenus', 'BitVenus 6', 'hildobby', date '2023-04-07')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)