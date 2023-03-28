{{config(alias='cex_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby","soispoke","ilemi"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    ('ethereum','0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd551234ae421e3bcba99a0da6d736074f22192ff', 'Binance 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x564286362092d8e7936f0549571a803b203aaced', 'Binance 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0681d8db095565fe8a346fa0277bffde9c0edbbf', 'Binance 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfe9e8709d3215310075d67e3ed32a380ccf451c8', 'Binance 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67', 'Binance 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8', 'Binance 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x001866ae5b3de6caa5a51543fd9fb64f524f5478', 'Binance 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x85b931a32a0725be14285b66f1a22178c672d69b', 'Binance 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x708396f17127c42383e3b9014072679b2f60b82f', 'Binance 11', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe0f0cfde7ee664943906f17f7f14342e76a5cec7', 'Binance 12', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8f22f2063d253846b53609231ed80fa571bc0c8f', 'Binance 13', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x28c6c06298d514db089934071355e5743bf21d60', 'Binance 14', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x21a31ee1afc51d94c2efccaa2092ad1028285549', 'Binance 15', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdfd5293d8e347dfe59e90efd55b2956a1343963d', 'Binance 16', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x56eddb7aa87536c09ccc2793473599fd21a8b17f', 'Binance 17', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9696f59e4d72e237be84ffd425dcad154bf96976', 'Binance 18', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4d9ff50ef4da947364bb9650892b2554e7be5e2b', 'Binance 19', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4976a4a02f38326660d17bf34b431dc6e2eb2327', 'Binance 20', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd88b55467f58af508dbfdc597e8ebd2ad2de49b3', 'Binance 21', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7dfe9a368b6cf0c0309b763bb8d16da326e8f46e', 'Binance 22', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x345d8e3a1f62ee6b1d483890976fd66168e390f2', 'Binance 23', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc3c8e0a39769e2308869f7461364ca48155d1d9e', 'Binance 24', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2e581a5ae722207aa59acd3939771e7c7052dd3d', 'Binance 25', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x44592b81c05b4c35efb8424eb9d62538b949ebbf', 'Binance 26', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x06a0048079ec6571cd1b537418869cde6191d42d', 'Binance 29', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x892e9e24aea3f27f4c6e9360e312cce93cc98ebe', 'Binance 30', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x00799bbc833d5b168f0410312d2a8fd9e0e3079c', 'Binance 31', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x141fef8cd8397a390afe94846c8bd6f4ab981c48', 'Binance 32', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x50d669f43b484166680ecc3670e4766cdb0945ce', 'Binance 33', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2f7e209e0f5f645c7612d7610193fe268f118b28', 'Binance 34', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5a52E96BAcdaBb82fd05763E25335261B270Efcb', 'Binance 36', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8b99f3660622e21f2910ecca7fbe51d654a1517d', 'Binance Charity', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4', 'Binance JEX', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc365c3315cf926351ccaf13fa7d19c8c4058c8e1', 'Binance Pool', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x61189da79177950a7272c88c6058b96d4bcd6be2', 'Binance US', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4fabb145d64652a948d72533023f6e7a623c7c53', 'Binance USD', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2f47a1c2db4a3b78cda44eade915c3b19107ddcc', 'Binance Eth2 Depositor', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb3f923eabaf178fc1bd8e13902fc5c61d3ddef5b', 'Wintermute Binance Deposit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance: BTC Proof of Assets', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x47ac0fb4F2d84898e4d9e7b4dab3c24507a6d503', 'Binance: Stablecoin Proof of Assets', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9be89d2a4cd102d8fecc6bf9da793be995c22541', 'Binance: ETH and ERC20 tokens Proof of Assets', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7884f51dc1410387371ce61747cb6264e1daee0b', 'Binance: DOT Proof of Assets', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xff0a024b66739357c4ed231fb3dbc0c8c22749f5', 'Binance: WRX Proof of Assets', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    -- FTX, Source: https://etherscan.io/accounts/label/ftx
    , ('ethereum', '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2', 'FTX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc098b2a3aa256d2140208c3de6543aaef5cd3a94', 'FTX 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x25eaff5b179f209cf186b1cdcbfa463a69df4c45', 'FTX (formerly Blockfolio)', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    -- FTX US, Source: https://etherscan.io/accounts/label/ftx
    , ('ethereum', '0x7abe0ce388281d2acf297cb089caef3819b13448', 'FTX US', 'institution', 'agaperste', 'static', timestamp('2022-11-15'), now(), 'cex_ethereum', 'identifier')
    -- Coinbase, Source: https://etherscan.io/accounts/label/coinbase
    , ('ethereum', '0x71660c4005ba85c37ccec55d0c4493e66fe775d3', 'Coinbase 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x503828976d22510aad0201ac7ec88293211d23da', 'Coinbase 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740', 'Coinbase 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x3cd751e6b0078be393132286c442345e5dc49699', 'Coinbase 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511', 'Coinbase 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xeb2629a2734e272bcc07bda959863f316f4bd4cf', 'Coinbase 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd688aea8f7d450909ade10c47faa95707b0682d9', 'Coinbase 7', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x02466e547bfdab679fc49e96bbfc62b9747d997c', 'Coinbase 8', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6b76f8b1e9e59913bfe758821887311ba1805cab', 'Coinbase 9', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43', 'Coinbase 10', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x77696bb39917c91a0c3908d577d5e322095425ca', 'Coinbase 11', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7c195d981abfdc3ddecd2ca0fed0958430488e34', 'Coinbase 12', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x95a9bd206ae52c4ba8eecfc93d18eacdd41c88cc', 'Coinbase 13', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb739d0895772dbb71a89a3754a160269068f0d45', 'Coinbase 14', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa090e606e30bd747d4e6245a1517ebe430f0057e', 'Coinbase Miscellaneous', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf6874c88757721a02f47592140905c4336dfbc61', 'Coinbase Commerce', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x881d4032abe4188e2237efcd27ab435e81fc6bb1', 'Coinbase Commerce 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- OKX, Source: https://etherscan.io/accounts/label/okex
    , ('ethereum', '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b', 'OKX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3', 'OKX 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa7efae728d2936e78bda97dc267687568dd593f3', 'OKX 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2c8fbb630289363ac80705a1a61273f76fd5a161', 'OKX 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x59fae149a8f8ec74d5bc038f8b76d25b136b9573', 'OKX 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x98ec059dc3adfbdd63429454aeb0c990fba4a128', 'OKX 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5041ed759dd4afc3a72b8192c143f72f4724081a', 'OKX 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xcba38020cd7b6f51df6afaf507685add148f6ab6', 'OKX 8', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x461249076b88189f8ac9418de28b365859e46bfd', 'OKX 9', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc5451b523d5fffe1351337a221688a62806ad91a', 'OKX 10', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x42436286a9c8d63aafc2eebbca193064d68068f2', 'OKX 11', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x69a722f0b5da3af02b4a205d6f0c285f4ed8f396', 'OKX 12', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc708a1c712ba26dc618f972ad7a187f76c8596fd', 'OKX 13', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6fb624b48d9299674022a23d92515e76ba880113', 'OKX 14', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf59869753f41db720127ceb8dbb8afaf89030de4', 'OKX 15', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x65a0947ba5175359bb457d3b34491edf4cbf7997', 'OKX 16', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4d19c0a5357bc48be0017095d3c871d9afc3f21d', 'OKX 17', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5c52cc7c96bde8594e5b77d5b76d042cb5fae5f2', 'OKX 18', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe9172daf64b05b26eb18f07ac8d6d723acb48f99', 'OKX 19', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7eb6c83ab7d8d9b8618c0ed973cbef71d1921ef2', 'OKX 20', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    -- Huobi, Source: https://etherscan.io/accounts/label/huobi
    , ('ethereum', '0xab5c66752a9e8167967685f1450532fb96d5d24f', 'Huobi 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b', 'Huobi 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfdb16996831753d5331ff813c29a93c76834a0ad', 'Huobi 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xeee28d484628d41a82d01e21d12e2e78d69920da', 'Huobi 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5c985e89dde482efe97ea9f1950ad149eb73829b', 'Huobi 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdc76cd25977e0a5ae17155770273ad58648900d3', 'Huobi 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xadb2b42f6bd96f5c65920b9ac88619dce4166f94', 'Huobi 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa8660c8ffd6d578f657b72c0c811284aef0b735e', 'Huobi 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1062a747393198f70f71ec65a582423dba7e5ab3', 'Huobi 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe93381fb4c4f14bda253907b18fad305d799241a', 'Huobi 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfa4b5be3f2f84f56703c42eb22142744e95a2c58', 'Huobi 11', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x46705dfff24256421a05d056c29e81bdc09723b8', 'Huobi 12', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x32598293906b5b17c27d657db3ad2c9b3f3e4265', 'Huobi 13', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5861b8446a2f6e19a067874c133f04c578928727', 'Huobi 14', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x926fc576b7facf6ae2d08ee2d4734c134a743988', 'Huobi 15', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xeec606a66edb6f497662ea31b5eb1610da87ab5f', 'Huobi 16', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7ef35bb398e0416b81b019fea395219b65c52164', 'Huobi 17', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x229b5c097f9b35009ca1321ad2034d4b3d5070f6', 'Huobi 18', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd8a83b72377476d0a66683cde20a8aad0b628713', 'Huobi 19', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325', 'Huobi 20', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x18916e1a2933cb349145a280473a5de8eb6630cb', 'Huobi 21', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380', 'Huobi 22', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94', 'Huobi 23', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5', 'Huobi 24', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e', 'Huobi 25', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x034f854b44d28e26386c1bc37ff9b20c6380b00d', 'Huobi 26', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404', 'Huobi 27', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0c6c34cdd915845376fb5407e0895196c9dd4eec', 'Huobi 28', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x794d28ac31bcb136294761a556b68d2634094153', 'Huobi 29', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfd54078badd5653571726c3370afb127351a6f26', 'Huobi 30', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9', 'Huobi 31', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4d77a1144dc74f26838b69391a6d3b1e403d0990', 'Huobi 32', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x28ffe35688ffffd0659aee2e34778b0ae4e193ad', 'Huobi 33', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xcac725bef4f114f728cbcfd744a731c2a463c3fc', 'Huobi 34', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x73f8fc2e74302eb2efda125a326655acf0dc2d1b', 'Huobi 35', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0a98fb70939162725ae66e626fe4b52cff62c2e5', 'Huobi 36', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf66852bc122fd40bfecc63cd48217e88bda12109', 'Huobi 37', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x49517ca7b7a50f592886d4c74175f4c07d460a70', 'Huobi 38', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x58c2cb4a6bee98c309215d0d2a38d7f8aa71211c', 'Huobi 39', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x39d9f4640b98189540a9c0edcfa95c5e657706aa', 'Huobi 40', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1d1e10e8c66b67692f4c002c0cb334de5d485e41', 'Huobi Old Address 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1b93129f05cc2e840135aab154223c75097b69bf', 'Huobi Old Address 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xeb6d43fe241fb2320b5a3c9be9cdfd4dd8226451', 'Huobi Old Address 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x956e0dbecc0e873d34a5e39b25f364b2ca036730', 'Huobi Old Address 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6f50c6bff08ec925232937b204b0ae23c488402a', 'Huobi Old Address 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdf95de30cdff4381b69f9e4fa8dddce31a0128df', 'Huobi Old Address 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x25c6459e5c5b01694f6453e8961420ccd1edf3b1', 'Huobi Old Address 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x04645af26b54bd85dc02ac65054e87362a72cb22', 'Huobi Old Address 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb2a48f542dc56b89b24c04076cbe565b3dc58e7b', 'Huobi Old Address 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xea0cfef143182d7b9208fbfeda9d172c2aced972', 'Huobi Old Address 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0c92efa186074ba716d0e2156a6ffabd579f8035', 'Huobi Old Address 11', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x91dfa9d9e062a50d2f351bfba0d35a9604993dac', 'Huobi Old Address 12', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8e8bc99b79488c276d6f3ca11901e9abd77efea4', 'Huobi Old Address 13', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb9a4873d8d2c22e56b8574e8605644d08e047549', 'Huobi Old Address 14', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x170af0a02339743687afd3dc8d48cffd1f660728', 'Huobi Old Address 15', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf775a9a0ad44807bc15936df0ee68902af1a0eee', 'Huobi Old Address 16', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x75a83599de596cbc91a1821ffa618c40e22ac8ca', 'Huobi Old Address 17', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x48ab9f29795dfb44b36587c50da4b30c0e84d3ed', 'Huobi Old Address 18', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x90f49e24a9554126f591d28174e157ca267194ba', 'Huobi Old Address 19', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe3314bbf3334228b257779e28228cfb86fa4261b', 'Huobi Old Address 20', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6edb9d6547befc3397801c94bb8c97d2e8087e2f', 'Huobi Old Address 21', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8aabba0077f1565df73e9d15dd3784a2b0033dad', 'Huobi Old Address 22', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd3a2f775e973c1671f2047e620448b8662dcd3ca', 'Huobi Old Address 23', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1c515eaa87568c850043a89c2d2c2e8187adb056', 'Huobi Old Address 24', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x60b45f993223dcb8bdf05e3391f7630e5a51d787', 'Huobi Old Address 25', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa23d7dd4b8a1060344caf18a29b42350852af481', 'Huobi Old Address 26', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9eebb2815dba2166d8287afa9a2c89336ba9deaa', 'Huobi Old Address 27', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd10e08325c0e95d59c607a693483680fe5b755b3', 'Huobi Old Address 28', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc837f51a0efa33f8eca03570e3d01a4b2cf97ffd', 'Huobi Old Address 29', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf7a8af16acb302351d7ea26ffc380575b741724c', 'Huobi Old Address 30', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x636b76ae213358b9867591299e5c62b8d014e372', 'Huobi Old Address 31', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9a755332d874c893111207b0b220ce2615cd036f', 'Huobi Old Address 32', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xecd8b3877d8e7cd0739de18a5b545bc0b3538566', 'Huobi Old Address 33', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xef54f559b5e3b55b783c7bc59850f83514b6149c', 'Huobi Old Address 34', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9d6d492bd500da5b33cf95a5d610a73360fcaaa0', 'Huobi Mining Pool', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Gate.io, Source: https://etherscan.io/accounts/label/gate-io
    , ('ethereum', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7793cd85c11a924478d358d49b05b37e91b5810f', 'Gate.io 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
	, ('ethereum', '0xD793281182A0e3E023116004778F45c29fc14F19', 'Gate.io: Contract', 'institution', 'web3_data', 'static', timestamp('2023-02-07'), now(), 'cex_ethereum', 'identifier')
	, ('ethereum', '0x234ee9e35f8e9749a002fc42970d570db716453b', 'Gate.io 4', 'institution', 'web3_data', 'static', timestamp('2023-02-07'), now(), 'cex_ethereum', 'identifier')
	, ('ethereum', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io 5', 'institution', 'web3_data', 'static', timestamp('2023-02-07'), now(), 'cex_ethereum', 'identifier')
	, ('ethereum', '0x6596da8b65995d5feacff8c2936f0b7a2051b0d0', 'Gate.io: Deposit Funde', 'institution', 'web3_data', 'static', timestamp('2023-02-07'), now(), 'cex_ethereum', 'identifier')
    -- Kraken, Source: https://etherscan.io/accounts/label/kraken
    , ('ethereum', '0x2910543af39aba0cd09dbb2d50200b3e800a63d2', 'Kraken 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13', 'Kraken 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe853c56864a2ebe4576a807d26fdc4a0ada51919', 'Kraken 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0', 'Kraken 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfa52274dd61e1643d2205169732f29114bc240b3', 'Kraken 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x53d284357ec70ce289d6d64134dfac8e511c8a3d', 'Kraken 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40', 'Kraken 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc6bed363b30df7f35b601a5547fe56cd31ec63da', 'Kraken 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x29728d0efd284d85187362faa2d4d76c2cfc2612', 'Kraken 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xae2d4617c862309a3d75a0ffb358c7a5009c673f', 'Kraken 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x43984d578803891dfa9706bdeee6078d80cfc79e', 'Kraken 11', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x66c57bf505a85a74609d2c83e94aabb26d691e1f', 'Kraken 12', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xda9dfa130df4de4673b89022ee50ff26f6ea73cf', 'Kraken 14', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa83b11093c858c86321fbc4c20fe82cdbd58e09e', 'Kraken 13', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe9f7ecae3a53d2a67105292894676b00d1fab785', 'Kraken Hot Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bitfinex, Source: https://etherscan.io/accounts/label/bitfinex
    , ('ethereum', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'Bitfinex: Hot Wallet', 'institution', 'soispoke', 'static', timestamp('2022-10-19'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f', 'Bitfinex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x742d35cc6634c0532925a3b844bc454e4438f44e', 'Bitfinex 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 'Bitfinex 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdcd0272462140d0a3ced6c4bf970c7641f08cd2c', 'Bitfinex 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4fdd5eb2fb260149a3903859043e962ab89d8ed4', 'Bitfinex 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1b29dd8ff0eb3240238bf97cafd6edea05d5ba82', 'Bitfinex 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x30a2ebf10f34c6c4874b0bdd5740690fd2f3b70c', 'Bitfinex 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x3f7e77b627676763997344a1ad71acb765fc8ac5', 'Bitfinex 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x59448fe20378357f206880c58068f095ae63d5a5', 'Bitfinex 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x36a85757645e8e8aec062a1dee289c7d615901ca', 'Bitfinex 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc56fefd1028b0534bfadcdb580d3519b5586246e', 'Bitfinex 11', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0b73f67a49273fc4b9a65dbd25d7d0918e734e63', 'Bitfinex 12', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x482f02e8bc15b5eabc52c6497b425b3ca3c821e8', 'Bitfinex 13', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1b8766d041567eed306940c587e21c06ab968663', 'Bitfinex 14', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5a710a3cdf2af218740384c52a10852d8870626a', 'Bitfinex 15', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x28140cb1ac771d4add91ee23788e50249c10263d', 'Bitfinex 16', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x53b36141490c419fa27ecabfeb8be1ecadc82431', 'Bitfinex 17', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0cd76cd43992c665fdc2d8ac91b935ca3165e782', 'Bitfinex 18', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe92d1a43df510f82c66382592a047d288f85226f', 'Bitfinex 19', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8103683202aa8da10536036edef04cdd865c225e', 'Bitfinex 20', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e', 'Bitfinex MultiSig 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828', 'Bitfinex MultiSig 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc61b9bb3a7a0767e3179713f3a5c7a9aedce193c', 'Bitfinex MultiSig 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xcafb10ee663f465f9d10588ac44ed20ed608c11e', 'Bitfinex Old Address 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7180eb39a6264938fdb3effd7341c4727c382153', 'Bitfinex Old Address 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5754284f345afc66a98fbb0a0afe71e0f007b949', 'Bitfinex Tether Treasury', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- KuCoin, Source: https://etherscan.io/accounts/label/kucoin
    , ('ethereum', '0x2b5634c42055806a59e9107ed44d43c426e58258', 'KuCoin 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x689c56aef474df92d44a1b70850f808488f9769c', 'KuCoin 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa1d8d972560c2f8144af871db508f0b0b10a3fbf', 'KuCoin 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4ad64983349c49defe8d7a4686202d24b25d0ce8', 'KuCoin 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1692e170361cefd1eb7240ec13d048fd9af6d667', 'KuCoin 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe59cd29be3be4461d79c0881d238cbe87d64595a', 'KuCoin 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x899b5d52671830f567bf43a14684eb14e1f945fe', 'KuCoin 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91', 'KuCoin 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xcad621da75a66c7a8f4ff86d30a2bf981bfc8fdd', 'KuCoin 10', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xec30d02f10353f8efc9601371f56e808751f396f', 'KuCoin 11', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x738cf6903e6c4e699d1c2dd9ab8b67fcdb3121ea', 'KuCoin 12', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd89350284c7732163765b23338f2ff27449e0bf5', 'KuCoin 13', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x88bd4d3e2997371bceefe8d9386c6b5b4de60346', 'KuCoin 14', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb8e6d31e7b212b2b7250ee9c26c56cebbfbe6b23', 'KuCoin 15', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    -- Crypto.com, Source: https://etherscan.io/accounts/label/crypto-com
    , ('ethereum', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x46340b20830761efd32832a74d7169b29feb9758', 'Crypto.com 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x72A53cDBBcc1b9efa39c834A540550e23463AAcB', 'Crypto.com 3', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com 4', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com 5', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_ethereum', 'identifier')
    -- Gemini, Source: https://etherscan.io/accounts/label/gemini
    , ('ethereum', '0xd24400ae8bfebb18ca49be86258a3c749cf46853', 'Gemini 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8', 'Gemini 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea', 'Gemini 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5f65f7b609678448494de4c87521cdf6cef1e932', 'Gemini 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb302bfe9c246c6e150af70b1caaa5e3df60dac05', 'Gemini 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8d6f396d210d385033b348bcae9e4f9ea4e045bd', 'Gemini 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd69b0089d9ca950640f5dc9931a41a5965f00303', 'Gemini 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- BitMart, Source: https://etherscan.io/accounts/label/bitmart
    , ('ethereum', '0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1', 'BitMart 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7', 'BitMart 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- LATOKEN, Source: https://etherscan.io/accounts/label/latoken
    , ('ethereum', '0x0861fca546225fbf8806986d211c8398f7457734', 'LAToken 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7891b20c690605f4e370d6944c8a5dbfac5a451c', 'LAToken 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1b6c1a0e20af81b922cb454c3e52408496ee7201', 'LAToken 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Upbit, Source: https://etherscan.io/accounts/label/upbit
    , ('ethereum', '0x390de26d772d2e2005c6d1d24afc902bae37a4bb', 'Upbit 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xba826fec90cefdf6706858e5fbafcb27a290fbe0', 'Upbit 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5e032243d507c743b061ef021e2ec7fcc6d3ab89', 'Upbit 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc9cf0ec93d764f5c9571fd12f764bae7fc87c84e', 'Upbit Cold Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bittrex, Source: https://etherscan.io/accounts/label/bittrex
    , ('ethereum', '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98', 'Bittrex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe94b04a0fed112f3664e45adb2b8915693dd5ff3', 'Bittrex 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x66f820a414680b5bcda5eeca5dea238543f42054', 'Bittrex 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bithumb, Source: https://etherscan.io/accounts/label/bithumb
    , ('ethereum', '0x88d34944cf554e9cccf4a24292d891f620e9c94f', 'Bithumb 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x3052cd6bf951449a984fe4b5a38b46aef9455c8e', 'Bithumb 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2140efd7ba31169c69dfff6cdc66c542f0211825', 'Bithumb 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa0ff1e0f30b5dda2dc01e7e828290bc72b71e57d', 'Bithumb 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc1da8f69e4881efe341600620268934ef01a3e63', 'Bithumb 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb4460b75254ce0563bb68ec219208344c7ea838c', 'Bithumb 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x15878e87c685f866edfaf454be6dc06fa517b35b', 'Bithumb 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x31d03f07178bcd74f9099afebd23b0ae30184ab5', 'Bithumb 8', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xed48dc0628789c2956b1e41726d062a86ec45bff', 'Bithumb 9', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x186549a4ae594fc1f70ba4cffdac714b405be3f9', 'Bithumb 10', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd273bd546b11bd60214a2f9d71f22a088aafe31b', 'Bithumb 11', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x558553d54183a8542f7832742e7b4ba9c33aa1e6', 'Bithumb 12', 'institution', 'hildobby', 'static', timestamp('2023-01-26'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xbb5a0408fa54287b9074a2f47ab54c855e95ef82', 'Bithumb Old Address 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5521a68d4f8253fc44bfb1490249369b3e299a4a', 'Bithumb Old Address 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8fa8af91c675452200e49b4683a33ca2e1a34e42', 'Bithumb Old Address 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x3b83cd1a8e516b6eb9f1af992e9354b15a6f9672', 'Bithumb Old Address 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bitstamp, Source: https://etherscan.io/accounts/label/bitstamp
    , ('ethereum', '0x00bdb5699745f5b860228c8f939abf1b9ae374ed', 'Bitstamp 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x1522900b6dafac587d499a862861c0869be6e428', 'Bitstamp 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72', 'Bitstamp 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x059799f2261d37b829c2850cee67b5b975432271', 'Bitstamp 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4c766def136f59f6494f0969b1355882080cf8e0', 'Bitstamp 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f', 'Bitstamp 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfca70e67b3f93f679992cd36323eeb5a5370c8e4', 'Bitstamp Old Address 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- BitMEX, Source: https://etherscan.io/accounts/label/bitmex
    , ('ethereum', '0xeea81c4416d71cef071224611359f6f99a4c4294', 'BitMEX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfb8131c260749c7835a08ccbdb64728de432858e', 'BitMEX 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- HitBTC, Source: https://etherscan.io/accounts/label/hitbtc
    , ('ethereum', '0x9c67e141c0472115aa1b98bd0088418be68fd249', 'HitBTC 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x59a5208b32e627891c389ebafc644145224006e8', 'HitBTC 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa12431d0b9db640034b0cdfceef9cce161e62be4', 'HitBTC 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Luno
    , ('ethereum', '0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0', 'Luno 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0
    , ('ethereum', '0x416299aade6443e6f6e8ab67126e65a7f606eef5', 'Luno 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x416299aade6443e6f6e8ab67126e65a7f606eef5
    -- Poloniex, Source: https://etherscan.io/accounts/label/poloniex
    , ('ethereum', '0x32be343b94f860124dc4fee278fdcbd38c102d88', 'Poloniex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef', 'Poloniex 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb794f5ea0ba39494ce839613fffba74279579268', 'Poloniex 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xa910f92acdaf488fa6ef02174fb86208ad7722ba', 'Poloniex 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b', 'Poloniex BAT', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec', 'Poloniex BNT', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1', 'Poloniex CVC', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437', 'Poloniex FOAM', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df', 'Poloniex GNO', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0536806df512d6cdde913cf95c9886f65b1d3462', 'Poloniex GNT', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb', 'Poloniex KNC', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21', 'Poloniex LOOM', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb', 'Poloniex MANA', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0', 'Poloniex NXC', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x36b01066b7fa4a0fdb2968ea0256c848e9135674', 'Poloniex OMG', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5', 'Poloniex REP', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6795cf8eb25585eadc356ae32ac6641016c550f2', 'Poloniex SNT', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfbf2173154f7625713be22e0504404ebfe021eae', 'Poloniex STORJ', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x6f803466bcd17f44fa18975bf7c509ba64bf3825', 'Poloniex USDC', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xead6be34ce315940264519f250d8160f369fa5cd', 'Poloniex ZRX', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- WhiteBIT, Source: https://etherscan.io/address/0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3
    , ('ethereum', '0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3', 'WhiteBIT 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- DigiFinex, Source: https://etherscan.io/accounts/label/digifinex
    , ('ethereum', '0xe17ee7b3c676701c66b395a35f0df4c2276a344e', 'DigiFinex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- MEXC
    , ('ethereum', '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88', 'MEXC 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88
    , ('ethereum', '0x0211f3cedbef3143223d3acf0e589747933e8527', 'MEXC 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x0211f3cedbef3143223d3acf0e589747933e8527
    , ('ethereum', '0x3cc936b795a188f0e246cbb2d74c5bd190aecf18', 'MEXC 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x3cc936b795a188f0e246cbb2d74c5bd190aecf18
    -- Yobit
    , ('ethereum', '0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3', 'Yobit 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3
    -- Paribu, Source: https://etherscan.io/accounts/label/paribu
    , ('ethereum', '0xbd8ef191caa1571e8ad4619ae894e07a75de0c35', 'Paribu 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2bb97b6cf6ffe53576032c11711d59bd056830ee', 'Paribu 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfb90501083a3b6af766c8da35d3dde01eb0d2a68', 'Paribu 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xabc74170f3cb8ab352820c39cc1d1e05ce9e41d3', 'Paribu 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9acbb72cf67103a30333a32cd203459c6a9c3311', 'Paribu 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- RenrenBit
    , ('ethereum', '0x28c9386ebab8d52ead4a327e6423316435b2d4fc', 'RenrenBit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x28c9386ebab8d52ead4a327e6423316435b2d4fc
    -- Exmo
    , ('ethereum', '0x1fd6267f0d86f62d88172b998390afee2a1f54b6', 'Exmo 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x1fd6267f0d86f62d88172b998390afee2a1f54b6
    , ('ethereum', '0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32', 'Exmo 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32
    -- Remitano, Source: https://etherscan.io/accounts/label/remitano
    , ('ethereum', '0xb8cf411b956b3f9013c1d0ac8c909b086218207c', 'Remitano 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x2819c144d5946404c0516b6f817a960db37d4929', 'Remitano 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- WEX Exchange 
    , ('ethereum', '0xb3aaaae47070264f3595c5032ee94b620a583a39', 'WEX Exchange 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xb3aaaae47070264f3595c5032ee94b620a583a39
    -- Peatio
    , ('ethereum', '0xd4dcd2459bb78d7a645aa7e196857d421b10d93f', 'Peatio 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xd4dcd2459bb78d7a645aa7e196857d421b10d93f
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('ethereum', '0x274f3c32c90517975e29dfc209a23f315c1e5fc7', 'Hotbit 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x8533a0bd9310eb63e7cc8e1116c18a3d67b1976a', 'Hotbit 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x562680a4dc50ed2f14d75bf31f494cfe0b8d10a1', 'Hotbit 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- CoinEx
    , ('ethereum', '0xb9ee1e551f538a464e8f8c41e9904498505b49b0', 'CoinEx 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xb9ee1e551f538a464e8f8c41e9904498505b49b0
    , ('ethereum', '0x33ddd548fe3a082d753e5fe721a26e1ab43e3598', 'CoinEx 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x33ddd548fe3a082d753e5fe721a26e1ab43e3598
    -- CoinExchange
    , ('ethereum', '0x4b01721f0244e7c5b5f63c20942850e447f5a5ee', 'CoinExchange 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x4b01721f0244e7c5b5f63c20942850e447f5a5ee
    -- AscendEX (formerly BitMax), Source: https://etherscan.io/accounts/label/ascendex
    , ('ethereum', '0x03bdf69b1322d623836afbd27679a1c0afa067e9', 'AscendEX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x4b1a99467a284cc690e3237bc69105956816f762', 'AscendEX 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd', 'AscendEX 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Liquid, Source: https://etherscan.io/accounts/label/liquid
    , ('ethereum', '0xedbb72e6b3cf66a792bff7faac5ea769fe810517', 'Liquid 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdf4b6fb700c428476bd3c02e6fa83e110741145b', 'Liquid 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdb2cad4f306b47c9b35541988c7656f1bb092e15', 'Liquid 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9cc2dce817093ceea82bb67a4cf43131fa354c06', 'Liquid 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Tidex, Source: https://etherscan.io/accounts/label/tidex
    , ('ethereum', '0x3613ef1125a078ef96ffc898c4ec28d73c5b8c52', 'Tidex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0a73573cf2903d2d8305b1ecb9e9730186a312ae', 'Tidex 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- OTCBTC
    , ('ethereum', '0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8', 'OTCBTC 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8
    -- ShapeShift, Source: https://etherscan.io/accounts/label/shapeshift 
    , ('ethereum', '0x120a270bbc009644e35f0bb6ab13f95b8199c4ad', 'ShapeShift 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x9e6316f44baeeee5d41a1070516cc5fa47baf227', 'ShapeShift 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413', 'ShapeShift 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x563b377a956c80d77a7c613a9343699ad6123911', 'ShapeShift 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd3273eba07248020bf98a8b560ec1576a612102f', 'ShapeShift 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x3b0bc51ab9de1e5b7b6e34e5b960285805c41736', 'ShapeShift 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xeed16856d551569d134530ee3967ec79995e2051', 'ShapeShift 7', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb36efd48c9912bd9fd58b67b65f7438f6364a256', 'ShapeShift Binance Deposit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xda1e5d4cc9873963f788562354b55a772253b92f', 'ShapeShift Bitfinex Deposit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe9319eba87af7c2fc1f55ccde9d10ea8efbd592d', 'ShapeShift Bittrex Deposit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xe8ed915e208b28c617d20f3f8ca8e11455933adf', 'ShapeShift Poloniex Deposit', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- TopBTC, Source: https://etherscan.io/accounts/label/topbtc
    , ('ethereum', '0xb2cc3cdd53fc9a1aeaf3a68edeba2736238ddc5d', 'TopBTC 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Trade.io, Source: https://etherscan.io/accounts/label/trade-io
    , ('ethereum', '0x1119aaefb02bf12b84d28a5d8ea48ec3c90ef1db', 'Trade.io 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x58f75ddacffb183a30f69fe58a67a0d0985fce0f', 'Trade.io Wallet 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x5a2fad810f990c4535ada938400b6b67ef7646af', 'Trade.io Wallet 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Uex
    , ('ethereum', '0x2f1233ec3a4930fd95874291db7da9e90dfb2f03', 'Uex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x2f1233ec3a4930fd95874291db7da9e90dfb2f03
    -- Uphold
    , ('ethereum', '0x340d693ed55d7ba167d184ea76ea2fd092a35bdc', 'Uphold 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x340d693ed55d7ba167d184ea76ea2fd092a35bdc
    -- Kuna.io
    , ('ethereum', '0xea81ce54a0afa10a027f65503bd52fba83d745b8', 'Kuna.io 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xea81ce54a0afa10a027f65503bd52fba83d745b8
    , ('ethereum', '0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d', 'Kuna.io 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d
    -- Bit-Z
    , ('ethereum', '0x4b729cf402cfcffd057e254924b32241aedc1795', 'Bit-Z 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x4b729cf402cfcffd057e254924b32241aedc1795
    -- Bitzlato 
    , ('ethereum', '0x00cdc153aa8894d08207719fe921fff964f28ba3', 'Bitzlato 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x00cdc153aa8894d08207719fe921fff964f28ba3
    -- Cobinhood, Source: https://etherscan.io/accounts/label/cobinhood
    , ('ethereum', '0x8958618332df62af93053cb9c535e26462c959b0', 'Cobinhood 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xb726da4fbdc3e4dbda97bb20998cf899b0e727e0', 'Cobinhood 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0bb9fc3ba7bcf6e5d6f6fc15123ff8d5f96cee00', 'Cobinhood MultiSig', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Cashierest
    , ('ethereum', '0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3', 'Cashierest 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3
    -- Bibox
    , ('ethereum', '0xf73c3c65bde10bf26c2e1763104e609a41702efe', 'Bibox 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xf73c3c65bde10bf26c2e1763104e609a41702efe
    -- Coinhako, Source: https://etherscan.io/accounts/label/coinhako
    , ('ethereum', '0xd4bddf5e3d0435d7a6214a0b949c7bb58621f37c', 'Coinhako 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf2d4766ad705e3a5c9ba5b0436b473085f82f82f', 'Coinhako Hot Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bitberry
    , ('ethereum', '0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a', 'Bitberry 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a
    -- BigONE
    , ('ethereum', '0xa30d8157911ef23c46c0eb71889efe6a648a41f7', 'BigONE 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xa30d8157911ef23c46c0eb71889efe6a648a41f7
    -- Allbit, Source: https://etherscan.io/accounts/label/allbit
    , ('ethereum', '0xdc1882f350b42ac9a23508996254b1915c78b204', 'Allbit 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xff6b1cdfd2d3e37977d7938aa06b6d89d6675e27', 'Allbit 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- COSS, Source: https://etherscan.io/accounts/label/coss-io
    , ('ethereum', '0x0d6b5a54f940bf3d52e438cab785981aaefdf40c', 'COSS 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd1560b3984b7481cd9a8f40435a53c860187174d', 'COSS Old Hot Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x43f07efe28e092a0fe4ec5b5662022b461ffac80', 'COSS Hot Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- ABCC, Source: https://etherscan.io/accounts/label/abcc
    , ('ethereum', '0x05f51aab068caa6ab7eeb672f88c180f67f17ec7', 'ABCC 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- ATAIX
    , ('ethereum', '0x4df5f3610e2471095a130d7d934d551f3dde01ed', 'ATAIX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x4df5f3610e2471095a130d7d934d551f3dde01ed
    -- Beaxy
    , ('ethereum', '0xadb72986ead16bdbc99208086bd431c1aa38938e', 'Beaxy 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xadb72986ead16bdbc99208086bd431c1aa38938e
    -- Bgogo, Source: https://etherscan.io/accounts/label/bgogo
    , ('ethereum', '0x7a10ec7d68a048bdae36a70e93532d31423170fa', 'Bgogo 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xce1bf8e51f8b39e51c6184e059786d1c0eaf360f', 'Bgogo 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Bilaxy
    , ('ethereum', '0xf7793d27a1b76cdf14db7c83e82c772cf7c92910', 'Bilaxy 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xf7793d27a1b76cdf14db7c83e82c772cf7c92910
    , ('ethereum', '0xcce8d59affdd93be338fc77fa0a298c2cb65da59', 'Bilaxy 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xcce8d59affdd93be338fc77fa0a298c2cb65da59
    -- Bity
    , ('ethereum', '0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9', 'Bity', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9
    -- BW 
    , ('ethereum', '0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1', 'BW Old Address', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1
    , ('ethereum', '0x73957709695e73fd175582105c44743cf0fb6f2f', 'BW 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x73957709695e73fd175582105c44743cf0fb6f2f
    -- bitFlyer
    , ('ethereum', '0x111cff45948819988857bbf1966a0399e0d1141e', 'bitFlyer 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x111cff45948819988857bbf1966a0399e0d1141e
    -- Coinone
    , ('ethereum', '0x167a9333bf582556f35bd4d16a7e80e191aa6476', 'Coinone 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x167a9333bf582556f35bd4d16a7e80e191aa6476
    -- Bitkub
    , ('ethereum', '0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33', 'Bitkub Hot Wallet 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33
    , ('ethereum', '0x3d1d8a1d418220fd53c18744d44c182c46f47468', 'Bitkub Hot Wallet 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x3d1d8a1d418220fd53c18744d44c182c46f47468
    , ('ethereum', '0x59e0cda5922efba00a57794faf09bf6252d64126', 'Bitkub Hot Wallet 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x59e0cda5922efba00a57794faf09bf6252d64126
    , ('ethereum', '0x1579b5f6582c7a04f5ffeec683c13008c4b0a520', 'Bitkub Hot Wallet 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x1579b5f6582c7a04f5ffeec683c13008c4b0a520
    -- Indodax
    , ('ethereum', '0x51836a753e344257b361519e948ffcaf5fb8d521', 'Indodax 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x51836a753e344257b361519e948ffcaf5fb8d521
    , ('ethereum', '0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719', 'Indodax 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719
    -- MaiCoin
    , ('ethereum', '0x477b8d5ef7c2c42db84deb555419cd817c336b6f', 'MaiCoin 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x477b8d5ef7c2c42db84deb555419cd817c336b6f
    -- Bitfront
    , ('ethereum', '0xdf5021a4c1401f1125cd347e394d977630e17cf7', 'Bitfront 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xdf5021a4c1401f1125cd347e394d977630e17cf7
    -- Bit2C
    , ('ethereum', '0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a', 'Bit2C 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a
    -- FixedFloat
    , ('ethereum', '0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f', 'FixedFloat 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f
    -- Bitrue, Source: 
    , ('ethereum', '0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21', 'Bitrue 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21
    -- CoinMetro, Source: https://etherscan.io/accounts/label/coinmetro
    , ('ethereum', '0xa270f3ad1a7a82e6a3157f12a900f1e25bc4fbfd', 'CoinMetro 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x7c1c73bf60feb40cbcf0f12324200238ee23bb54', 'CoinMetro MultiSig', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xbac7c449689a2d3c51c386d8e657338c41ab3030', 'CoinMetro Treasury', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xf3e35734b7413f87c2054a16ce04230d803e4dc3', 'CoinMetro Vault Aug 2020', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xfad672dc92c2d2db0aa093331bd1098e30249ab8', 'CoinMetro Vault Feb 2020', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x165fe6a10812faa49515522d685a27c6bf12dba9', 'CoinMetro Vault Feb 2021', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xdd06b66c76d9c6fdc41935a7b32566c646325005', 'CoinMetro XCM Utility Vault', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- BlockTrades
    , ('ethereum', '0x007174732705604bbbf77038332dc52fd5a5000c', 'BlockTrades 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x007174732705604bbbf77038332dc52fd5a5000c
    -- Catex
    , ('ethereum', '0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f', 'Catex', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f
    -- Mercatox
    , ('ethereum', '0xe03c23519e18d64f144d2800e30e81b0065c48b5', 'Mercatox 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') --https://etherscan.io/address/0xe03c23519e18d64f144d2800e30e81b0065c48b5
    -- Sparrow 
    , ('ethereum', '0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5', 'Sparrow 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5
    , ('ethereum', '0xe855283086fbee485aecf2084345a91424c23954', 'Sparrow 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xe855283086fbee485aecf2084345a91424c23954
    -- DMEX
    , ('ethereum', '0x2101e480e22c953b37b9d0fe6551c1354fe705e6', 'DMEX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x2101e480e22c953b37b9d0fe6551c1354fe705e6
    -- BitBlinx
    , ('ethereum', '0x5d375281582791a38e0348915fa9cbc6139e9c2a', 'BitBlinx', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x5d375281582791a38e0348915fa9cbc6139e9c2a
    -- OMGFIN
    , ('ethereum', '0x03e3ff995863828554282e80870b489cc31dc8bc', 'OMGFIN', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x03e3ff995863828554282e80870b489cc31dc8bc
    -- CREX24, Source: https://etherscan.io/accounts/label/crex24
    , ('ethereum', '0x521db06bf657ed1d6c98553a70319a8ddbac75a3', 'CREX24 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Panda
    , ('ethereum', '0xcacc694840ecebadd9b4c419e5b7f1d73fedf999', 'Panda Exchange Hot Wallet 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xcacc694840ecebadd9b4c419e5b7f1d73fedf999
    , ('ethereum', '0xb709d82f0706476457ae6bad7c3534fbf424382c', 'Panda Exchange Hot Wallet 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xb709d82f0706476457ae6bad7c3534fbf424382c
    -- IDAX
    , ('ethereum', '0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7', 'IDAX', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7
    -- FlataExchange
    , ('ethereum', '0x14301566b9669b672878d86ff0b1d18dd58054e9', 'FlataExchange', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x14301566b9669b672878d86ff0b1d18dd58054e9
    -- XT.com Exchange
    , ('ethereum', '0xefda0cb780a8564903285ed25df3cc024f3b2982', 'XT.com Exchange 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xefda0cb780a8564903285ed25df3cc024f3b2982
    -- BitBase
    , ('ethereum', '0x0d8824ca76e627e9cc8227faa3b3993986ce9e48', 'BitBase 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x0d8824ca76e627e9cc8227faa3b3993986ce9e48
    , ('ethereum', '0x6dcd15a0dbefd0700063a4445382d3506391a41a', 'BitBase 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6dcd15a0dbefd0700063a4445382d3506391a41a
    -- KickEX
    , ('ethereum', '0x352bdabe484499e4c25c3536cc3eda1edbc5ad29', 'KickEX 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x352bdabe484499e4c25c3536cc3eda1edbc5ad29
    , ('ethereum', '0xaf4ff15c9809e246111802f04a6acc7160992fef', 'KickEX Hot Wallet 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xaf4ff15c9809e246111802f04a6acc7160992fef
    , ('ethereum', '0xc153121042832ac11587ebe361b8dc3ccd90e9e4', 'KickEX Cold Wallet', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xc153121042832ac11587ebe361b8dc3ccd90e9e4
    -- Coinbene
    , ('ethereum', '0x9539e0b14021a43cde41d9d45dc34969be9c7cb0', 'Coinbene 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x9539e0b14021a43cde41d9d45dc34969be9c7cb0
    , ('ethereum', '0x33683b94334eebc9bd3ea85ddbda4a86fb461405', 'Coinbene Cold Wallet 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x33683b94334eebc9bd3ea85ddbda4a86fb461405
    -- QuantaEx
    , ('ethereum', '0xd344539efe31f8b6de983a0cab4fb721fc69c547', 'QuantaEx 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xd344539efe31f8b6de983a0cab4fb721fc69c547
    , ('ethereum', '0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf', 'QuantaEx 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf
    , ('ethereum', '0x2a048d9a8ffdd239f063b09854976c3049ae659c', 'QuantaEx 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x2a048d9a8ffdd239f063b09854976c3049ae659c
    -- Yunbi, Source: https://etherscan.io/accounts/label/yunbi
    , ('ethereum', '0xd94c9ff168dc6aebf9b6cc86deff54f3fb0afc33', 'Yunbi 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x42da8a05cb7ed9a43572b5ba1b8f82a0a6e263dc', 'Yunbi 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x700f6912e5753e91ea3fae877a2374a2db1245d7', 'Yunbi 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- CoinW
    , ('ethereum', '0x8705ccfd8a6df3785217c307cbebf9b793310b94', 'CoinW 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x8705ccfd8a6df3785217c307cbebf9b793310b94
    , ('ethereum', '0xcb243bf48fb443082fae7db47ec96cb120cd6801', 'CoinW 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xcb243bf48fb443082fae7db47ec96cb120cd6801
    , ('ethereum', '0x429bf8ec3330e02401d72beade86000d9a2e19eb', 'CoinW 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x429bf8ec3330e02401d72beade86000d9a2e19eb
    , ('ethereum', '0x6f31d347457962c9811ff953742870ef5a755de3', 'CoinW 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6f31d347457962c9811ff953742870ef5a755de3
    -- Cryptopia
    , ('ethereum', '0x5baeac0a0417a05733884852aa068b706967e790', 'Cryptopia 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x5baeac0a0417a05733884852aa068b706967e790
    , ('ethereum', '0x2984581ece53a4390d1f568673cf693139c97049', 'Cryptopia 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x2984581ece53a4390d1f568673cf693139c97049
    -- CoinDhan
    , ('ethereum', '0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9', 'CoinDhan 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9
    -- BIKI
    , ('ethereum', '0x6eff3372fa352b239bb24ff91b423a572347000d', 'BIKI 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6eff3372fa352b239bb24ff91b423a572347000d
    , ('ethereum', '0x6efb20f61b80f6a7ebe7a107bace58288a51fb34', 'BIKI Old Address', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x6efb20f61b80f6a7ebe7a107bace58288a51fb34
    -- Liqui
    , ('ethereum', '0x8271b2e8cbe29396e9563229030c89679b9470db', 'Liqui 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x8271b2e8cbe29396e9563229030c89679b9470db
    , ('ethereum', '0x5e575279bf9f4acf0a130c186861454247394c06', 'Liqui 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x5e575279bf9f4acf0a130c186861454247394c06
    -- Hoo.com, Source: https://etherscan.io/accounts/label/hoo-com
    , ('ethereum', '0x980a4732c8855ffc8112e6746bd62095b4c2228f', 'Hoo.com 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xd0ec209ad2134899148bec8aef905a6e9997456a', 'Hoo.com 2', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x993b7fcba51d8f75c2dfaec0d17b6649ee0c9068', 'Hoo.com 3', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0xec293b9c56f06c8f71392269313d7e2da681d9ac', 'Hoo.com 4', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x0093e5f2a850268c0ca3093c7ea53731296487eb', 'Hoo.com 5', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    , ('ethereum', '0x008932be50098089c6a075d35f4b5182ee549f8a', 'Hoo.com 6', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier')
    -- Beldex
    , ('ethereum', '0x258b7b9a1ba92f47f5f4f5e733293477620a82cb', 'Beldex 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x258b7b9a1ba92f47f5f4f5e733293477620a82cb
    -- SouthXchange
    , ('ethereum', '0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4', 'SouthXchange 1', 'institution', 'hildobby', 'static', timestamp('2022-08-28'), now(), 'cex_ethereum', 'identifier') -- https://etherscan.io/address/0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4     
     
     ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
