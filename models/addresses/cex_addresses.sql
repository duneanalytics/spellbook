{{config(schema='cex', alias='addresses')}}

SELECT address, cex_name, distinct_name
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    ('0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance', 'Binance 1')
    , ('0xd551234ae421e3bcba99a0da6d736074f22192ff', 'Binance', 'Binance 2')
    , ('0x564286362092d8e7936f0549571a803b203aaced', 'Binance', 'Binance 3')
    , ('0x0681d8db095565fe8a346fa0277bffde9c0edbbf', 'Binance', 'Binance 4')
    , ('0xfe9e8709d3215310075d67e3ed32a380ccf451c8', 'Binance', 'Binance 5')
    , ('0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67', 'Binance', 'Binance 6')
    , ('0xbe0eb53f46cd790cd13851d5eff43d12404d33e8', 'Binance', 'Binance 7')
    , ('0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance', 'Binance 8')
    , ('0x001866ae5b3de6caa5a51543fd9fb64f524f5478', 'Binance', 'Binance 9')
    , ('0x85b931a32a0725be14285b66f1a22178c672d69b', 'Binance', 'Binance 10')
    , ('0x708396f17127c42383e3b9014072679b2f60b82f', 'Binance', 'Binance 11')
    , ('0xe0f0cfde7ee664943906f17f7f14342e76a5cec7', 'Binance', 'Binance 12')
    , ('0x8f22f2063d253846b53609231ed80fa571bc0c8f', 'Binance', 'Binance 13')
    , ('0x28c6c06298d514db089934071355e5743bf21d60', 'Binance', 'Binance 14')
    , ('0x21a31ee1afc51d94c2efccaa2092ad1028285549', 'Binance', 'Binance 15')
    , ('0xdfd5293d8e347dfe59e90efd55b2956a1343963d', 'Binance', 'Binance 16')
    , ('0x56eddb7aa87536c09ccc2793473599fd21a8b17f', 'Binance', 'Binance 17')
    , ('0x9696f59e4d72e237be84ffd425dcad154bf96976', 'Binance', 'Binance 18')
    , ('0x4d9ff50ef4da947364bb9650892b2554e7be5e2b', 'Binance', 'Binance 19')
    , ('0x4976a4a02f38326660d17bf34b431dc6e2eb2327', 'Binance', 'Binance 20')
    , ('0xd88b55467f58af508dbfdc597e8ebd2ad2de49b3', 'Binance', 'Binance 21')
    , ('0x7dfe9a368b6cf0c0309b763bb8d16da326e8f46e', 'Binance', 'Binance 22')
    , ('0x345d8e3a1f62ee6b1d483890976fd66168e390f2', 'Binance', 'Binance 23')
    , ('0xc3c8e0a39769e2308869f7461364ca48155d1d9e', 'Binance', 'Binance 24')
    , ('0x2e581a5ae722207aa59acd3939771e7c7052dd3d', 'Binance', 'Binance 25')
    , ('0x44592b81c05b4c35efb8424eb9d62538b949ebbf', 'Binance', 'Binance 26')
    , ('0x06a0048079ec6571cd1b537418869cde6191d42d', 'Binance', 'Binance 29')
    , ('0x892e9e24aea3f27f4c6e9360e312cce93cc98ebe', 'Binance', 'Binance 30')
    , ('0x00799bbc833d5b168f0410312d2a8fd9e0e3079c', 'Binance', 'Binance 31')
    , ('0x141fef8cd8397a390afe94846c8bd6f4ab981c48', 'Binance', 'Binance 32')
    , ('0x50d669f43b484166680ecc3670e4766cdb0945ce', 'Binance', 'Binance 33')
    , ('0x2f7e209e0f5f645c7612d7610193fe268f118b28', 'Binance', 'Binance 34')
    , ('0x8b99f3660622e21f2910ecca7fbe51d654a1517d', 'Binance', 'Binance Charity')
    , ('0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4', 'Binance', 'Binance JEX')
    , ('0xc365c3315cf926351ccaf13fa7d19c8c4058c8e1', 'Binance', 'Binance Pool')
    , ('0x61189da79177950a7272c88c6058b96d4bcd6be2', 'Binance', 'Binance US')
    , ('0x4fabb145d64652a948d72533023f6e7a623c7c53', 'Binance', 'Binance USD')
    , ('0x2f47a1c2db4a3b78cda44eade915c3b19107ddcc', 'Binance', 'Binance Eth2 Depositor')
    , ('0xb3f923eabaf178fc1bd8e13902fc5c61d3ddef5b', 'Binance', 'Wintermute Binance Deposit')
    -- FTX, Source: https://etherscan.io/accounts/label/ftx
    , ('0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2', 'FTX', 'FTX 1')
    , ('0xc098b2a3aa256d2140208c3de6543aaef5cd3a94', 'FTX', 'FTX 2')
    -- Coinbase, Source: https://etherscan.io/accounts/label/coinbase
    , ('0x71660c4005ba85c37ccec55d0c4493e66fe775d3', 'Coinbase', 'Coinbase 1')
    , ('0x503828976d22510aad0201ac7ec88293211d23da', 'Coinbase', 'Coinbase 2')
    , ('0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740', 'Coinbase', 'Coinbase 3')
    , ('0x3cd751e6b0078be393132286c442345e5dc49699', 'Coinbase', 'Coinbase 4')
    , ('0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511', 'Coinbase', 'Coinbase 5')
    , ('0xeb2629a2734e272bcc07bda959863f316f4bd4cf', 'Coinbase', 'Coinbase 6')
    , ('0xa090e606e30bd747d4e6245a1517ebe430f0057e', 'Coinbase', 'Coinbase Miscellaneous')
    , ('0xf6874c88757721a02f47592140905c4336dfbc61', 'Coinbase', 'Coinbase Commerce')
    , ('0x881d4032abe4188e2237efcd27ab435e81fc6bb1', 'Coinbase', 'Coinbase Commerce 2')
    -- OKX, Source: https://etherscan.io/accounts/label/okex
    , ('0x6cc5f688a315f3dc28a7781717a9a798a59fda7b', 'OKX', 'OKX')
    , ('0x236f9f97e0e62388479bf9e5ba4889e46b0273c3', 'OKX', 'OKX 2')
    , ('0xa7efae728d2936e78bda97dc267687568dd593f3', 'OKX', 'OKX 3')
    , ('0x2c8fbb630289363ac80705a1a61273f76fd5a161', 'OKX', 'OKX 4')
    , ('0x59fae149a8f8ec74d5bc038f8b76d25b136b9573', 'OKX', 'OKX 5')
    , ('0x98ec059dc3adfbdd63429454aeb0c990fba4a128', 'OKX', 'OKX 6')
    , ('0x5041ed759dd4afc3a72b8192c143f72f4724081a', 'OKX', 'OKX 7')
    -- Huobi, Source: https://etherscan.io/accounts/label/huobi
    , ('0xab5c66752a9e8167967685f1450532fb96d5d24f', 'Huobi', 'Huobi 1')
    , ('0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b', 'Huobi', 'Huobi 2')
    , ('0xfdb16996831753d5331ff813c29a93c76834a0ad', 'Huobi', 'Huobi 3')
    , ('0xeee28d484628d41a82d01e21d12e2e78d69920da', 'Huobi', 'Huobi 4')
    , ('0x5c985e89dde482efe97ea9f1950ad149eb73829b', 'Huobi', 'Huobi 5')
    , ('0xdc76cd25977e0a5ae17155770273ad58648900d3', 'Huobi', 'Huobi 6')
    , ('0xadb2b42f6bd96f5c65920b9ac88619dce4166f94', 'Huobi', 'Huobi 7')
    , ('0xa8660c8ffd6d578f657b72c0c811284aef0b735e', 'Huobi', 'Huobi 8')
    , ('0x1062a747393198f70f71ec65a582423dba7e5ab3', 'Huobi', 'Huobi 9')
    , ('0xe93381fb4c4f14bda253907b18fad305d799241a', 'Huobi', 'Huobi 10')
    , ('0xfa4b5be3f2f84f56703c42eb22142744e95a2c58', 'Huobi', 'Huobi 11')
    , ('0x46705dfff24256421a05d056c29e81bdc09723b8', 'Huobi', 'Huobi 12')
    , ('0x32598293906b5b17c27d657db3ad2c9b3f3e4265', 'Huobi', 'Huobi 13')
    , ('0x5861b8446a2f6e19a067874c133f04c578928727', 'Huobi', 'Huobi 14')
    , ('0x926fc576b7facf6ae2d08ee2d4734c134a743988', 'Huobi', 'Huobi 15')
    , ('0xeec606a66edb6f497662ea31b5eb1610da87ab5f', 'Huobi', 'Huobi 16')
    , ('0x7ef35bb398e0416b81b019fea395219b65c52164', 'Huobi', 'Huobi 17')
    , ('0x229b5c097f9b35009ca1321ad2034d4b3d5070f6', 'Huobi', 'Huobi 18')
    , ('0xd8a83b72377476d0a66683cde20a8aad0b628713', 'Huobi', 'Huobi 19')
    , ('0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325', 'Huobi', 'Huobi 20')
    , ('0x18916e1a2933cb349145a280473a5de8eb6630cb', 'Huobi', 'Huobi 21')
    , ('0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380', 'Huobi', 'Huobi 22')
    , ('0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94', 'Huobi', 'Huobi 23')
    , ('0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5', 'Huobi', 'Huobi 24')
    , ('0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e', 'Huobi', 'Huobi 25')
    , ('0x034f854b44d28e26386c1bc37ff9b20c6380b00d', 'Huobi', 'Huobi 26')
    , ('0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404', 'Huobi', 'Huobi 27')
    , ('0x0c6c34cdd915845376fb5407e0895196c9dd4eec', 'Huobi', 'Huobi 28')
    , ('0x794d28ac31bcb136294761a556b68d2634094153', 'Huobi', 'Huobi 29')
    , ('0xfd54078badd5653571726c3370afb127351a6f26', 'Huobi', 'Huobi 30')
    , ('0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9', 'Huobi', 'Huobi 31')
    , ('0x4d77a1144dc74f26838b69391a6d3b1e403d0990', 'Huobi', 'Huobi 32')
    , ('0x28ffe35688ffffd0659aee2e34778b0ae4e193ad', 'Huobi', 'Huobi 33')
    , ('0xcac725bef4f114f728cbcfd744a731c2a463c3fc', 'Huobi', 'Huobi 34')
    , ('0x73f8fc2e74302eb2efda125a326655acf0dc2d1b', 'Huobi', 'Huobi 35')
    , ('0x0a98fb70939162725ae66e626fe4b52cff62c2e5', 'Huobi', 'Huobi 36')
    , ('0xf66852bc122fd40bfecc63cd48217e88bda12109', 'Huobi', 'Huobi 37')
    , ('0x49517ca7b7a50f592886d4c74175f4c07d460a70', 'Huobi', 'Huobi 38')
    , ('0x58c2cb4a6bee98c309215d0d2a38d7f8aa71211c', 'Huobi', 'Huobi 39')
    , ('0x1d1e10e8c66b67692f4c002c0cb334de5d485e41', 'Huobi', 'Huobi Old Address 1')
    , ('0x1b93129f05cc2e840135aab154223c75097b69bf', 'Huobi', 'Huobi Old Address 2')
    , ('0xeb6d43fe241fb2320b5a3c9be9cdfd4dd8226451', 'Huobi', 'Huobi Old Address 3')
    , ('0x956e0dbecc0e873d34a5e39b25f364b2ca036730', 'Huobi', 'Huobi Old Address 4')
    , ('0x6f50c6bff08ec925232937b204b0ae23c488402a', 'Huobi', 'Huobi Old Address 5')
    , ('0xdf95de30cdff4381b69f9e4fa8dddce31a0128df', 'Huobi', 'Huobi Old Address 6')
    , ('0x25c6459e5c5b01694f6453e8961420ccd1edf3b1', 'Huobi', 'Huobi Old Address 7')
    , ('0x04645af26b54bd85dc02ac65054e87362a72cb22', 'Huobi', 'Huobi Old Address 8')
    , ('0xb2a48f542dc56b89b24c04076cbe565b3dc58e7b', 'Huobi', 'Huobi Old Address 9')
    , ('0xea0cfef143182d7b9208fbfeda9d172c2aced972', 'Huobi', 'Huobi Old Address 10')
    , ('0x0c92efa186074ba716d0e2156a6ffabd579f8035', 'Huobi', 'Huobi Old Address 11')
    , ('0x91dfa9d9e062a50d2f351bfba0d35a9604993dac', 'Huobi', 'Huobi Old Address 12')
    , ('0x8e8bc99b79488c276d6f3ca11901e9abd77efea4', 'Huobi', 'Huobi Old Address 13')
    , ('0xb9a4873d8d2c22e56b8574e8605644d08e047549', 'Huobi', 'Huobi Old Address 14')
    , ('0x170af0a02339743687afd3dc8d48cffd1f660728', 'Huobi', 'Huobi Old Address 15')
    , ('0xf775a9a0ad44807bc15936df0ee68902af1a0eee', 'Huobi', 'Huobi Old Address 16')
    , ('0x75a83599de596cbc91a1821ffa618c40e22ac8ca', 'Huobi', 'Huobi Old Address 17')
    , ('0x48ab9f29795dfb44b36587c50da4b30c0e84d3ed', 'Huobi', 'Huobi Old Address 18')
    , ('0x90f49e24a9554126f591d28174e157ca267194ba', 'Huobi', 'Huobi Old Address 19')
    , ('0xe3314bbf3334228b257779e28228cfb86fa4261b', 'Huobi', 'Huobi Old Address 20')
    , ('0x6edb9d6547befc3397801c94bb8c97d2e8087e2f', 'Huobi', 'Huobi Old Address 21')
    , ('0x8aabba0077f1565df73e9d15dd3784a2b0033dad', 'Huobi', 'Huobi Old Address 22')
    , ('0xd3a2f775e973c1671f2047e620448b8662dcd3ca', 'Huobi', 'Huobi Old Address 23')
    , ('0x1c515eaa87568c850043a89c2d2c2e8187adb056', 'Huobi', 'Huobi Old Address 24')
    , ('0x60b45f993223dcb8bdf05e3391f7630e5a51d787', 'Huobi', 'Huobi Old Address 25')
    , ('0xa23d7dd4b8a1060344caf18a29b42350852af481', 'Huobi', 'Huobi Old Address 26')
    , ('0x9eebb2815dba2166d8287afa9a2c89336ba9deaa', 'Huobi', 'Huobi Old Address 27')
    , ('0xd10e08325c0e95d59c607a693483680fe5b755b3', 'Huobi', 'Huobi Old Address 28')
    , ('0xc837f51a0efa33f8eca03570e3d01a4b2cf97ffd', 'Huobi', 'Huobi Old Address 29')
    , ('0xf7a8af16acb302351d7ea26ffc380575b741724c', 'Huobi', 'Huobi Old Address 30')
    , ('0x636b76ae213358b9867591299e5c62b8d014e372', 'Huobi', 'Huobi Old Address 31')
    , ('0x9a755332d874c893111207b0b220ce2615cd036f', 'Huobi', 'Huobi Old Address 32')
    , ('0xecd8b3877d8e7cd0739de18a5b545bc0b3538566', 'Huobi', 'Huobi Old Address 33')
    , ('0xef54f559b5e3b55b783c7bc59850f83514b6149c', 'Huobi', 'Huobi Old Address 34')
    , ('0x9d6d492bd500da5b33cf95a5d610a73360fcaaa0', 'Huobi', 'Huobi Mining Pool')
    -- Gate.io, Source: https://etherscan.io/accounts/label/gate-io
    , ('0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io', 'Gate.io 1')
    , ('0x7793cd85c11a924478d358d49b05b37e91b5810f', 'Gate.io', 'Gate.io 2')
    , ('0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io', 'Gate.io 3')
    -- Kraken, Source: https://etherscan.io/accounts/label/kraken
    , ('0x2910543af39aba0cd09dbb2d50200b3e800a63d2', 'Kraken', 'Kraken 1')
    , ('0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13', 'Kraken', 'Kraken 2')
    , ('0xe853c56864a2ebe4576a807d26fdc4a0ada51919', 'Kraken', 'Kraken 3')
    , ('0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0', 'Kraken', 'Kraken 4')
    , ('0xfa52274dd61e1643d2205169732f29114bc240b3', 'Kraken', 'Kraken 5')
    , ('0x53d284357ec70ce289d6d64134dfac8e511c8a3d', 'Kraken', 'Kraken 6')
    , ('0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40', 'Kraken', 'Kraken 7')
    , ('0xc6bed363b30df7f35b601a5547fe56cd31ec63da', 'Kraken', 'Kraken 8')
    , ('0x29728d0efd284d85187362faa2d4d76c2cfc2612', 'Kraken', 'Kraken 9')
    , ('0xae2d4617c862309a3d75a0ffb358c7a5009c673f', 'Kraken', 'Kraken 10')
    , ('0x43984d578803891dfa9706bdeee6078d80cfc79e', 'Kraken', 'Kraken 11')
    , ('0x66c57bf505a85a74609d2c83e94aabb26d691e1f', 'Kraken', 'Kraken 12')
    , ('0xda9dfa130df4de4673b89022ee50ff26f6ea73cf', 'Kraken', 'Kraken 13')
    , ('0xe9f7ecae3a53d2a67105292894676b00d1fab785', 'Kraken', 'Kraken Hot Wallet')
    -- Bitfinex, Source: https://etherscan.io/accounts/label/bitfinex
    , ('0x1151314c646ce4e0efd76d1af4760ae66a9fe30f', 'Bitfinex', 'Bitfinex 1')
    , ('0x742d35cc6634c0532925a3b844bc454e4438f44e', 'Bitfinex', 'Bitfinex 2')
    , ('0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 'Bitfinex', 'Bitfinex 3')
    , ('0xdcd0272462140d0a3ced6c4bf970c7641f08cd2c', 'Bitfinex', 'Bitfinex 4')
    , ('0x4fdd5eb2fb260149a3903859043e962ab89d8ed4', 'Bitfinex', 'Bitfinex 5')
    , ('0x1b29dd8ff0eb3240238bf97cafd6edea05d5ba82', 'Bitfinex', 'Bitfinex 6')
    , ('0x30a2ebf10f34c6c4874b0bdd5740690fd2f3b70c', 'Bitfinex', 'Bitfinex 7')
    , ('0x3f7e77b627676763997344a1ad71acb765fc8ac5', 'Bitfinex', 'Bitfinex 8')
    , ('0x59448fe20378357f206880c58068f095ae63d5a5', 'Bitfinex', 'Bitfinex 9')
    , ('0x36a85757645e8e8aec062a1dee289c7d615901ca', 'Bitfinex', 'Bitfinex 10')
    , ('0xc56fefd1028b0534bfadcdb580d3519b5586246e', 'Bitfinex', 'Bitfinex 11')
    , ('0x0b73f67a49273fc4b9a65dbd25d7d0918e734e63', 'Bitfinex', 'Bitfinex 12')
    , ('0x482f02e8bc15b5eabc52c6497b425b3ca3c821e8', 'Bitfinex', 'Bitfinex 13')
    , ('0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e', 'Bitfinex', 'Bitfinex MultiSig 1')
    , ('0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828', 'Bitfinex', 'Bitfinex MultiSig 2')
    , ('0xc61b9bb3a7a0767e3179713f3a5c7a9aedce193c', 'Bitfinex', 'Bitfinex MultiSig 3')
    , ('0xcafb10ee663f465f9d10588ac44ed20ed608c11e', 'Bitfinex', 'Bitfinex Old Address 1')
    , ('0x7180eb39a6264938fdb3effd7341c4727c382153', 'Bitfinex', 'Bitfinex Old Address 2')
    , ('0x5754284f345afc66a98fbb0a0afe71e0f007b949', 'Bitfinex', 'Tether Treasury')
    -- KuCoin, Source: https://etherscan.io/accounts/label/kucoin
    , ('0x2b5634c42055806a59e9107ed44d43c426e58258', 'KuCoin', 'KuCoin 1')
    , ('0x689c56aef474df92d44a1b70850f808488f9769c', 'KuCoin', 'KuCoin 2')
    , ('0xa1d8d972560c2f8144af871db508f0b0b10a3fbf', 'KuCoin', 'KuCoin 3')
    , ('0x4ad64983349c49defe8d7a4686202d24b25d0ce8', 'KuCoin', 'KuCoin 4')
    , ('0x1692e170361cefd1eb7240ec13d048fd9af6d667', 'KuCoin', 'KuCoin 5')
    , ('0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin', 'KuCoin 6')
    , ('0xe59cd29be3be4461d79c0881d238cbe87d64595a', 'KuCoin', 'KuCoin 7')
    , ('0x899b5d52671830f567bf43a14684eb14e1f945fe', 'KuCoin', 'KuCoin 8')
    , ('0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91', 'KuCoin', 'KuCoin 9')
    , ('0xcad621da75a66c7a8f4ff86d30a2bf981bfc8fdd', 'KuCoin', 'KuCoin 10')
    -- Crypto.com, Source: https://etherscan.io/accounts/label/crypto-com
    , ('0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com', 'Crypto.com 1')
    , ('0x46340b20830761efd32832a74d7169b29feb9758', 'Crypto.com', 'Crypto.com 2')
    -- Gemini, Source: https://etherscan.io/accounts/label/gemini
    , ('0xd24400ae8bfebb18ca49be86258a3c749cf46853', 'Gemini', 'Gemini 1')
    , ('0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8', 'Gemini', 'Gemini 2')
    , ('0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea', 'Gemini', 'Gemini 3')
    , ('0x5f65f7b609678448494de4c87521cdf6cef1e932', 'Gemini', 'Gemini 4')
    , ('0xb302bfe9c246c6e150af70b1caaa5e3df60dac05', 'Gemini', 'Gemini 5')
    , ('0x8d6f396d210d385033b348bcae9e4f9ea4e045bd', 'Gemini', 'Gemini 6')
    , ('0xd69b0089d9ca950640f5dc9931a41a5965f00303', 'Gemini', 'Gemini 7')
    -- BitMart, Source: https://etherscan.io/accounts/label/bitmart
    , ('0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1', 'BitMart', 'BitMart 1')
    , ('0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7', 'BitMart', 'BitMart 2')
    -- LATOKEN, Source: https://etherscan.io/accounts/label/latoken
    , ('0x0861fca546225fbf8806986d211c8398f7457734', 'LAToken', 'LAToken 1')
    , ('0x7891b20c690605f4e370d6944c8a5dbfac5a451c', 'LAToken', 'LAToken 2')
    , ('0x1b6c1a0e20af81b922cb454c3e52408496ee7201', 'LAToken', 'LAToken 3')
    -- Upbit, Source: https://etherscan.io/accounts/label/upbit
    , ('0x390de26d772d2e2005c6d1d24afc902bae37a4bb', 'Upbit', 'Upbit 1')
    , ('0xba826fec90cefdf6706858e5fbafcb27a290fbe0', 'Upbit', 'Upbit 2')
    , ('0x5e032243d507c743b061ef021e2ec7fcc6d3ab89', 'Upbit', 'Upbit 3')
    , ('0xc9cf0ec93d764f5c9571fd12f764bae7fc87c84e', 'Upbit', 'Upbit Cold Wallet')
    -- Bittrex, Source: https://etherscan.io/accounts/label/bittrex
    , ('0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98', 'Bittrex', 'Bittrex 1')
    , ('0xe94b04a0fed112f3664e45adb2b8915693dd5ff3', 'Bittrex', 'Bittrex 2')
    , ('0x66f820a414680b5bcda5eeca5dea238543f42054', 'Bittrex', 'Bittrex 3')
    -- Bithumb, Source: https://etherscan.io/accounts/label/bithumb
    , ('0x88d34944cf554e9cccf4a24292d891f620e9c94f', 'Bithumb', 'Bithumb 1')
    , ('0x186549a4ae594fc1f70ba4cffdac714b405be3f9', 'Bithumb', 'Bithumb 10')
    , ('0xd273bd546b11bd60214a2f9d71f22a088aafe31b', 'Bithumb', 'Bithumb 11')
    , ('0x558553d54183a8542f7832742e7b4ba9c33aa1e6', 'Bithumb', 'Bithumb 12')
    , ('0x3052cd6bf951449a984fe4b5a38b46aef9455c8e', 'Bithumb', 'Bithumb 2')
    , ('0x2140efd7ba31169c69dfff6cdc66c542f0211825', 'Bithumb', 'Bithumb 3')
    , ('0xa0ff1e0f30b5dda2dc01e7e828290bc72b71e57d', 'Bithumb', 'Bithumb 4')
    , ('0xc1da8f69e4881efe341600620268934ef01a3e63', 'Bithumb', 'Bithumb 5')
    , ('0xb4460b75254ce0563bb68ec219208344c7ea838c', 'Bithumb', 'Bithumb 6')
    , ('0x15878e87c685f866edfaf454be6dc06fa517b35b', 'Bithumb', 'Bithumb 7')
    , ('0x31d03f07178bcd74f9099afebd23b0ae30184ab5', 'Bithumb', 'Bithumb 8')
    , ('0xed48dc0628789c2956b1e41726d062a86ec45bff', 'Bithumb', 'Bithumb 9')
    , ('0xbb5a0408fa54287b9074a2f47ab54c855e95ef82', 'Bithumb', 'Bithumb Old Address 1')
    , ('0x5521a68d4f8253fc44bfb1490249369b3e299a4a', 'Bithumb', 'Bithumb Old Address 2')
    , ('0x8fa8af91c675452200e49b4683a33ca2e1a34e42', 'Bithumb', 'Bithumb Old Address 3')
    , ('0x3b83cd1a8e516b6eb9f1af992e9354b15a6f9672', 'Bithumb', 'Bithumb Old Address 4')
    -- Bitstamp, Source: https://etherscan.io/accounts/label/bitstamp
    , ('0x00bdb5699745f5b860228c8f939abf1b9ae374ed', 'Bitstamp', 'Bitstamp 1')
    , ('0x1522900b6dafac587d499a862861c0869be6e428', 'Bitstamp', 'Bitstamp 2')
    , ('0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72', 'Bitstamp', 'Bitstamp 3')
    , ('0x059799f2261d37b829c2850cee67b5b975432271', 'Bitstamp', 'Bitstamp 4')
    , ('0x4c766def136f59f6494f0969b1355882080cf8e0', 'Bitstamp', 'Bitstamp 5')
    , ('0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f', 'Bitstamp', 'Bitstamp 6')
    , ('0xfca70e67b3f93f679992cd36323eeb5a5370c8e4', 'Bitstamp', 'Bitstamp Old Address 1')
    -- BitMEX, Source: https://etherscan.io/accounts/label/bitmex
    , ('0xeea81c4416d71cef071224611359f6f99a4c4294', 'BitMEX', 'BitMEX 1')
    , ('0xfb8131c260749c7835a08ccbdb64728de432858e', 'BitMEX', 'BitMEX 2')
    -- HitBTC, Source: https://etherscan.io/accounts/label/hitbtc
    , ('0x9c67e141c0472115aa1b98bd0088418be68fd249', 'HitBTC', 'HitBTC 1')
    , ('0x59a5208b32e627891c389ebafc644145224006e8', 'HitBTC', 'HitBTC 2')
    , ('0xa12431d0b9db640034b0cdfceef9cce161e62be4', 'HitBTC', 'HitBTC 3')
    -- Luno
    , ('0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0', 'Luno', 'Luno 1') -- https://etherscan.io/address/0xaf1931c20ee0c11bea17a41bfbbad299b2763bc0
    , ('0x416299aade6443e6f6e8ab67126e65a7f606eef5', 'Luno', 'Luno 2') -- https://etherscan.io/address/0x416299aade6443e6f6e8ab67126e65a7f606eef5
    -- Poloniex, Source: https://etherscan.io/accounts/label/poloniex
    , ('0x32be343b94f860124dc4fee278fdcbd38c102d88', 'Poloniex', 'Poloniex')
    , ('0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef', 'Poloniex', 'Poloniex 2')
    , ('0xb794f5ea0ba39494ce839613fffba74279579268', 'Poloniex', 'Poloniex 3')
    , ('0xa910f92acdaf488fa6ef02174fb86208ad7722ba', 'Poloniex', 'Poloniex 4')
    , ('0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b', 'Poloniex', 'Poloniex BAT')
    , ('0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec', 'Poloniex', 'Poloniex BNT')
    , ('0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1', 'Poloniex', 'Poloniex CVC')
    , ('0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437', 'Poloniex', 'Poloniex FOAM')
    , ('0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df', 'Poloniex', 'Poloniex GNO')
    , ('0x0536806df512d6cdde913cf95c9886f65b1d3462', 'Poloniex', 'Poloniex GNT')
    , ('0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb', 'Poloniex', 'Poloniex KNC')
    , ('0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21', 'Poloniex', 'Poloniex LOOM')
    , ('0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb', 'Poloniex', 'Poloniex MANA')
    , ('0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0', 'Poloniex', 'Poloniex NXC')
    , ('0x36b01066b7fa4a0fdb2968ea0256c848e9135674', 'Poloniex', 'Poloniex OMG')
    , ('0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5', 'Poloniex', 'Poloniex REP')
    , ('0x6795cf8eb25585eadc356ae32ac6641016c550f2', 'Poloniex', 'Poloniex SNT')
    , ('0xfbf2173154f7625713be22e0504404ebfe021eae', 'Poloniex', 'Poloniex STORJ')
    , ('0x6f803466bcd17f44fa18975bf7c509ba64bf3825', 'Poloniex', 'Poloniex USDC')
    , ('0xead6be34ce315940264519f250d8160f369fa5cd', 'Poloniex', 'Poloniex ZRX')
    -- WhiteBIT, Source: https://etherscan.io/address/0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3
    , ('0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3', 'WhiteBIT', 'WhiteBIT 1')
    -- DigiFinex, Source: https://etherscan.io/accounts/label/digifinex
    , ('0xe17ee7b3c676701c66b395a35f0df4c2276a344e', 'DigiFinex', 'DigiFinex 1')
    -- MEXC
    , ('0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88', 'MEXC', 'MEXC 1') -- https://etherscan.io/address/0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88
    , ('0x0211f3cedbef3143223d3acf0e589747933e8527', 'MEXC', 'MEXC 2') -- https://etherscan.io/address/0x0211f3cedbef3143223d3acf0e589747933e8527
    , ('0x3cc936b795a188f0e246cbb2d74c5bd190aecf18', 'MEXC', 'MEXC 3') -- https://etherscan.io/address/0x3cc936b795a188f0e246cbb2d74c5bd190aecf18
    -- Yobit
    , ('0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3', 'Yobit', 'Yobit 1') -- https://etherscan.io/address/0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3
    -- Paribu, Source: https://etherscan.io/accounts/label/paribu
    , ('0xbd8ef191caa1571e8ad4619ae894e07a75de0c35', 'Paribu', 'Paribu 1')
    , ('0x2bb97b6cf6ffe53576032c11711d59bd056830ee', 'Paribu', 'Paribu 2')
    , ('0xfb90501083a3b6af766c8da35d3dde01eb0d2a68', 'Paribu', 'Paribu 3')
    , ('0xabc74170f3cb8ab352820c39cc1d1e05ce9e41d3', 'Paribu', 'Paribu 4')
    , ('0x9acbb72cf67103a30333a32cd203459c6a9c3311', 'Paribu', 'Paribu 5')
    -- RenrenBit
    , ('0x28c9386ebab8d52ead4a327e6423316435b2d4fc', 'RenrenBit', 'RenrenBit') -- https://etherscan.io/address/0x28c9386ebab8d52ead4a327e6423316435b2d4fc
    -- Exmo
    , ('0x1fd6267f0d86f62d88172b998390afee2a1f54b6', 'Exmo', 'Exmo 1') -- https://etherscan.io/address/0x1fd6267f0d86f62d88172b998390afee2a1f54b6
    , ('0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32', 'Exmo', 'Exmo 2') -- https://etherscan.io/address/0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32
    -- Remitano, Source: https://etherscan.io/accounts/label/remitano
    , ('0xb8cf411b956b3f9013c1d0ac8c909b086218207c', 'Remitano', 'Remitano 1')
    , ('0x2819c144d5946404c0516b6f817a960db37d4929', 'Remitano', 'Remitano 2')
    -- WEX Exchange 
    , ('0xb3aaaae47070264f3595c5032ee94b620a583a39', 'WEX Exchange', 'WEX Exchange 1') -- https://etherscan.io/address/0xb3aaaae47070264f3595c5032ee94b620a583a39
    -- Peatio
    , ('0xd4dcd2459bb78d7a645aa7e196857d421b10d93f', 'Peatio', 'Peatio 1') -- https://etherscan.io/address/0xd4dcd2459bb78d7a645aa7e196857d421b10d93f
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('0x274f3c32c90517975e29dfc209a23f315c1e5fc7', 'Hotbit', 'Hotbit 1')
    , ('0x8533a0bd9310eb63e7cc8e1116c18a3d67b1976a', 'Hotbit', 'Hotbit 2')
    , ('0x562680a4dc50ed2f14d75bf31f494cfe0b8d10a1', 'Hotbit', 'Hotbit 3')
    -- CoinEx
    , ('0xb9ee1e551f538a464e8f8c41e9904498505b49b0', 'CoinEx', 'CoinEx 1') -- https://etherscan.io/address/0xb9ee1e551f538a464e8f8c41e9904498505b49b0
    , ('0x33ddd548fe3a082d753e5fe721a26e1ab43e3598', 'CoinEx', 'CoinEx 2') -- https://etherscan.io/address/0x33ddd548fe3a082d753e5fe721a26e1ab43e3598
    -- CoinExchange
    , ('0x4b01721f0244e7c5b5f63c20942850e447f5a5ee', 'CoinExchange', 'CoinExchange 1') -- https://etherscan.io/address/0x4b01721f0244e7c5b5f63c20942850e447f5a5ee
    -- AscendEX (formerly BitMax), Source: https://etherscan.io/accounts/label/ascendex
    , ('0x03bdf69b1322d623836afbd27679a1c0afa067e9', 'AscendEX', 'AscendEX 1')
    , ('0x4b1a99467a284cc690e3237bc69105956816f762', 'AscendEX', 'AscendEX 2')
    , ('0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd', 'AscendEX', 'AscendEX 3')
    -- Liquid, Source: https://etherscan.io/accounts/label/liquid
    , ('0xedbb72e6b3cf66a792bff7faac5ea769fe810517', 'Liquid', 'Liquid 1')
    , ('0xdf4b6fb700c428476bd3c02e6fa83e110741145b', 'Liquid', 'Liquid 2')
    , ('0xdb2cad4f306b47c9b35541988c7656f1bb092e15', 'Liquid', 'Liquid 3')
    , ('0x9cc2dce817093ceea82bb67a4cf43131fa354c06', 'Liquid', 'Liquid 4')
    -- Tidex, Source: https://etherscan.io/accounts/label/tidex
    , ('0x3613ef1125a078ef96ffc898c4ec28d73c5b8c52', 'Tidex', 'Tidex 1')
    , ('0x0a73573cf2903d2d8305b1ecb9e9730186a312ae', 'Tidex', 'Tidex 2')
    -- OTCBTC
    , ('0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8', 'OTCBTC', 'OTCBTC 1') -- https://etherscan.io/address/0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8
    -- ShapeShift, Source: https://etherscan.io/accounts/label/shapeshift 
    , ('0x120a270bbc009644e35f0bb6ab13f95b8199c4ad', 'ShapeShift', 'ShapeShift 1')
    , ('0x9e6316f44baeeee5d41a1070516cc5fa47baf227', 'ShapeShift', 'ShapeShift 2')
    , ('0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413', 'ShapeShift', 'ShapeShift 3')
    , ('0x563b377a956c80d77a7c613a9343699ad6123911', 'ShapeShift', 'ShapeShift 4')
    , ('0xd3273eba07248020bf98a8b560ec1576a612102f', 'ShapeShift', 'ShapeShift 5')
    , ('0x3b0bc51ab9de1e5b7b6e34e5b960285805c41736', 'ShapeShift', 'ShapeShift 6')
    , ('0xeed16856d551569d134530ee3967ec79995e2051', 'ShapeShift', 'ShapeShift 7')
    , ('0xb36efd48c9912bd9fd58b67b65f7438f6364a256', 'ShapeShift', 'ShapeShift Binance Deposit')
    , ('0xda1e5d4cc9873963f788562354b55a772253b92f', 'ShapeShift', 'ShapeShift Bitfinex Deposit')
    , ('0xe9319eba87af7c2fc1f55ccde9d10ea8efbd592d', 'ShapeShift', 'ShapeShift Bittrex Deposit')
    , ('0xe8ed915e208b28c617d20f3f8ca8e11455933adf', 'ShapeShift', 'ShapeShift Poloniex Deposit')
    -- TopBTC, Source: https://etherscan.io/accounts/label/topbtc
    , ('0xb2cc3cdd53fc9a1aeaf3a68edeba2736238ddc5d', 'TopBTC', 'TopBTC 1')
    -- Trade.io, Source: https://etherscan.io/accounts/label/trade-io
    , ('0x1119aaefb02bf12b84d28a5d8ea48ec3c90ef1db', 'Trade.io', 'Trade.io 1')
    , ('0x58f75ddacffb183a30f69fe58a67a0d0985fce0f', 'Trade.io', 'Trade.io Wallet 1')
    , ('0x5a2fad810f990c4535ada938400b6b67ef7646af', 'Trade.io', 'Trade.io Wallet 2')
    -- Uex
    , ('0x2f1233ec3a4930fd95874291db7da9e90dfb2f03', 'Uex', 'Uex 1') -- https://etherscan.io/address/0x2f1233ec3a4930fd95874291db7da9e90dfb2f03
    -- Uphold
    , ('0x340d693ed55d7ba167d184ea76ea2fd092a35bdc', 'Uphold', 'Uphold 1') -- https://etherscan.io/address/0x340d693ed55d7ba167d184ea76ea2fd092a35bdc
    -- Kuna.io
    , ('0xea81ce54a0afa10a027f65503bd52fba83d745b8', 'Kuna.io', 'Kuna.io 1') -- https://etherscan.io/address/0xea81ce54a0afa10a027f65503bd52fba83d745b8
    , ('0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d', 'Kuna.io', 'Kuna.io 2') -- https://etherscan.io/address/0x77ab999d1e9f152156b4411e1f3e2a42dab8cd6d
    -- Bit-Z
    , ('0x4b729cf402cfcffd057e254924b32241aedc1795', 'Bit-Z', 'Bit-Z 1') -- https://etherscan.io/address/0x4b729cf402cfcffd057e254924b32241aedc1795
    -- Bitzlato 
    , ('0x00cdc153aa8894d08207719fe921fff964f28ba3', 'Bitzlato', 'Bitzlato 1') -- https://etherscan.io/address/0x00cdc153aa8894d08207719fe921fff964f28ba3
    -- Cobinhood, Source: https://etherscan.io/accounts/label/cobinhood
    , ('0x8958618332df62af93053cb9c535e26462c959b0', 'Cobinhood', 'Cobinhood 1')
    , ('0xb726da4fbdc3e4dbda97bb20998cf899b0e727e0', 'Cobinhood', 'Cobinhood 2')
    , ('0x0bb9fc3ba7bcf6e5d6f6fc15123ff8d5f96cee00', 'Cobinhood', 'Cobinhood MultiSig')
    -- Cashierest
    , ('0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3', 'Cashierest', 'Cashierest 1') -- https://etherscan.io/address/0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3
    -- Bibox
    , ('0xf73c3c65bde10bf26c2e1763104e609a41702efe', 'Bibox', 'Bibox 1') -- https://etherscan.io/address/0xf73c3c65bde10bf26c2e1763104e609a41702efe
    -- Coinhako, Source: https://etherscan.io/accounts/label/coinhako
    , ('0xd4bddf5e3d0435d7a6214a0b949c7bb58621f37c', 'Coinhako', 'Coinhako 1')
    , ('0xf2d4766ad705e3a5c9ba5b0436b473085f82f82f', 'Coinhako', 'Coinhako Hot Wallet')
    -- Bitberry
    , ('0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a', 'Bitberry', 'Bitberry 1') -- https://etherscan.io/address/0x6b59210ade46b62b25e82e95ab390a7ccadd4c3a
    -- BigONE
    , ('0xa30d8157911ef23c46c0eb71889efe6a648a41f7', 'BigONE', 'BigONE 1') -- https://etherscan.io/address/0xa30d8157911ef23c46c0eb71889efe6a648a41f7
    -- Allbit, Source: https://etherscan.io/accounts/label/allbit
    , ('0xdc1882f350b42ac9a23508996254b1915c78b204', 'Allbit', 'Allbit 1')
    , ('0xff6b1cdfd2d3e37977d7938aa06b6d89d6675e27', 'Allbit', 'Allbit 2')
    -- COSS, Source: https://etherscan.io/accounts/label/coss-io
    , ('0x0d6b5a54f940bf3d52e438cab785981aaefdf40c', 'COSS', 'COSS 1')
    , ('0xd1560b3984b7481cd9a8f40435a53c860187174d', 'COSS', 'COSS Old Hot Wallet')
    , ('0x43f07efe28e092a0fe4ec5b5662022b461ffac80', 'COSS', 'COSS Hot Wallet')
    -- ABCC, Source: https://etherscan.io/accounts/label/abcc
    , ('0x05f51aab068caa6ab7eeb672f88c180f67f17ec7', 'ABCC', 'ABCC 1')
    -- ATAIX
    , ('0x4df5f3610e2471095a130d7d934d551f3dde01ed', 'ATAIX', 'ATAIX 1') -- https://etherscan.io/address/0x4df5f3610e2471095a130d7d934d551f3dde01ed
    -- Beaxy
    , ('0xadb72986ead16bdbc99208086bd431c1aa38938e', 'Beaxy', 'Beaxy 1') -- https://etherscan.io/address/0xadb72986ead16bdbc99208086bd431c1aa38938e
    -- Bgogo, Source: https://etherscan.io/accounts/label/bgogo
    , ('0x7a10ec7d68a048bdae36a70e93532d31423170fa', 'Bgogo', 'Bgogo 1')
    , ('0xce1bf8e51f8b39e51c6184e059786d1c0eaf360f', 'Bgogo', 'Bgogo 2')
    -- Bilaxy
    , ('0xf7793d27a1b76cdf14db7c83e82c772cf7c92910', 'Bilaxy', 'Bilaxy 1') -- https://etherscan.io/address/0xf7793d27a1b76cdf14db7c83e82c772cf7c92910
    , ('0xcce8d59affdd93be338fc77fa0a298c2cb65da59', 'Bilaxy', 'Bilaxy 2') -- https://etherscan.io/address/0xcce8d59affdd93be338fc77fa0a298c2cb65da59
    -- Bity
    , ('0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9', 'Bity', 'Bity') -- https://etherscan.io/address/0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9
    -- BW 
    , ('0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1', 'BW', 'BW Old Address') -- https://etherscan.io/address/0xbcdfc35b86bedf72f0cda046a3c16829a2ef41d1
    , ('0x73957709695e73fd175582105c44743cf0fb6f2f', 'BW', 'BW 1') -- https://etherscan.io/address/0x73957709695e73fd175582105c44743cf0fb6f2f
    -- Blockfolio 
    , ('0x25eaff5b179f209cf186b1cdcbfa463a69df4c45', 'Blockfolio', 'Blockfolio') -- https://etherscan.io/address/0x25eaff5b179f209cf186b1cdcbfa463a69df4c45
    -- bitFlyer
    , ('0x111cff45948819988857bbf1966a0399e0d1141e', 'bitFlyer', 'bitFlyer 1') -- https://etherscan.io/address/0x111cff45948819988857bbf1966a0399e0d1141e
    -- Coinone
    , ('0x167a9333bf582556f35bd4d16a7e80e191aa6476', 'Coinone', 'Coinone 1') -- https://etherscan.io/address/0x167a9333bf582556f35bd4d16a7e80e191aa6476
    -- Bitkub
    , ('0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33', 'Bitkub', 'Bitkub Hot Wallet 1') -- https://etherscan.io/address/0xdb044b8298e04d442fdbe5ce01b8cc8f77130e33
    , ('0x3d1d8a1d418220fd53c18744d44c182c46f47468', 'Bitkub', 'Bitkub Hot Wallet 2') -- https://etherscan.io/address/0x3d1d8a1d418220fd53c18744d44c182c46f47468
    , ('0x59e0cda5922efba00a57794faf09bf6252d64126', 'Bitkub', 'Bitkub Hot Wallet 3') -- https://etherscan.io/address/0x59e0cda5922efba00a57794faf09bf6252d64126
    , ('0x1579b5f6582c7a04f5ffeec683c13008c4b0a520', 'Bitkub', 'Bitkub Hot Wallet 4') -- https://etherscan.io/address/0x1579b5f6582c7a04f5ffeec683c13008c4b0a520
    -- Indodax
    , ('0x51836a753e344257b361519e948ffcaf5fb8d521', 'Indodax', 'Indodax 1') -- https://etherscan.io/address/0x51836a753e344257b361519e948ffcaf5fb8d521
    , ('0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719', 'Indodax', 'Indodax 2') -- https://etherscan.io/address/0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719
    -- MaiCoin
    , ('0x477b8d5ef7c2c42db84deb555419cd817c336b6f', 'MaiCoin', 'MaiCoin 1') -- https://etherscan.io/address/0x477b8d5ef7c2c42db84deb555419cd817c336b6f
    -- Bitfront
    , ('0xdf5021a4c1401f1125cd347e394d977630e17cf7', 'Bitfront', 'Bitfront 1') -- https://etherscan.io/address/0xdf5021a4c1401f1125cd347e394d977630e17cf7
    -- Bit2C
    , ('0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a', 'Bit2C', 'Bit2C 1') -- https://etherscan.io/address/0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a
    -- FixedFloat
    , ('0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f', 'FixedFloat', 'FixedFloat 1') -- https://etherscan.io/address/0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f
    -- Bitrue, Source: 
    , ('0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21', 'Bitrue', 'Bitrue 1') -- https://etherscan.io/address/0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21
    -- CoinMetro, Source: https://etherscan.io/accounts/label/coinmetro
    , ('0xa270f3ad1a7a82e6a3157f12a900f1e25bc4fbfd', 'CoinMetro', 'CoinMetro 1')
    , ('0x7c1c73bf60feb40cbcf0f12324200238ee23bb54', 'CoinMetro', 'CoinMetro MultiSig')
    , ('0xbac7c449689a2d3c51c386d8e657338c41ab3030', 'CoinMetro', 'CoinMetro Treasury')
    , ('0xf3e35734b7413f87c2054a16ce04230d803e4dc3', 'CoinMetro', 'CoinMetro Vault Aug 2020')
    , ('0xfad672dc92c2d2db0aa093331bd1098e30249ab8', 'CoinMetro', 'CoinMetro Vault Feb 2020')
    , ('0x165fe6a10812faa49515522d685a27c6bf12dba9', 'CoinMetro', 'CoinMetro Vault Feb 2021')
    , ('0xdd06b66c76d9c6fdc41935a7b32566c646325005', 'CoinMetro', 'CoinMetro XCM Utility Vault')
    -- BlockTrades
    , ('0x007174732705604bbbf77038332dc52fd5a5000c', 'BlockTrades', 'BlockTrades 1') -- https://etherscan.io/address/0x007174732705604bbbf77038332dc52fd5a5000c
    -- Catex
    , ('0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f', 'Catex', 'Catex') -- https://etherscan.io/address/0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f
    -- Mercatox
    , ('0xe03c23519e18d64f144d2800e30e81b0065c48b5', 'Mercatox', 'Mercatox 1') --https://etherscan.io/address/0xe03c23519e18d64f144d2800e30e81b0065c48b5
    -- Sparrow 
    , ('0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5', 'Sparrow', 'Sparrow 1') -- https://etherscan.io/address/0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5
    , ('0xe855283086fbee485aecf2084345a91424c23954', 'Sparrow', 'Sparrow 2') -- https://etherscan.io/address/0xe855283086fbee485aecf2084345a91424c23954
    -- DMEX
    , ('0x2101e480e22c953b37b9d0fe6551c1354fe705e6', 'DMEX', 'DMEX 1') -- https://etherscan.io/address/0x2101e480e22c953b37b9d0fe6551c1354fe705e6
    -- BitBlinx
    , ('0x5d375281582791a38e0348915fa9cbc6139e9c2a', 'BitBlinx', 'BitBlinx') -- https://etherscan.io/address/0x5d375281582791a38e0348915fa9cbc6139e9c2a
    -- OMGFIN
    , ('0x03e3ff995863828554282e80870b489cc31dc8bc', 'OMGFIN', 'OMGFIN') -- https://etherscan.io/address/0x03e3ff995863828554282e80870b489cc31dc8bc
    -- CREX24, Source: https://etherscan.io/accounts/label/crex24
    , ('0x521db06bf657ed1d6c98553a70319a8ddbac75a3', 'CREX24', 'CREX24 1')
    -- Panda
    , ('0xcacc694840ecebadd9b4c419e5b7f1d73fedf999', 'Panda Exchange', 'Panda Exchange Hot Wallet 1') -- https://etherscan.io/address/0xcacc694840ecebadd9b4c419e5b7f1d73fedf999
    , ('0xb709d82f0706476457ae6bad7c3534fbf424382c', 'Panda Exchange', 'Panda Exchange Hot Wallet 2') -- https://etherscan.io/address/0xb709d82f0706476457ae6bad7c3534fbf424382c
    -- IDAX
    , ('0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7', 'IDAX', 'IDAX') -- https://etherscan.io/address/0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7
    -- FlataExchange
    , ('0x14301566b9669b672878d86ff0b1d18dd58054e9', 'FlataExchange', 'FlataExchange') -- https://etherscan.io/address/0x14301566b9669b672878d86ff0b1d18dd58054e9
    -- XT.com Exchange
    , ('0xefda0cb780a8564903285ed25df3cc024f3b2982', 'XT.com Exchange', 'XT.com Exchange 1') -- https://etherscan.io/address/0xefda0cb780a8564903285ed25df3cc024f3b2982
    -- BitBase
    , ('0x0d8824ca76e627e9cc8227faa3b3993986ce9e48', 'BitBase', 'BitBase 1') -- https://etherscan.io/address/0x0d8824ca76e627e9cc8227faa3b3993986ce9e48
    , ('0x6dcd15a0dbefd0700063a4445382d3506391a41a', 'BitBase', 'BitBase 2') -- https://etherscan.io/address/0x6dcd15a0dbefd0700063a4445382d3506391a41a
    -- KickEX
    , ('0x352bdabe484499e4c25c3536cc3eda1edbc5ad29', 'KickEX', 'KickEX 1') -- https://etherscan.io/address/0x352bdabe484499e4c25c3536cc3eda1edbc5ad29
    , ('0xaf4ff15c9809e246111802f04a6acc7160992fef', 'KickEX', 'KickEX Hot Wallet 1') -- https://etherscan.io/address/0xaf4ff15c9809e246111802f04a6acc7160992fef
    , ('0xc153121042832ac11587ebe361b8dc3ccd90e9e4', 'KickEX', 'KickEX Cold Wallet') -- https://etherscan.io/address/0xc153121042832ac11587ebe361b8dc3ccd90e9e4
    -- Coinbene
    , ('0x9539e0b14021a43cde41d9d45dc34969be9c7cb0', 'Coinbene', 'Coinbene 1') -- https://etherscan.io/address/0x9539e0b14021a43cde41d9d45dc34969be9c7cb0
    , ('0x33683b94334eebc9bd3ea85ddbda4a86fb461405', 'Coinbene', 'Coinbene Cold Wallet 1') -- https://etherscan.io/address/0x33683b94334eebc9bd3ea85ddbda4a86fb461405
    -- QuantaEx
    , ('0xd344539efe31f8b6de983a0cab4fb721fc69c547', 'QuantaEx', 'QuantaEx 1') -- https://etherscan.io/address/0xd344539efe31f8b6de983a0cab4fb721fc69c547
    , ('0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf', 'QuantaEx', 'QuantaEx 2') -- https://etherscan.io/address/0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf
    , ('0x2a048d9a8ffdd239f063b09854976c3049ae659c', 'QuantaEx', 'QuantaEx 3') -- https://etherscan.io/address/0x2a048d9a8ffdd239f063b09854976c3049ae659c
    -- Yunbi, Source: https://etherscan.io/accounts/label/yunbi
    , ('0xd94c9ff168dc6aebf9b6cc86deff54f3fb0afc33', 'Yunbi', 'Yunbi 1')
    , ('0x42da8a05cb7ed9a43572b5ba1b8f82a0a6e263dc', 'Yunbi', 'Yunbi 2')
    , ('0x700f6912e5753e91ea3fae877a2374a2db1245d7', 'Yunbi', 'Yunbi 3')
    -- CoinW
    , ('0x8705ccfd8a6df3785217c307cbebf9b793310b94', 'CoinW', 'CoinW 1') -- https://etherscan.io/address/0x8705ccfd8a6df3785217c307cbebf9b793310b94
    , ('0xcb243bf48fb443082fae7db47ec96cb120cd6801', 'CoinW', 'CoinW 2') -- https://etherscan.io/address/0xcb243bf48fb443082fae7db47ec96cb120cd6801
    , ('0x429bf8ec3330e02401d72beade86000d9a2e19eb', 'CoinW', 'CoinW 3') -- https://etherscan.io/address/0x429bf8ec3330e02401d72beade86000d9a2e19eb
    , ('0x6f31d347457962c9811ff953742870ef5a755de3', 'CoinW', 'CoinW 4') -- https://etherscan.io/address/0x6f31d347457962c9811ff953742870ef5a755de3
    -- Cryptopia
    , ('0x5baeac0a0417a05733884852aa068b706967e790', 'Cryptopia', 'Cryptopia 1') -- https://etherscan.io/address/0x5baeac0a0417a05733884852aa068b706967e790
    , ('0x2984581ece53a4390d1f568673cf693139c97049', 'Cryptopia', 'Cryptopia 2') -- https://etherscan.io/address/0x2984581ece53a4390d1f568673cf693139c97049
    -- CoinDhan
    , ('0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9', 'CoinDhan', 'CoinDhan 1') -- https://etherscan.io/address/0xbf1a97d8d4229d61b031214d5bbe9a5cb1e737f9
    -- BIKI
    , ('0x6eff3372fa352b239bb24ff91b423a572347000d', 'BIKI', 'BIKI 1') -- https://etherscan.io/address/0x6eff3372fa352b239bb24ff91b423a572347000d
    , ('0x6efb20f61b80f6a7ebe7a107bace58288a51fb34', 'BIKI', 'BIKI Old Address') -- https://etherscan.io/address/0x6efb20f61b80f6a7ebe7a107bace58288a51fb34
    -- Liqui
    , ('0x8271b2e8cbe29396e9563229030c89679b9470db', 'Liqui', 'Liqui 1') -- https://etherscan.io/address/0x8271b2e8cbe29396e9563229030c89679b9470db
    , ('0x5e575279bf9f4acf0a130c186861454247394c06', 'Liqui', 'Liqui 2') -- https://etherscan.io/address/0x5e575279bf9f4acf0a130c186861454247394c06
    -- Hoo.com, Source: https://etherscan.io/accounts/label/hoo-com
    , ('0x980a4732c8855ffc8112e6746bd62095b4c2228f', 'Hoo.com', 'Hoo.com 1')
    , ('0xd0ec209ad2134899148bec8aef905a6e9997456a', 'Hoo.com', 'Hoo.com 2')
    , ('0x993b7fcba51d8f75c2dfaec0d17b6649ee0c9068', 'Hoo.com', 'Hoo.com 3')
    , ('0xec293b9c56f06c8f71392269313d7e2da681d9ac', 'Hoo.com', 'Hoo.com 4')
    , ('0x0093e5f2a850268c0ca3093c7ea53731296487eb', 'Hoo.com', 'Hoo.com 5')
    , ('0x008932be50098089c6a075d35f4b5182ee549f8a', 'Hoo.com', 'Hoo.com 6')
    -- Beldex
    , ('0x258b7b9a1ba92f47f5f4f5e733293477620a82cb', 'Beldex', 'Beldex 1') -- https://etherscan.io/address/0x258b7b9a1ba92f47f5f4f5e733293477620a82cb
    -- SouthXchange
    , ('0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4', 'SouthXchange', 'SouthXchange 1') -- https://etherscan.io/address/0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4
    ) AS x (address, cex_name, distinct_name)