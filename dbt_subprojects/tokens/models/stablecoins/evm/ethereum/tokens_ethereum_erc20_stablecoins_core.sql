{% set chain = 'ethereum' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_ethereum_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x6b175474e89094c44da98b954eedeac495271d0f, 'USD'), -- DAI
     (0x96f6ef951840721adbf46ac996b59e0235cb985c, 'USD'), -- USDY
     (0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'USD'), -- FDUSD
     (0x8d6cebd76f18e1558d4db88138e2defb3909fad6, 'USD'), -- MAI
     (0x5f98805a4e8be255a32880fdec7f6728c6568ba0, 'USD'), -- LUSD
     (0x45fdb1b92a649fb6a64ef1511d3ba5bf60044838, 'USD'), -- USDS
     (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USD'), -- USDC
     (0xdac17f958d2ee523a2206206994597c13d831ec7, 'USD'), -- USDT
     (0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab, 'USD'), -- BEAN
     (0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'USD'), -- mUSD
     (0x7712c34205737192402172409a8f7ccef8aa2aec, 'USD'), -- BUIDL
     (0x4fabb145d64652a948d72533023f6e7a623c7c53, 'USD'), -- BUSD
     (0x1456688345527be1f37e9e627da0837d6f08c925, 'USD'), -- USDP
     (0x2a8e1e676ec238d8a992307b495b45b3feaa5e86, 'USD'), -- OUSD
     (0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6, 'USD'), -- USDD (old)
     (0x3D7975EcCFc61a2102b08925CbBa0a4D4dBB6555, 'USD'), -- USDD
     (0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'USD'), -- USDP
     (0x853d955acef822db058eb8505911ed77f175b99e, 'USD'), -- FRAX
     (0x6c3ea9036406852006290770bedfcaba0e23a0e8, 'USD'), -- PYUSD
     (0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'USD'), -- HUSD
     (0xc285b7e09a4584d027e5bc36571785b515898246, 'USD'), -- CUSD
     (0xdc59ac4fefa32293a95889dc396682858d52e5db, 'USD'), -- BEAN
     (0xa774ffb4af6b0a91331c084e1aebae6ad535e6f3, 'USD'), -- FLEXUSD
     (0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'USD'), -- USTC
     (0x0000000000085d4780b73119b644ae5ecd22b376, 'USD'), -- TUSD
     (0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 'USD'), -- USDe
     (0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa, 'USD'), -- ZUSD
     (0x0a5e677a6a24b2f1a2bf4f3bffc443231d2fdec8, 'USD'), -- USX
     (0x196f4727526ea7fb1e17b2071b3d8eaa38486988, 'USD'), -- RSV
     (0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'USD'), -- alUSD
     (0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5, 'USD'), -- USD0
     (0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'USD'), -- MIM
     (0x57ab1ec28d129707052df4df418d58a2d46d5f51, 'USD'), -- sUSD
     (0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'USD'), -- GUSD
     (0x1c48f86ae57291f7686349f12601910bd8d470bb, 'USD'), -- USDK
     (0x865377367054516e17014ccded1e7d814edc9ce4, 'USD'), -- DOLA
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'USD'), -- BOB
     (0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'USD'), -- FEI
     (0x9a1997c130f4b2997166975d9aff92797d5134c2, 'USD'), -- USDap
     (0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'USD'), -- AMPL
     (0x03ab458634910aad20ef5f1c8ee96f1d6ac54919, 'USD'), -- RAI
     (0x674c6ad92fd080e4004b2312b45f796a192d27a0, 'USD'), -- USDN
     (0x866a2bf4e572cbcf37d5071a7a58503bfb36be1b, 'USD'), -- M
     (0x57Ab1E02fEE23774580C119740129eAC7081e9D3, 'USD'), -- sUSD
     (0xa693b19d2931d498c5b318df961919bb4aee87a5, 'USD'), -- UST
     (0xe07F9D810a48ab5c3c914BA3cA53AF14E4491e8A, 'USD'), -- GYD
     (0xdb25f211ab05b1c97d595516f45794528a807ad8, 'EUR'), -- EURS
     (0x15f74458ae0bfdaa1a96ca1aa779d715cc1eefe4, 'USD'), -- GRAI
     (0xf939e0a03fb07f59a73314e73794be0e57ac1b4e, 'USD'), -- crvUSD
     (0xab5eb14c09d416f0ac63661e57edb7aecdb9befa, 'USD'), -- msUSD
     (0x15700b564ca08d9439c58ca5053166e8317aa138, 'USD'), -- deUSD
     (0x085780639cc2cacd35e474e71f4d000e2405d8f6, 'USD'), -- fxUSD
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a, 'USD'), -- AUSD
     (0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f, 'USD'), -- eUSD
     (0xa469b7ee9ee773642b3e93e842e5d9b5baa10067, 'USD'), -- USDz
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'USD'), -- USDM
     (0x79c58f70905f734641735bc61e45c19dd9ad60bc, 'USD'), -- usdc-dai-usdt
     (0x0000206329b97db379d5e1bf586bbdb969c63274, 'USD'), -- USDA
     (0x35d8949372d46b7a3d5a56006ae77b215fc69bc0, 'USD'), -- USD0++
     (0x4591dbff62656e7859afe5e45f6f47d3669fbb28, 'USD'), -- mkUSD
     (0x0bffdd787c83235f6f0afa0faed42061a4619b7a, 'USD'), -- VUSD
     (0xcfc5bd99915aaa815401c5a41a927ab7a38d29cf, 'USD'), -- thUSD
     (0xdf3ac4f479375802a821f7b7b46cd7eb5e4262cc, 'USD'), -- eUSD
     (0xbbaec992fc2d637151daf40451f160bf85f3c8c1, 'USD'), -- USDM
     (0x7945b0a6674b175695e5d1d08ae1e6f13744abb0, 'USD'), -- BaoUSD
     (0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f, 'USD'), -- GHO
     (0xfd03723a9a3abe0562451496a9a394d2c4bad4ab, 'USD'), -- DYAD
     (0xdc035d45d973e3ec169d2276ddab16f1e407384f, 'USD'), -- USDS
     (0x7B43E3875440B44613DC3bC08E7763e6Da63C8f8, 'USD'), -- USDR
     (0x50753CfAf86c094925Bf976f218D043f8791e408, 'EUR'), -- EURR
     (0x2c537e5624e4af88a7ae4060c022609376c8d0eb, 'TRY'), -- TRYb
     (0x4cCe605eD955295432958d8951D0B176C10720d5, 'AUD'), -- AUDD
     (0x7751e2f4b8ae93ef6b79d86419d42fe3295a4559, 'USD'), -- wUSDL
     (0xbdc7c08592ee4aa51d06c27ee23d5087d65adbcd, 'USD'), -- USDL
     (0xcacd6fd266af91b8aed52accc382b4e165586e29, 'USD'), -- frxUSD
     (0xc581b735a1688071a1746c968e0798d642ede491, 'EUR'), -- EURT
     (0x70e8de73ce538da2beed35d14187f6959a8eca96, 'SGD'), -- XSGD
     (0xed03ed872159e199065401b6d0d487d78d9464aa, 'MXN'), -- MXNT
     (0x1abaea1f7c830bd89acc67ec4af516284b1bc33c, 'EUR'), -- EURC
     (0x09d4214c03d01f49544c0448dbe3a27f768f2b34, 'USD'), -- rUSD
     (0x8a60e489004ca22d775c5f2c657598278d17d9c2, 'USD'), -- USDa
     (0x8292bb45bf1ee4d140127049757c2e0ff06317ed, 'USD'), -- RLUSD
     (0x66a1e37c9b0eaddca17d3662d6c05f4decf3e110, 'USD'), -- USR
     (0x1a7e4e63778b4f12a199c062f3efdd288afcbce8, 'EUR'), -- EURA
     (0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98, 'USD'), -- BOLD
     (0x6e109e9dd7fa1a58bc3eff667e8e41fc3cc07aef, 'CNY'), -- CNHT
     (0x4a16baf414b8e637ed12019fad5dd705735db2e0, 'CAD'), -- QCAD
     (0xcadc0acd4b445166f12d2c07eac6e2544fbe2eef, 'CAD'), -- CADC
     (0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'USD'), -- wUSDM
     (0x7c1156e515aa1a2e851674120074968c905aaf37, 'USD'), -- lvlUSD
     (0xb58e61c3098d85632df34eecfb899a1ed80921cb, 'CHF'), -- ZCHF
     (0x57ab1e0003f623289cd798b1824be09a793e4bec, 'USD'), -- reUSD
     (0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d, 'USD'), -- USD1
     (0x01d33fd36ec67c6ada32cf36b31e88ee190b1839, 'BRL'), -- BRZ
     (0x07a24ca74a7592c16827d186b802e004cec33bb3, 'CLP'), -- CLPC
     (0x17cdb2a01e7a34cbb3dd4b83260b05d0274c8dab, 'NGN'), -- cNGN
     (0xba3f535bbcccca2a154b573ca6c5a49baae0a3ea, 'EUR'), -- dEURO
     (0x888883b5f5d21fb10dfeb70e8f9722b9fb0e5e51, 'EUR'), -- EUROP
     (0xa40640458fbc27b6eefedea1e9c9e17d4cee7a21, 'EUR'), -- AEUR
     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2, 'EUR'), -- EURAU
     (0x3c89cd1884e7bef73ca3ef08d2ef6ec338fd8e49, 'EUR'), -- EUR0
     (0x5f7827fdeb7c20b443265fc2f40845b715385ff2, 'EUR'), -- EURCV
     (0x39b8b6385416f4ca36a20319f70d28621895279d, 'EUR'), -- EURe
     (0x9d1a7a3191102e9f900faa10540837ba84dcbae7, 'EUR'), -- EURI
     (0x8df723295214ea6f21026eeeb4382d475f146f9f, 'EUR'), -- EURQ
     (0x79d4f0232a66c4c91b89c76362016a1707cfbf4f, 'CHF'), -- VCHF
     (0x998ffe1e43facffb941dc337dd0468d52ba5b48a, 'IDR'), -- IDRT
     (0xf197ffc28c23e0309b5559e7a166f2c6164c80aa, 'MXN'), -- MXNB
     (0x6ba75d640bebfe5da1197bb5a2aff3327789b5d3, 'EUR'), -- VEUR
     (0xebf2096e01455108badcbaf86ce30b6e5a72aa52, 'IDR'), -- XIDR
     (0xc08512927d12348f6620a698105e1baac6ecd911, 'JPY'), -- GYEN
     (0xe7c3d8c9a439fede00d2600032d5db0be71c3c29, 'JPY'), -- JPYC
     (0x48f07301e9e29c3c38a80ae8d9ae771f224f1054, 'ZAR'), -- xZAR
     (0xb755506531786c8ac63b756bab1ac387bacb0c04, 'ZAR'), -- ZARP
     (0xe343167631d89b6ffc58b88d6b7fb0228795491d, 'USD'), -- USDG

     (0xaca92e438df0b2401ff60da7e4337b687a2435da, 'USD'), -- mUSD
     (0xc139190f447e929f090edeb554d95abb8b18ac1c, 'USD'), -- USDtb
     (0xfa2b947eec368f42195f24f36d2af29f7c24cec2, 'USD'), -- USDf
     (0x4f8e5de400de08b164e7421b3ee387f461becd1a, 'USD'), -- USDD
     (0xc08e7e23c235073c6807c2efe7021304cb7c2815, 'USD'), -- XUSD
     (0xdd468a1ddc392dcdbef6db6e34e89aa338f9f186, 'USD'), -- MUSD
     (0xe556aba6fe6036275ec1f87eda296be72c811bce, 'USD'), -- NUSD
     (0xcccc62962d17b8914c62d74ffb843d73b2a3cccc, 'USD'), -- cUSD
     (0x1e33e98af620f1d563fcd3cfd3c75ace841204ef, 'USD'), -- DUSD
     (0x5422374b27757da72d5265cc745ea906e0446634, 'USD'), -- USDCV
     (0x4274cd7277c7bb0806bd5fe84b9adae466a8da0a, 'USD'), -- YUSD
     (0xde17a000ba631c5d7c2bd9fb692efea52d90dee2, 'USD'), -- USDN
     (0xc83e27f270cce0a3a3a29521173a83f402c1768b, 'USD'), -- USDQ
     (0x9cf12ccd6020b6888e4d4c4e4c7aca33c1eb91f8, 'USD')  -- USDaf

) as temp_table (contract_address, currency)
