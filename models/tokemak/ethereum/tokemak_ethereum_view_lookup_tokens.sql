{{ config (
    alias = 'tokemak_ethereum_view_lookup_tokens',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
        "project", 
            "Tokemak",
             \'["needmorebass"]\') }}'
) }}


WITH tokemak_ethereum_view_lookup_tokens(symbol, display_name, address, pricing_contract, decimals, is_pool, is_active, is_liability, is_dollar_stable) AS
(
    SELECT 'ETH' , 'Ether' ,'0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' ,'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' , CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'WETH' , 'Wrapped Ether' ,'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' ,'' , CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tWETH' , 'Tokemak-Wrapped Ether' ,'0xD3D13a578a53685B4ac36A1Bab31912D2B2A2F36' ,'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'stETH' , 'Staked Ether' ,'0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84' ,'0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84' , CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'USDC' , 'USD Coin' ,'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' ,'' ,  CAST(6 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'USDT' , 'Tether USD' ,'0xdAC17F958D2ee523a2206206994597C13D831ec7' ,'' ,  CAST(6 AS DECIMAL) , false, true, false, true
    UNION
    SELECT '3Crv' , 'Curve.fi DAI/USDC/USDT' ,'0x6c3f90f043a72fa612cbac8115ee7e52bde6e490' ,'' ,  CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'CRV' , 'Curve DAO Token' ,'0xD533a949740bb3306d119CC777fa900bA034cd52' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'CVX' , 'Convex Token' ,'0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'ALCX' , 'Alchemix' ,'0xdBdb4d16EdA451D0503b854CF79D55697F90c8DF' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tALCX' , 'Tokemak-Alchemix' ,'0xD3B5D9a561c293Fb42b446FE7e237DaA9BF9AA84' ,'0xdBdb4d16EdA451D0503b854CF79D55697F90c8DF' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'tOHM' , 'Tokemak-tOHM' ,'0xe7a7D17e2177f66D035d9D50A7f48d8D8E31532D' ,'0x383518188c0c6d7730d91b2c03a03c837814a899' ,  CAST(9 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'TCR' ,  'Tracer' ,'0x9C4A4204B79dd291D6b6571C5BE8BbcD0622F050' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tTCR' ,  'Tokemak-tTracer' ,'0x15A629f0665A3Eb97D7aE9A7ce7ABF73AeB79415' ,'0x9C4A4204B79dd291D6b6571C5BE8BbcD0622F050' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'OHMv1' ,'Olympus v1' ,'0x383518188c0c6d7730d91b2c03a03c837814a899' ,'' ,  CAST(9 AS DECIMAL) , false, false, false, false
    UNION
    SELECT 'sOHM' ,'Staked Olympus' ,'0x04F2694C8fcee23e8Fd0dfEA1d4f5Bb8c352111F' ,'' ,  CAST(9 AS DECIMAL) , false, false, false, false
    UNION
    SELECT 'SUSHI' , 'Sushi Token' ,'0x6B3595068778DD592e39A122f4f5a5cF09C90fE2' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tSUSHI' , 'Tokemak-tSushi Token' ,'0xf49764c9C5d644ece6aE2d18Ffd9F1E902629777' ,'0x6B3595068778DD592e39A122f4f5a5cF09C90fE2' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'FXS' , 'Frax Share' ,'0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tFXS' , 'Tokemak-tFrax Share' ,'0xADF15Ec41689fc5b6DcA0db7c53c9bFE7981E655' ,'0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'FOX' ,  'FOX' ,'0xc770EEfAd204B5180dF6a14Ee197D99d808ee52d' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tFOX' ,  'Tokemak-tFOX' ,'0x808D3E6b23516967ceAE4f17a5F9038383ED5311' ,'0xc770EEfAd204B5180dF6a14Ee197D99d808ee52d' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'APW' ,  'APWine Token' ,'0x4104b135DBC9609Fc1A9490E61369036497660c8' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tAPW' ,  'Tokemak-tAPWine Token' ,'0xDc0b02849Bb8E0F126a216A2840275Da829709B0' ,'0x4104b135DBC9609Fc1A9490E61369036497660c8' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'FRAX' , 'FRAX' ,'0x853d955aCEf822Db058eb8505911ED77F175b99e' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'VISR' , 'VISOR' ,'0xF938424F7210f31dF2Aee3011291b658f872e91e' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tVISR' , 'Tokemak-tVISR' ,'0x2d3eADE781c4E203c6028DAC11ABB5711C022029' ,'0xF938424F7210f31dF2Aee3011291b658f872e91e' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'DAI' ,  'Dai Stablecoin' ,'0x6B175474E89094C44Da98b954EedeAC495271d0F' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'LUSD' ,  'LUSD Stablecoin' ,'0x5f98805A4E8be255a32880FDeC7F6728C6568bA0' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'tLUSD' ,  'Tokemak-LUSD Stablecoin' ,'0x9eEe9eE0CBD35014e12E1283d9388a40f69797A3' ,'0x5f98805A4E8be255a32880FDeC7F6728C6568bA0' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'UST (Wormhole)' ,  'UST (Wormhole)' ,'0xa693b19d2931d498c5b318df961919bb4aee87a5' ,'' ,  CAST(6 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'twormUST' ,  'Tokemak-twormUST' ,'0x482258099De8De2d0bda84215864800EA7e6B03D' ,'0xa693b19d2931d498c5b318df961919bb4aee87a5' ,  CAST(6 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'FEI' ,  'Fei USD' ,'0x956f47f50a910163d8bf957cf5846d573e7f87ca' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'SNX' , 'Synthetix Network Token' ,'0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'MATIC' , 'MATIC Token' ,'0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tSNX' , 'Tokemak-tSNX' ,'0xeff721Eae19885e17f5B80187d6527aad3fFc8DE' ,'0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'MIM' , 'Magic Internet Money' ,'0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'ALUSD' , 'Alchemix USD' , '0xbc6da0fe9ad5f3b0d58160288917aa56653660e9' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'sUSD' , 'Synth sUSD' , '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, true
    UNION
    SELECT 'tsUSD' , 'Tokemak-Synth sUSD' , '0x8d2254f3AE37201EFe9Dfd9131924FE0bDd97832' ,'0x57Ab1ec28D129707052df4dF418D58a2D46d5f51' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'GAMMA' , 'GAMMA' , '0x6BeA7CFEF803D1e3d5f7C0103f7ded065644e197' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'GAMMA_D' , 'GAMMA' , '0x8a539cB67785974DAA8E423750fFd7d28FEd793A' ,'' ,  CAST(18 AS DECIMAL) , false, false, false, false
    UNION
    SELECT 'tGAMMA' , 'Tokemak-tGAMMA' , '0x2Fc6e9c1b2C07E18632eFE51879415a580AD22E1' ,'0x6BeA7CFEF803D1e3d5f7C0103f7ded065644e197' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'MYC' , 'MYC' ,'0x4b13006980acb09645131b91d259eaa111eaf5ba' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tMYC' , 'Tokemak-tMYC' ,'0x061aee9ab655e73719577EA1df116D7139b2A7E7' ,'0x4b13006980acb09645131b91d259eaa111eaf5ba' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'tgOHM' , 'Tokemak-tgOHM' , '0x41f6a95Bacf9bC43704c4A4902BA5473A8B00263' ,'0x0ab87046fbb341d058f17cbc4c1133f25a20a52f' ,  CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'tFEI' , 'Tokemak-tFEI' , '0x03DccCd17CC36eE61f9004BCfD7a85F58B2D360D' ,'0x956f47f50a910163d8bf957cf5846d573e7f87ca' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'tUST' , 'Tokemak-tUST' , '0x7A75ec20249570c935Ec93403A2B840fBdAC63fd' ,'0xdAC17F958D2ee523a2206206994597C13D831ec7' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'tDAI' , 'Tokemak-tDAI' , '0x0CE34F4c26bA69158BC2eB8Bf513221e44FDfB75' ,'0x6B175474E89094C44Da98b954EedeAC495271d0F' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'tFRAX' , 'Tokemak-tFRAX' , '0x94671A3ceE8C7A12Ea72602978D1Bb84E920eFB2' ,'0x853d955aCEf822Db058eb8505911ED77F175b99e' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'talUSD' , 'Tokemak-talUSD' , '0x7211508D283353e77b9A7ed2f22334C219AD4b4C' ,'0xbc6da0fe9ad5f3b0d58160288917aa56653660e9' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'tMIM' , 'Tokemak-tMIM' , '0x2e9F9bECF5229379825D0D3C1299759943BD4fED' ,'0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3' ,  CAST(18 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'gOHM' , 'Governance OHM' , '0x0ab87046fbb341d058f17cbc4c1133f25a20a52f' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'FPIS' , 'Frax Price Index Share' , '0x4eb8b4c65d8430647586cf44af4bf23ded2bb794' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'UST' , 'Wrapped UST Token' , '0xa47c8bf37f92aBed4A126BDA807A7b7498661acD' ,'' ,  CAST(18 AS DECIMAL) , false, false, false, true
    UNION
    SELECT 'UNI-V2' ,'Uniswap V2 LP Token' , '0x5Fa464CEfe8901d66C09b85d5Fcdc55b3738c688' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'SLP' , 'SushiSwap LP Token' ,'0xd4e7a6e2D03e4e48DfC27dd3f46DF1c176647E38' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'TOKE' , 'Tokemak' ,'0x2e9d63788249371f1DFC918a52f8d799F4a38C94' ,'' ,  CAST(18 AS DECIMAL) , false, true, false, false
    UNION
    SELECT 'tTOKE' , 'Tokemak-TokePool' ,'0xa760e26aA76747020171fCF8BdA108dFdE8Eb930' ,'0x2e9d63788249371f1DFC918a52f8d799F4a38C94' , CAST(18 AS DECIMAL) , false, true, true, false
    UNION
    SELECT 'tUSDC' , 'Tokemak-UsdcPool ' ,'0x04bDA0CF6Ad025948Af830E75228ED420b0e860d' ,'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' , CAST(6 AS DECIMAL) , false, true, true, true
    UNION
    SELECT 'ftWETH-26' , 'Token Mass Injection Pool TokemakWethPool' ,'0xEaC275b19d55cC2b79783C894FbaC218c0f6D8d5' ,'' , CAST(6 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'UST_whv23CRV-f' , 'Curve.fi Factory USD Metapool: wormhole v2 UST-3Pool' ,'0xCEAF7747579696A2F0bb206a14210e3c9e6fB269' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'alUSD3CRV-f' , 'Curve.fi Factory USD Metapool: Alchemix USD' ,'0x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'LUSD3CRV-f' , 'Curve.fi Factory USD Metapool: Liquity USD' ,'0xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'FRAX3CRV-f' , 'Curve.fi Factory USD Metapool: Frax' ,'0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'TOKEETH-f' , 'Curve.fi Factory Crypto Pool: TOKE/ETH' ,'0x7ea4aD8C803653498bF6AC1D2dEbc04DCe8Fd2aD' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tGAMMA-f' , 'Curve.fi Factory Plain Pool: Tokemak tGAMMA/GAMMA' ,'0x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tMYC-f' , 'Curve.fi Factory Plain Pool: Tokemak tMYC/MYC' ,'0x83D78bf3f861e898cCA47BD076b3839Ab5469d70' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tSNX-f' , 'Curve.fi Factory Plain Pool: Tokemak tSNX/SNX' ,'0x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tAPW-f' , 'Curve.fi Factory Plain Pool: Tokemak tAPW/APW' ,'0xCaf8703f8664731cEd11f63bB0570E53Ab4600A9' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tFOX-f' , 'Curve.fi Factory Plain Pool: Tokemak tFOX/FOX' ,'0xC250B22d15e43d95fBE27B12d98B6098f8493eaC' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tFXS-f' , 'Curve.fi Factory Plain Pool: Tokemak tFXS/FXS' ,'0x961226B64AD373275130234145b96D100Dc0b655' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tSUSHI-f' , 'Curve.fi Factory Plain Pool: Tokemak tSUSHI/SUSHI' ,'0x0437ac6109e8A366A1F4816edF312A36952DB856' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tTCR-f' , 'Curve.fi Factory Plain Pool: Tokemak tTCR/TCR' ,'0x01FE650EF2f8e2982295489AE6aDc1413bF6011F' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tALCX-f' , 'Curve.fi Factory Plain Pool: Tokemak tALCX/ALCX' ,'0x9001a452d39A8710D27ED5c2E10431C13F5Fba74' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tFRAX+FRAX-f' , 'Curve.fi Factory Plain Pool: tFRAX/FRAX Test' ,'0x694650a0B2866472c2EEA27827CE6253C1D13074' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'tWETH-f' , 'Curve.fi Factory Plain Pool: Tokemak tWETH/WETH' ,'0x06d39e95977349431e3d800d49c63b4d472e10fb' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'steCRV' , 'Curve.fi ETH/stETH' ,'0x06325440D014e39736583c165C2963BA99fAf14E' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'fraxUSDC' , 'Curve.fi FRAX/USDC' ,'0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'CONVEX-FRAX-fraxUSDC' , 'Convex Frax FRAX/USDC' ,'0x8a53ee42FB458D4897e15cc7dEa3F75D0F1c3475' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'crvFXS/FXS' , 'Curve.fi crvFXS/FXS' ,'0xF3A43307DcAFa93275993862Aae628fCB50dC768' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'UNI-V2-FXS/ETH' , 'Uniswap FXS/ETH LP' ,'0xecba967d84fcf0405f6b32bc45f4d36bfdbb2e81' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION 
    SELECT 'UNI-V2-ETH/FOX' , 'Uniswap ETH/FOX LP' ,'0x470e8de2eBaef52014A47Cb5E6aF86884947F08c' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'UNI-V2-SNX/ETH' , 'Uniswap SNX/ETH LP' ,'0x43AE24960e5534731Fc831386c07755A2dc33D47' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'UNI-V2-TCR/ETH' , 'Uniswap TCR/ETH LP' ,'0xdc08159a6c82611aeb347ba897d82ac1b80d9419' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'UNI-V2-GAMMA/ETH' , 'Uniswap GAMMA/ETH LP' ,'0xad5b1a6abc1c9598c044cea295488433a3499efc' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-WETH/ALCX-SLP' , 'SushiSwap WETH/ALCX LP (SLP)' ,'0xc3f279090a47e80990fe3a9c30d24cb117ef91a8' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-TCR/WETH' , 'SushiSwap TCR/WETH LP' ,'0xe55c3e83852429334a986b265d03b879a3d188ac' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-SUSHI/WETH-SLP' , 'SushiSwap SUSHI/WETH LP (SLP)' ,'0x795065dcc9f64b5614c407a6efdc400da6221fb0' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-FXS/WETH' , 'SushiSwap FXS/WETH LP' ,'0x61eb53ee427ab4e007d78a9134aacb3101a2dc23' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-APW/WETH' , 'SushiSwap APW/WETH LP (SLP)' ,'0x53162d78dca413d9e28cf62799d17a9e278b60e8' ,'' , CAST(18 AS DECIMAL) , true, true, false, false
    UNION
    SELECT 'SUSHI-SNX/WETH' , 'SushiSwap SNX/WETH LP (SLP)' ,'0xa1d7b2d891e3a1f9ef4bbc5be20630c2feb1c470' ,'' , CAST(18 AS DECIMAL) , true, true, false, false

)

SELECT 
    symbol, 
    display_name, 
    address, 
    pricing_contract, 
    decimals, 
    is_pool, 
    is_active, 
    is_liability, 
    is_dollar_stable
FROM tokemak_ethereum_view_lookup_tokens