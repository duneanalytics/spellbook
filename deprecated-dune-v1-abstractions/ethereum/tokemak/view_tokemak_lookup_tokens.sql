DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_tokens CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_tokens
(
	symbol, display_name, address, pricing_contract, decimals, is_pool, is_active, is_liability, is_dollar_stable
) AS (
    SELECT 'ETH' as symbol, 'Ether' as display_name,'\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as address,'\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea as pricing_contract, 18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'WETH' as symbol, 'Wrapped Ether' as display_name,'\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tWETH' as symbol, 'Tokemak-Wrapped Ether' as display_name,'\xD3D13a578a53685B4ac36A1Bab31912D2B2A2F36'::bytea as address,'\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'stETH' as symbol, 'Staked Ether' as display_name,'\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea as address,'\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea as pricing_contract, 18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'USDC' as symbol, 'USD Coin' as display_name,'\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea as address,''::bytea as pricing_contract,  6::numeric as decimals, false, true, false, true
    UNION
    SELECT 'USDT' as symbol, 'Tether USD' as display_name,'\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea as address,''::bytea as pricing_contract,  6::numeric as decimals, false, true, false, true
    UNION
    SELECT '3Crv' as symbol, 'Curve.fi DAI/USDC/USDT' as display_name,'\x6c3f90f043a72fa612cbac8115ee7e52bde6e490'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'CRV' as symbol, 'Curve DAO Token' as display_name,'\xD533a949740bb3306d119CC777fa900bA034cd52'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'CVX' as symbol, 'Convex Token' as display_name,'\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'ALCX' as symbol, 'Alchemix' as display_name,'\xdBdb4d16EdA451D0503b854CF79D55697F90c8DF'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tALCX' as symbol, 'Tokemak-Alchemix' as display_name,'\xD3B5D9a561c293Fb42b446FE7e237DaA9BF9AA84'::bytea as address,'\xdBdb4d16EdA451D0503b854CF79D55697F90c8DF'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'tOHM' as symbol, 'Tokemak-tOHM' as display_name,'\xe7a7D17e2177f66D035d9D50A7f48d8D8E31532D'::bytea as address,'\x383518188c0c6d7730d91b2c03a03c837814a899'::bytea as pricing_contract,  9::numeric as decimals, false, true, true, false
    UNION
    SELECT 'TCR' as symbol,  'Tracer' as display_name,'\x9C4A4204B79dd291D6b6571C5BE8BbcD0622F050'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tTCR' as symbol,  'Tokemak-tTracer' as display_name,'\x15A629f0665A3Eb97D7aE9A7ce7ABF73AeB79415'::bytea as address,'\x9C4A4204B79dd291D6b6571C5BE8BbcD0622F050'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'OHMv1' as symbol,'Olympus v1' as display_name,'\x383518188c0c6d7730d91b2c03a03c837814a899'::bytea as address,''::bytea as pricing_contract,  9::numeric as decimals, false, false, false, false
    UNION
    SELECT 'sOHM' as symbol,'Staked Olympus' as display_name,'\x04F2694C8fcee23e8Fd0dfEA1d4f5Bb8c352111F'::bytea as address,''::bytea as pricing_contract,  9::numeric as decimals, false, false, false, false
    UNION
    SELECT 'SUSHI' as symbol, 'Sushi Token' as display_name,'\x6B3595068778DD592e39A122f4f5a5cF09C90fE2'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tSUSHI' as symbol, 'Tokemak-tSushi Token' as display_name,'\xf49764c9C5d644ece6aE2d18Ffd9F1E902629777'::bytea as address,'\x6B3595068778DD592e39A122f4f5a5cF09C90fE2'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'FXS' as symbol, 'Frax Share' as display_name,'\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tFXS' as symbol, 'Tokemak-tFrax Share' as display_name,'\xADF15Ec41689fc5b6DcA0db7c53c9bFE7981E655'::bytea as address,'\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'FOX' as symbol,  'FOX' as display_name,'\xc770EEfAd204B5180dF6a14Ee197D99d808ee52d'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tFOX' as symbol,  'Tokemak-tFOX' as display_name,'\x808D3E6b23516967ceAE4f17a5F9038383ED5311'::bytea as address,'\xc770EEfAd204B5180dF6a14Ee197D99d808ee52d'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'APW' as symbol,  'APWine Token' as display_name,'\x4104b135DBC9609Fc1A9490E61369036497660c8'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tAPW' as symbol,  'Tokemak-tAPWine Token' as display_name,'\xDc0b02849Bb8E0F126a216A2840275Da829709B0'::bytea as address,'\x4104b135DBC9609Fc1A9490E61369036497660c8'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'FRAX' as symbol, 'FRAX' as display_name,'\x853d955aCEf822Db058eb8505911ED77F175b99e'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'VISR' as symbol, 'VISOR' as display_name,'\xF938424F7210f31dF2Aee3011291b658f872e91e'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tVISR' as symbol, 'Tokemak-tVISR' as display_name,'\x2d3eADE781c4E203c6028DAC11ABB5711C022029'::bytea as address,'\xF938424F7210f31dF2Aee3011291b658f872e91e'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'DAI' as symbol,  'Dai Stablecoin' as display_name,'\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'LUSD' as symbol,  'LUSD Stablecoin' as display_name,'\x5f98805A4E8be255a32880FDeC7F6728C6568bA0'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'tLUSD' as symbol,  'Tokemak-LUSD Stablecoin' as display_name,'\x9eEe9eE0CBD35014e12E1283d9388a40f69797A3'::bytea as address,'\x5f98805A4E8be255a32880FDeC7F6728C6568bA0'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'UST (Wormhole)' as symbol,  'UST (Wormhole)' as display_name,'\xa693b19d2931d498c5b318df961919bb4aee87a5'::bytea as address,''::bytea as pricing_contract,  6::numeric as decimals, false, true, false, true
    UNION
    SELECT 'twormUST' as symbol,  'Tokemak-twormUST' as display_name,'\x482258099De8De2d0bda84215864800EA7e6B03D'::bytea as address,'\xa693b19d2931d498c5b318df961919bb4aee87a5'::bytea as pricing_contract,  6::numeric as decimals, false, true, true, true
    UNION
    SELECT 'FEI' as symbol,  'Fei USD' as display_name,'\x956f47f50a910163d8bf957cf5846d573e7f87ca'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'SNX' as symbol, 'Synthetix Network Token' as display_name,'\xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'MATIC' as symbol, 'MATIC Token' as display_name,'\x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tSNX' as symbol, 'Tokemak-tSNX' as display_name,'\xeff721Eae19885e17f5B80187d6527aad3fFc8DE'::bytea as address,'\xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'MIM' as symbol, 'Magic Internet Money' as display_name,'\x99d8a9c45b2eca8864373a26d1459e3dff1e17f3'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'ALUSD' as symbol, 'Alchemix USD' as display_name, '\xbc6da0fe9ad5f3b0d58160288917aa56653660e9'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'sUSD' as symbol, 'Synth sUSD' as display_name, '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, true
    UNION
    SELECT 'tsUSD' as symbol, 'Tokemak-Synth sUSD' as display_name, '\x8d2254f3AE37201EFe9Dfd9131924FE0bDd97832'::bytea as address,'\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'GAMMA' as symbol, 'GAMMA' as display_name, '\x6BeA7CFEF803D1e3d5f7C0103f7ded065644e197'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'GAMMA_D' as symbol, 'GAMMA' as display_name, '\x8a539cB67785974DAA8E423750fFd7d28FEd793A'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, false, false, false
    UNION
    SELECT 'tGAMMA' as symbol, 'Tokemak-tGAMMA' as display_name, '\x2Fc6e9c1b2C07E18632eFE51879415a580AD22E1'::bytea as address,'\x6BeA7CFEF803D1e3d5f7C0103f7ded065644e197'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'tgOHM' as symbol, 'Tokemak-tgOHM' as display_name, '\x41f6a95Bacf9bC43704c4A4902BA5473A8B00263'::bytea as address,'\x0ab87046fbb341d058f17cbc4c1133f25a20a52f'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'tFEI' as symbol, 'Tokemak-tFEI' as display_name, '\x03DccCd17CC36eE61f9004BCfD7a85F58B2D360D'::bytea as address,'\x956f47f50a910163d8bf957cf5846d573e7f87ca'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'tUST' as symbol, 'Tokemak-tUST' as display_name, '\x7A75ec20249570c935Ec93403A2B840fBdAC63fd'::bytea as address,'\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'tDAI' as symbol, 'Tokemak-tDAI' as display_name, '\x0CE34F4c26bA69158BC2eB8Bf513221e44FDfB75'::bytea as address,'\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'tFRAX' as symbol, 'Tokemak-tFRAX' as display_name, '\x94671A3ceE8C7A12Ea72602978D1Bb84E920eFB2'::bytea as address,'\x853d955aCEf822Db058eb8505911ED77F175b99e'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'talUSD' as symbol, 'Tokemak-talUSD' as display_name, '\x7211508D283353e77b9A7ed2f22334C219AD4b4C'::bytea as address,'\xbc6da0fe9ad5f3b0d58160288917aa56653660e9'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'tMIM' as symbol, 'Tokemak-tMIM' as display_name, '\x2e9F9bECF5229379825D0D3C1299759943BD4fED'::bytea as address,'\x99d8a9c45b2eca8864373a26d1459e3dff1e17f3'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, true
    UNION
    SELECT 'gOHM' as symbol, 'Governance OHM' as display_name, '\x0ab87046fbb341d058f17cbc4c1133f25a20a52f'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'FPIS' as symbol, 'Frax Price Index Share' as display_name, '\x4eb8b4c65d8430647586cf44af4bf23ded2bb794'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'UST' as symbol, 'Wrapped UST Token' as display_name, '\xa47c8bf37f92aBed4A126BDA807A7b7498661acD'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, false, false, true
    UNION
    SELECT 'UNI-V2' as symbol,'Uniswap V2 LP Token' as display_name, '\x5Fa464CEfe8901d66C09b85d5Fcdc55b3738c688'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'SLP' as symbol, 'SushiSwap LP Token' as display_name,'\xd4e7a6e2D03e4e48DfC27dd3f46DF1c176647E38'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'TOKE' as symbol, 'Tokemak' as display_name,'\x2e9d63788249371f1DFC918a52f8d799F4a38C94'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tTOKE' as symbol, 'Tokemak-TokePool' as display_name,'\xa760e26aA76747020171fCF8BdA108dFdE8Eb930'::bytea as address,'\x2e9d63788249371f1DFC918a52f8d799F4a38C94'::bytea as pricing_contract, 18::numeric as decimals, false, true, true, false
    UNION
    SELECT 'tUSDC' as symbol, 'Tokemak-UsdcPool ' as display_name,'\x04bDA0CF6Ad025948Af830E75228ED420b0e860d'::bytea as address,'\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea as pricing_contract, 6::numeric as decimals, false, true, true, true
    UNION
    SELECT 'ftWETH-26' as symbol, 'Token Mass Injection Pool TokemakWethPool' as display_name,'\xEaC275b19d55cC2b79783C894FbaC218c0f6D8d5'::bytea as address,''::bytea as pricing_contract, 6::numeric as decimals, true, true, false, false
    UNION
    SELECT 'UST_whv23CRV-f' as symbol, 'Curve.fi Factory USD Metapool: wormhole v2 UST-3Pool' as display_name,'\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'alUSD3CRV-f' as symbol, 'Curve.fi Factory USD Metapool: Alchemix USD' as display_name,'\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'LUSD3CRV-f' as symbol, 'Curve.fi Factory USD Metapool: Liquity USD' as display_name,'\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'FRAX3CRV-f' as symbol, 'Curve.fi Factory USD Metapool: Frax' as display_name,'\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'TOKEETH-f' as symbol, 'Curve.fi Factory Crypto Pool: TOKE/ETH' as display_name,'\x7ea4aD8C803653498bF6AC1D2dEbc04DCe8Fd2aD'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tGAMMA-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tGAMMA/GAMMA' as display_name,'\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tSNX-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tSNX/SNX' as display_name,'\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tAPW-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tAPW/APW' as display_name,'\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tFOX-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tFOX/FOX' as display_name,'\xC250B22d15e43d95fBE27B12d98B6098f8493eaC'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tFXS-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tFXS/FXS' as display_name,'\x961226B64AD373275130234145b96D100Dc0b655'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tSUSHI-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tSUSHI/SUSHI' as display_name,'\x0437ac6109e8A366A1F4816edF312A36952DB856'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tTCR-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tTCR/TCR' as display_name,'\x01FE650EF2f8e2982295489AE6aDc1413bF6011F'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tALCX-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tALCX/ALCX' as display_name,'\x9001a452d39A8710D27ED5c2E10431C13F5Fba74'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tFRAX+FRAX-f' as symbol, 'Curve.fi Factory Plain Pool: tFRAX/FRAX Test' as display_name,'\x694650a0B2866472c2EEA27827CE6253C1D13074'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'tWETH-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tWETH/WETH' as display_name,'\x06d39e95977349431e3d800d49c63b4d472e10fb'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'steCRV' as symbol, 'Curve.fi ETH/stETH' as display_name,'\x06325440D014e39736583c165C2963BA99fAf14E'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'fraxUSDC' as symbol, 'Curve.fi FRAX/USDC' as display_name,'\x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'UNI-V2-FXS/ETH' as symbol, 'Uniswap FXS/ETH LP' as display_name,'\xecba967d84fcf0405f6b32bc45f4d36bfdbb2e81'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION 
    SELECT 'UNI-V2-ETH/FOX' as symbol, 'Uniswap ETH/FOX LP' as display_name,'\x470e8de2eBaef52014A47Cb5E6aF86884947F08c'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'UNI-V2-SNX/ETH' as symbol, 'Uniswap SNX/ETH LP' as display_name,'\x43AE24960e5534731Fc831386c07755A2dc33D47'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'UNI-V2-TCR/ETH' as symbol, 'Uniswap TCR/ETH LP' as display_name,'\xdc08159a6c82611aeb347ba897d82ac1b80d9419'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'UNI-V2-GAMMA/ETH' as symbol, 'Uniswap GAMMA/ETH LP' as display_name,'\xad5b1a6abc1c9598c044cea295488433a3499efc'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-WETH/ALCX-SLP' as symbol, 'SushiSwap WETH/ALCX LP (SLP)' as display_name,'\xc3f279090a47e80990fe3a9c30d24cb117ef91a8'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-TCR/WETH' as symbol, 'SushiSwap TCR/WETH LP' as display_name,'\xe55c3e83852429334a986b265d03b879a3d188ac'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-SUSHI/WETH-SLP' as symbol, 'SushiSwap SUSHI/WETH LP (SLP)' as display_name,'\x795065dcc9f64b5614c407a6efdc400da6221fb0'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-FXS/WETH' as symbol, 'SushiSwap FXS/WETH LP' as display_name,'\x61eb53ee427ab4e007d78a9134aacb3101a2dc23'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-APW/WETH' as symbol, 'SushiSwap APW/WETH LP (SLP)' as display_name,'\x53162d78dca413d9e28cf62799d17a9e278b60e8'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'SUSHI-SNX/WETH' as symbol, 'SushiSwap SNX/WETH LP (SLP)' as display_name,'\xa1d7b2d891e3a1f9ef4bbc5be20630c2feb1c470'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false

);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_tokens (
   address,
   pricing_contract
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_lookup_tokens$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;