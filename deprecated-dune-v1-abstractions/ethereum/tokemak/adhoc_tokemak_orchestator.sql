--step 1
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_addresses cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_addresses 
(
	address
) AS (

    SELECT '\x9e0bcE7ec474B481492610eB9dd5D69EB03718D5' ::bytea AS address /*deployer*/
    UNION 
    SELECT '\x90b6C61B102eA260131aB48377E143D6EB3A9d4B' ::bytea AS address/*coordinator*/
    UNION 
    SELECT '\xA86e412109f77c45a3BC1c5870b880492Fb86A14' ::bytea AS address/*manager*/
    UNION 
    SELECT '\x8b4334d4812c530574bd4f2763fcd22de94a969b' ::bytea as address /*treasury*/

);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_addresses (
   address
);

--step 2
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_sources cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_sources
(
	id, source_name
) AS (
    SELECT 0 as id, 'Undefined' as source_name
    UNION
    SELECT 1 as id, 'Curve' as source_name
    UNION
    SELECT 2 as id, 'Convex' as source_name
    UNION
    SELECT 3 as id, 'Sushiswap' as source_name
    UNION
    SELECT 4 as id, 'UniswapV2' as source_name
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_sources (
   id
);


--step 3
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_metapools cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_metapools
(
	tokemak_curve_metapool_id, base_pool_symbol, base_pool_address, pool_token_address, is_active
) AS (
    SELECT 1 as tokemak_curve_metapool_id, 'Curve.fi: DAI/USDC/USDT Pool' as base_pool_symbol, '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea as base_pool_address, '\x6c3f90f043a72fa612cbac8115ee7e52bde6e490'::bytea as pool_token_address, true
    UNION
    SELECT 2 as tokemak_curve_metapool_id, 'Curve.fi ETH/stETH' as base_pool_symbol, '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea as base_pool_address, '\x06325440D014e39736583c165C2963BA99fAf14E'::bytea as pool_token_address, true
    UNION
    SELECT 3 as tokemak_curve_metapool_id, 'Curve.fi FRAX/USDC' as base_pool_symbol, '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'::bytea as base_pool_address, '\x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC'::bytea as pool_token_address, true
    UNION
    SELECT 4 as tokemak_curve_metapool_id, 'Convex FRAX/USDC' as base_pool_symbol, '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'::bytea as base_pool_address, '\x8a53ee42FB458D4897e15cc7dEa3F75D0F1c3475'::bytea as pool_token_address, true
    UNION
    SELECT 5 as tokemak_curve_metapool_id, 'Curve.fi crvFXS/FXS' as base_pool_symbol, '\xd658A338613198204DCa1143Ac3F01A722b5d94A'::bytea as base_pool_address, '\xF3A43307DcAFa93275993862Aae628fCB50dC768'::bytea as pool_token_address, true
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_metapools (
   tokemak_curve_metapool_id, base_pool_address
);

--step 4
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_reactors cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_reactors
(
	    reactor_address, underlyer_address, reactor_name, is_deployable
) AS (

    SELECT '\xD3B5D9a561c293Fb42b446FE7e237DaA9BF9AA84'::bytea as reactor_address, '\xdBdb4d16EdA451D0503b854CF79D55697F90c8DF'::bytea as underlyer_address, 'ALCX Reactor', true
    UNION
    SELECT '\xD3D13a578a53685B4ac36A1Bab31912D2B2A2F36'::bytea as reactor_address, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea as underlyer_address, 'WETH Reactor', true
    UNION
    SELECT '\x04bDA0CF6Ad025948Af830E75228ED420b0e860d'::bytea as reactor_address, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea as underlyer_address, 'USDC Reactor', true
    UNION
    SELECT '\x15A629f0665A3Eb97D7aE9A7ce7ABF73AeB79415'::bytea as reactor_address, '\x9C4A4204B79dd291D6b6571C5BE8BbcD0622F050'::bytea as underlyer_address, 'TCR Reactor', true
    UNION
    SELECT '\xe7a7D17e2177f66D035d9D50A7f48d8D8E31532D'::bytea as reactor_address, '\x383518188c0c6d7730d91b2c03a03c837814a899'::bytea as underlyer_address, 'OHMv1 Reactor', true
    UNION
    SELECT '\xf49764c9C5d644ece6aE2d18Ffd9F1E902629777'::bytea as reactor_address, '\x6B3595068778DD592e39A122f4f5a5cF09C90fE2'::bytea as underlyer_address, 'SUSHI Reactor', true
    UNION
    SELECT '\xADF15Ec41689fc5b6DcA0db7c53c9bFE7981E655'::bytea as reactor_address, '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea as underlyer_address, 'FXS Reactor', true
    UNION
    SELECT '\x808D3E6b23516967ceAE4f17a5F9038383ED5311'::bytea as reactor_address, '\xc770EEfAd204B5180dF6a14Ee197D99d808ee52d'::bytea as underlyer_address, 'FOX Reactor', true
    UNION
    SELECT '\xDc0b02849Bb8E0F126a216A2840275Da829709B0'::bytea as reactor_address, '\x4104b135DBC9609Fc1A9490E61369036497660c8'::bytea as underlyer_address, 'APW Reactor', true
    UNION
    SELECT '\x94671A3ceE8C7A12Ea72602978D1Bb84E920eFB2'::bytea as reactor_address, '\x853d955aCEf822Db058eb8505911ED77F175b99e'::bytea as underlyer_address, 'FRAX Reactor', true
    UNION
    SELECT '\x0CE34F4c26bA69158BC2eB8Bf513221e44FDfB75'::bytea as reactor_address, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea as underlyer_address, 'DAI Reactor', true
    UNION
    SELECT '\x9eEe9eE0CBD35014e12E1283d9388a40f69797A3'::bytea as reactor_address, '\x5f98805A4E8be255a32880FDeC7F6728C6568bA0'::bytea as underlyer_address, 'LUSD Reactor', true
    UNION
    SELECT '\x482258099De8De2d0bda84215864800EA7e6B03D'::bytea as reactor_address, '\xa693b19d2931d498c5b318df961919bb4aee87a5'::bytea as underlyer_address, 'WORMUST Reactor', true
    UNION
    SELECT '\x03DccCd17CC36eE61f9004BCfD7a85F58B2D360D'::bytea as reactor_address, '\x956f47f50a910163d8bf957cf5846d573e7f87ca'::bytea as underlyer_address, 'FEI Reactor', true
    UNION
    SELECT '\xeff721Eae19885e17f5B80187d6527aad3fFc8DE'::bytea as reactor_address, '\xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F'::bytea as underlyer_address, 'SNX Reactor', true
    UNION
    SELECT '\x2e9F9bECF5229379825D0D3C1299759943BD4fED'::bytea as reactor_address, '\x99d8a9c45b2eca8864373a26d1459e3dff1e17f3'::bytea as underlyer_address, 'MIM Reactor', true
    UNION
    SELECT '\x7211508D283353e77b9A7ed2f22334C219AD4b4C'::bytea as reactor_address, '\xbc6da0fe9ad5f3b0d58160288917aa56653660e9'::bytea as underlyer_address, 'ALUSD Reactor', true
    UNION
    SELECT '\x2Fc6e9c1b2C07E18632eFE51879415a580AD22E1'::bytea as reactor_address, '\x6BeA7CFEF803D1e3d5f7C0103f7ded065644e197'::bytea as underlyer_address, 'GAMMA Reactor', true
    UNION
    SELECT '\x061aee9ab655e73719577EA1df116D7139b2A7E7'::bytea as reactor_address, '\x4b13006980aCB09645131b91D259eaA111eaF5Ba'::bytea as underlyer_address, 'MYC Reactor', true
    UNION
    SELECT '\x41f6a95Bacf9bC43704c4A4902BA5473A8B00263'::bytea as reactor_address, '\x0ab87046fbb341d058f17cbc4c1133f25a20a52f'::bytea as underlyer_address, 'gOHM Reactor', true
    UNION
    SELECT '\x7A75ec20249570c935Ec93403A2B840fBdAC63fd'::bytea as reactor_address, '\xa47c8bf37f92aBed4A126BDA807A7b7498661acD'::bytea as underlyer_address, 'UST Reactor', true
    UNION
    SELECT '\x1b429e75369ea5cd84421c1cc182cee5f3192fd3'::bytea as reactor_address, '\x5Fa464CEfe8901d66C09b85d5Fcdc55b3738c688'::bytea as underlyer_address, 'UNI-LP Reactor', false
    UNION
    SELECT '\x8858A739eA1dd3D80FE577EF4e0D03E88561FaA3'::bytea as reactor_address, '\xd4e7a6e2D03e4e48DfC27dd3f46DF1c176647E38'::bytea as underlyer_address, 'SUSHI-LP Reactor', false
    UNION
    SELECT '\xa760e26aA76747020171fCF8BdA108dFdE8Eb930'::bytea as reactor_address, '\x2e9d63788249371f1DFC918a52f8d799F4a38C94'::bytea as underlyer_address, 'TOKE Reactor', false
    UNION
    SELECT '\x96F98Ed74639689C3A11daf38ef86E59F43417D3'::bytea as reactor_address, '\x2e9d63788249371f1DFC918a52f8d799F4a38C94'::bytea as underlyer_address, 'TOKE-Staking Reactor', false
    UNION
    SELECT '\x2d3eADE781c4E203c6028DAC11ABB5711C022029'::bytea as reactor_address, '\xF938424F7210f31dF2Aee3011291b658f872e91e'::bytea as underlyer_address, 'VISOR Reactor', true
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_reactors (
   reactor_address, underlyer_address
);

--step 5
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_tokens cascade
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
    SELECT 'MYC' as symbol, 'MYC' as display_name,'\x4b13006980acb09645131b91d259eaa111eaf5ba'::bytea as address,''::bytea as pricing_contract,  18::numeric as decimals, false, true, false, false
    UNION
    SELECT 'tMYC' as symbol, 'Tokemak-tMYC' as display_name,'\x061aee9ab655e73719577EA1df116D7139b2A7E7'::bytea as address,'\x4b13006980acb09645131b91d259eaa111eaf5ba'::bytea as pricing_contract,  18::numeric as decimals, false, true, true, false
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
    SELECT 'tMYC-f' as symbol, 'Curve.fi Factory Plain Pool: Tokemak tMYC/MYC' as display_name,'\x83D78bf3f861e898cCA47BD076b3839Ab5469d70'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
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
    SELECT 'CONVEX-fraxUSDC' as symbol, 'Convex FRAX/USDC' as display_name,'\x8a53ee42FB458D4897e15cc7dEa3F75D0F1c3475'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
    UNION
    SELECT 'crvFXS/FXS' as symbol, 'Curve.fi crvFXS/FXS' as display_name,'\xF3A43307DcAFa93275993862Aae628fCB50dC768'::bytea as address,''::bytea as pricing_contract, 18::numeric as decimals, true, true, false, false
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


--step 6
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_curve_convex_pool_total_supply cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_curve_convex_pool_total_supply (
	"date"
    ,address
    ,symbol
    ,total_supply
) AS (
WITH calendar AS  
        (SELECT i::date as "date"
            ,tl.address
            ,tl.symbol
            ,tl.decimals
        FROM tokemak."view_tokemak_lookup_tokens" tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        WHERE tl.is_pool = true order by "date" desc
 ) 

    , result AS(
        SELECT symbol, contract_address as address, date_trunc('day', "date")::date as "date", total_supply[1] as evt_block_number, total_supply[3] as total_supply FROM (
        --3Crv
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
        --eth/stETH
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
            --frax/USDC
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
            --alUSD3CRV
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --LUSD
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidityOne"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
        --wormhole3CRV
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --FRAX3crv
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --WETH
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --ALCX
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY symbol, contract_address,  "date")
            --TCR
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --sushi
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --fxs
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  evt_block_time
            ) as t GROUP BY  symbol, contract_address,  "date")
            --fox
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --apw
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --snx
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  evt_block_time
            ) as t GROUP BY  symbol, contract_address,  "date")
            --gamma
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --myc
            UNION
            (SELECT symbol, contract_address, "date", MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tMYC_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tMYC_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tMYC_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tMYC_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address, "date"
            ) as t GROUP BY  symbol, contract_address, "date")
        ) as t order by "date" desc )

    , temp_table AS ( 
        SELECT 
            c."date"
            , c.address
            , c.symbol
            , total_supply
            , count(total_supply) OVER (PARTITION BY c.address ORDER BY c."date") AS grpSupply
        FROM calendar c 
        LEFT OUTER JOIN result r on r."date" = c."date" and r.address = c.address)
    
    , res_temp AS(    
    SELECT 
        "date"::date
        ,address
        ,symbol
        ,first_value(total_supply) OVER (PARTITION BY symbol, address, grpSupply ORDER BY "date") AS total_supply
    FROM  temp_table 
    order by "date" desc, symbol)

    SELECT "date"
        , address
        , symbol
        , total_supply
    FROM res_temp
    WHERE total_supply>0
    
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_curve_convex_pool_total_supply (
   "date", address
);



--step 7
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_convex_pool_stats_daily cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_convex_pool_stats_daily
(   
    source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
) AS (
    
WITH  convex_pools As 
    (
        SELECT contract_address as pool_address, symbol, sum(qty) as qty 
            FROM (
            SELECT contract_address,tl.symbol, (value/10^tl.decimals)*-1 as qty 
            FROM erc20."ERC20_evt_Transfer" t
            INNER JOIN tokemak.view_tokemak_lookup_tokens tl on tl.address = t.contract_address
            WHERE t."to"='\xA86e412109f77c45a3BC1c5870b880492Fb86A14' and t."from"='\xF403C135812408BFbE8713b5A23a04b3D48AAE31'
            AND NOT (t."to" = t."from")
            UNION
            SELECT contract_address,tl.symbol, value/10^tl.decimals as qty 
            FROM erc20."ERC20_evt_Transfer" t
            INNER JOIN tokemak.view_tokemak_lookup_tokens tl on tl.address = t.contract_address
            WHERE t."from"='\xA86e412109f77c45a3BC1c5870b880492Fb86A14' and t."to"='\x989aeb4d175e16225e39e87d0d97a3360524ad80'
            AND NOT (t."to" = t."from")
        )as t GROUP BY contract_address, symbol 
    ),
    pools_and_constituents AS (
        --fraxUSDC
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' OR "from" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'
     UNION
        --3crv
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' OR "from" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'
      UNION
      --ETH and stETH 
         SELECT"date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol,  (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', block_time) as"date",
                    '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                    SUM(CASE WHEN "to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' THEN value ELSE value *-1 END) as qty 
                FROM ethereum.traces 
                WHERE ("to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' OR "from" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022')
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
        UNION
        --stETH
        SELECT DATE_TRUNC('day', call_block_time) as"date",token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT DISTINCT ON (call_block_time::date) call_block_time
            , contract_address as token_address
            , output_0 as qty
                FROM lido."steth_call_balanceOf" where _account = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
                    --order by call_block_time::date desc, call_block_time desc
           )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
       --all other convex pools
    UNION 
       SELECT"date", token_address,p.symbol as pool_symbol, pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,pool_address,SUM(qty) OVER (PARTITION BY pool_address,token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                        cp.pool_address as pool_address,
                     t.contract_address as token_address,
                    SUM(CASE WHEN "to" = cp.pool_address THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" t INNER JOIN convex_pools cp on (t."to" = cp.pool_address OR t."from" = cp.pool_address) and cp.pool_address<>'\x06325440D014e39736583c165C2963BA99fAf14E' --omit the steETH pool because we get those quantities above
                AND NOT (cp.pool_address = '\xceaf7747579696a2f0bb206a14210e3c9e6fb269' AND t.contract_address = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')--somehow $100 worth of USDC was put in this pool so we need to omit it otherwise it looks like the ust 3crv pool also consists of a third instrument
                WHERE t.contract_address <>'\x4eb8b4c65d8430647586cf44af4bf23ded2bb794'   --need to omit anything that was airdropped into the pool 
                AND NOT (t."to" = t."from")
                GROUP BY 1,2,3 
            ) as tt 
      )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address  
       CROSS JOIN tokemak.view_tokemak_lookup_tokens p WHERE p.address = t.pool_address
      AND qty>0  
 )
 
,  calendar AS  
    (SELECT DISTINCT  i::date as "date", pool_address,pool_symbol,token_address,token_symbol
        FROM pools_and_constituents
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        )

  , temp AS
  (
  SELECT c."date"
  ,c.pool_address
  ,c.pool_symbol
  ,c.token_address
  ,c.token_symbol
  ,pc.qty  
  ,count(pc.qty) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpQty
  FROM calendar c 
  LEFT JOIN pools_and_constituents pc on pc."date" = c."date" AND pc.pool_address = c.pool_address AND pc.token_address = c.token_address 
  )

    SELECT 
        2 as source
        ,t."date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,ts.total_supply as lp_total_supply
        ,first_value(qty) OVER (PARTITION BY pool_address, token_address, grpQty ORDER BY t."date") AS qty
        FROM  temp t 
        INNER JOIN tokemak."view_tokemak_curve_convex_pool_total_supply" ts ON (t.pool_address = ts.address AND ts."date" = t."date")
        ORDER BY "date" desc, pool_symbol asc

        
            
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_convex_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

--step 8
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_curve_pool_stats_daily cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_curve_pool_stats_daily
(
    source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
) AS (
    
WITH   pools_and_constituents As 
    (SELECT  t."date",pool_address,pool_symbol,token_address,t.symbol as token_symbol,qty
                                                                     
    FROM(
        --3crv
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, 
        (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' OR "from" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' 
    UNION    
        --fraxUSDC
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, 
        (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' OR "from" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' 
    UNION    
       --wormhole
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269' OR "from" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269') AND contract_address <> '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' --somehow $100 worth of USDC was put in this pool so we need to omit it
                AND NOT ("to" = "from") AND DATE_TRUNC('day', evt_block_time) < '2022-05-11'
                GROUP BY 1,2 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'
       AND qty>0 
    UNION 
    --alUSD
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c' OR "from" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c'
       AND qty>0 
    UNION    
    --FRAX3CRV
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B' OR "from" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B')
                AND contract_address <>'\x4eb8b4c65d8430647586cf44af4bf23ded2bb794' --need to omit anything that was airdropped into the pool
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B'
       AND qty>0  
    UNION
    --LUSD3CRV
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA' OR "from" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA'
       AND qty>0  
    UNION   
        --ALCX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74' OR "from" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74'
       AND qty>0 
    UNION    
        --TCR
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F' OR "from" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F'
       AND qty>0
    UNION    
        --SUSHI
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x0437ac6109e8A366A1F4816edF312A36952DB856' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x0437ac6109e8A366A1F4816edF312A36952DB856' OR "from" = '\x0437ac6109e8A366A1F4816edF312A36952DB856')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x0437ac6109e8A366A1F4816edF312A36952DB856'
       AND qty>0
    UNION    
        --FXS
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x961226B64AD373275130234145b96D100Dc0b655' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x961226B64AD373275130234145b96D100Dc0b655' OR "from" = '\x961226B64AD373275130234145b96D100Dc0b655')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x961226B64AD373275130234145b96D100Dc0b655'
       AND qty>0
    UNION    
        --FOX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC' OR "from" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC'
       AND qty>0
    UNION
    --APW
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9' OR "from" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9'
       AND qty>0
    UNION    
        --SNX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4' OR "from" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4'
       AND qty>0
    UNION
    --GAMMA
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA' OR "from" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA'
       AND qty>0 
    UNION  
    --MYC
        SELECT"date", token_address, p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date", token_address, SUM(qty) OVER (PARTITION BY token_address ORDER BY"date") as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x83D78bf3f861e898cCA47BD076b3839Ab5469d70' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x83D78bf3f861e898cCA47BD076b3839Ab5469d70' OR "from" = '\x83D78bf3f861e898cCA47BD076b3839Ab5469d70')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x83D78bf3f861e898cCA47BD076b3839Ab5469d70'
       AND qty>0
    UNION    
    --WETH
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x06d39e95977349431e3d800d49c63b4d472e10fb' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x06d39e95977349431e3d800d49c63b4d472e10fb' OR "from" = '\x06d39e95977349431e3d800d49c63b4d472e10fb')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x06d39e95977349431e3d800d49c63b4d472e10fb'
       AND qty>0 
     UNION
      --ETH and stETH 
         SELECT"date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol,  (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', block_time) as"date",
                    '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                    SUM(CASE WHEN "to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' THEN value ELSE value *-1 END) as qty 
                FROM ethereum.traces 
                WHERE ("to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' OR "from" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022')
                AND success
                AND NOT ("to" = "from")
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
        UNION
        --stETH
        SELECT DATE_TRUNC('day', call_block_time) as"date",token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT DISTINCT ON (call_block_time::date) call_block_time
            , contract_address as token_address
            , output_0 as qty
                FROM lido."steth_call_balanceOf" where _account = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
                    --order by call_block_time::date desc, call_block_time desc
           )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
    )as t 
    --order by "date" desc, t.pool_symbol asc
    )
    
,  calendar AS  
    (SELECT DISTINCT  i::date as "date", pool_address,pool_symbol,token_address,token_symbol
        FROM pools_and_constituents
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        --ORDER BY "date" desc, pool_symbol asc
        )
  
  , temp AS
  (
 
  SELECT c."date"
  ,c.pool_address
  ,c.pool_symbol
  ,c.token_address
  ,c.token_symbol
  ,pc.qty  
  ,count(pc.qty) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpQty
  FROM calendar c 
  LEFT JOIN pools_and_constituents pc on pc."date" = c."date" AND pc.pool_address = c.pool_address AND pc.token_address = c.token_address
  --order by "date" desc, pool_symbol 
  )
 
    SELECT 
        1 as source
        ,t."date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,ts.total_supply as lp_total_supply
        ,first_value(qty) OVER (PARTITION BY pool_address, token_address, grpQty ORDER BY t."date") AS qty
        FROM  temp t
        INNER JOIN tokemak."view_tokemak_curve_convex_pool_total_supply" ts ON (t.pool_address = ts.address AND ts."date" = t."date")
        --ORDER BY "date" desc, pool_symbol asc
        
            
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_curve_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

--step 9
drop MATERIALIZED VIEW tokemak.view_tokemak_sushiswap_pool_stats_daily cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_sushiswap_pool_stats_daily
(   source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
    --,cumulative_fees
) AS (
    WITH pairs AS(
            SELECT t.token_address, t.symbol as token_symbol,t.token_decimals, t.index,t.pool_address,tl.symbol as pool_symbol, tl.decimals as pool_decimals FROM(
                SELECT token0 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 1 as index FROM sushi."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token0
                UNION
                SELECT token1 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 2 as index FROM sushi."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token1
            ) as t INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = pool_address 
            --ORDER BY pool_symbol, token_symbol
        ),
    pools as (
        SELECT DISTINCT pool_address, pool_decimals FROM pairs
    )
    
    , calendar AS (
        SELECT c.*, p.token_address, p.token_symbol,p.index, p.token_decimals 
        FROM (SELECT i::date as "date"
            ,tl.address as pool_address
            ,tl.symbol as pool_symbol
            ,tl.decimals as pool_decimals
            FROM sushi."Factory_evt_PairCreated" pc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on pc.pair = tl.address
            CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
            WHERE tl.is_pool = true order by "date" desc) c
        INNER JOIN pairs p ON p.pool_address = c.pool_address
        --ORDER BY "date" desc, c.pool_symbol asc
    )
    ,supply AS
        (SELECT 
            "date"
            ,d.pool_address 
            ,SUM(transfer/10^d.pool_decimals) OVER (PARTITION BY d.pool_address ORDER BY "date") AS supply
        FROM (SELECT 
                    date_trunc('day', evt_block_time) AS "date"
                    ,t.pool_address 
                    ,t.pool_decimals
                    ,sum(value) AS transfer
                FROM (SELECT 
                        evt_block_time
                        ,p.pool_address
                        ,p.pool_decimals
                        ,CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN value ELSE -value END as value
                    FROM sushi."Pair_evt_Transfer" t
                    INNER JOIN pools p ON p.pool_address = t.contract_address 
                    WHERE ("from" = '\x0000000000000000000000000000000000000000' OR  "to" = '\x0000000000000000000000000000000000000000')

                    ) AS t GROUP BY 1, 2, 3 --order by "date" desc
            ) AS d --order by "date" desc
        )

    ,reserves AS
        (SELECT c."date"
                ,c.pool_address
                ,c.pool_symbol
                ,c.pool_decimals
                ,c.token_address
                ,c.token_decimals
                ,c.token_symbol
                ,c.index
                ,latest_reserves[c.index+2]/10^c.token_decimals AS reserve
        FROM calendar c 
        LEFT JOIN
            (SELECT date_trunc('day', t.evt_block_time)::date as "date"
                ,tl.address as pool_address
                ,MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1]) AS latest_reserves
                FROM sushi."Pair_evt_Sync" t 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = t.contract_address 
                GROUP BY 1, 2 ORDER BY "date" desc) dr ON c.pool_address = dr.pool_address and dr."date" = c."date")

    /*,fees AS
        (SELECT 
            c."date"
            ,c.pool_address
            ,c.token_address
            ,c.token_decimals
            ,c.index
            ,CASE WHEN c.index = 1 THEN (0.003*sum("amount0In"/10^c.token_decimals)) ELSE (0.003*sum("amount1In"/10^c.token_decimals)) END AS token_fees 
        FROM calendar c LEFT JOIN
        sushi."Pair_evt_Swap" t  ON c.pool_address = t.contract_address and c."date" = t."evt_block_time"::date
        GROUP BY 1,2,3,4,5) */
        
    ,temp_table AS
        (SELECT DISTINCT
            c."date"
            ,c.pool_address
            ,c.pool_symbol
            ,c.token_address 
            ,c.token_symbol 
            ,r.reserve
            ,s.supply
            --,f.token_fees
            ,count(s.supply) OVER (PARTITION BY c.pool_address ORDER BY c."date") AS grpSupply
            ,count(r.reserve) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpRes
        FROM calendar c 
        LEFT JOIN supply s ON s.pool_address = c.pool_address AND c."date" = s."date"
        INNER JOIN reserves r ON c."date" = r."date" AND r.token_address = c.token_address AND r.pool_address = c.pool_address
        --INNER JOIN fees f ON c."date"=f."date" AND f.token_address = c.token_address AND f.pool_address = c.pool_address
        --ORDER BY c."date" desc, pool_symbol asc, token_symbol asc
        )

    SELECT 
        3
        ,"date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,first_value(supply) OVER (PARTITION BY pool_address, grpSupply ORDER BY "date") AS supply
        ,first_value(reserve) OVER (PARTITION BY pool_address, token_address, grpRes ORDER BY "date") AS reserve
        --,sum(token_fees) OVER (PARTITION BY pool_address,token_address ORDER BY "date") AS cumulative_fees
            FROM  temp_table order by "date" desc, pool_symbol asc, token_symbol asc
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_sushiswap_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

--step 10
drop MATERIALIZED VIEW tokemak.view_tokemak_uniswap_pool_stats_daily cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_uniswap_pool_stats_daily
(
    source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
    --,cumulative_fees
) AS (
    WITH pairs AS(
            SELECT t.token_address, t.symbol as token_symbol,t.token_decimals, t.index,t.pool_address,tl.symbol as pool_symbol, tl.decimals as pool_decimals FROM(
                SELECT token0 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 1 as index FROM uniswap_V2."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token0
                UNION
                SELECT token1 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 2 as index FROM uniswap_V2."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token1
            ) as t INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = pool_address 
            --ORDER BY pool_symbol, token_symbol
        ),
    pools as (
        SELECT DISTINCT pool_address, pool_decimals FROM pairs
    )
    , calendar AS (
        SELECT c.*, p.token_address, p.token_symbol,p.index, p.token_decimals 
        FROM (SELECT i::date as "date"
            ,tl.address as pool_address
            ,tl.symbol as pool_symbol
            ,tl.decimals as pool_decimals
            FROM uniswap_V2."Factory_evt_PairCreated" pc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on pc.pair = tl.address
            CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
            WHERE tl.is_pool = true --order by "date" desc
            ) c
        INNER JOIN pairs p ON p.pool_address = c.pool_address
        --ORDER BY "date" desc, c.pool_symbol asc
    )
    ,supply AS
        (SELECT 
            "date"
            ,d.pool_address 
            ,SUM(transfer/10^d.pool_decimals) OVER (PARTITION BY d.pool_address ORDER BY "date") AS supply
        FROM (SELECT 
                    date_trunc('day', evt_block_time) AS "date"
                    ,t.pool_address 
                    ,t.pool_decimals
                    ,sum(value) AS transfer
                FROM (SELECT 
                        evt_block_time
                        ,p.pool_address
                        ,p.pool_decimals
                        ,CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN value ELSE -value END as value
                    FROM uniswap_V2."Pair_evt_Transfer" t
                    INNER JOIN pools p ON p.pool_address = t.contract_address 
                    WHERE ("from" = '\x0000000000000000000000000000000000000000' OR  "to" = '\x0000000000000000000000000000000000000000')

                    ) AS t GROUP BY 1, 2, 3 --order by "date" desc
            ) AS d --order by "date" desc
        )

    ,reserves AS
        (SELECT c."date"
                ,c.pool_address
                ,c.pool_symbol
                ,c.pool_decimals
                ,c.token_address
                ,c.token_decimals
                ,c.token_symbol
                ,c.index
                ,latest_reserves[c.index+2]/10^c.token_decimals AS reserve
        FROM calendar c 
        LEFT JOIN
            (SELECT date_trunc('day', t.evt_block_time)::date as "date"
                ,tl.address as pool_address
                ,MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1]) AS latest_reserves
                FROM uniswap_V2."Pair_evt_Sync" t 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = t.contract_address 
                --WHERE "evt_block_time">'8/1/2021'
                GROUP BY 1, 2 ORDER BY "date" desc
                ) dr ON c.pool_address = dr.pool_address and dr."date" = c."date")
--SELECT * from reserves
    /*,fees AS
        (SELECT 
            c."date"
            ,c.pool_address
            ,c.token_address
            ,c.token_decimals
            ,c.index
            ,CASE WHEN c.index = 1 THEN (0.003*sum("amount0In"/10^c.token_decimals)) ELSE (0.003*sum("amount1In"/10^c.token_decimals)) END AS token_fees 
        FROM calendar c LEFT JOIN
        uniswap_V2."Pair_evt_Swap" t  ON c.pool_address = t.contract_address and c."date" =  date_trunc('day', t.evt_block_time)::date
        GROUP BY 1,2,3,4,5) */

    ,temp_table AS
        (SELECT DISTINCT
            c."date"
            ,c.pool_address
            ,c.pool_symbol
            ,c.token_address 
            ,c.token_symbol 
            ,r.reserve
            ,s.supply
            --,f.token_fees
            ,count(s.supply) OVER (PARTITION BY c.pool_address ORDER BY c."date") AS grpSupply
            ,count(r.reserve) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpRes
        FROM calendar c 
        LEFT JOIN supply s ON s.pool_address = c.pool_address AND c."date" = s."date"
        INNER JOIN reserves r ON c."date" = r."date" AND r.token_address = c.token_address AND r.pool_address = c.pool_address
        --INNER JOIN fees f ON c."date"=f."date" AND f.token_address = c.token_address AND f.pool_address = c.pool_address
        --ORDER BY c."date" desc, pool_symbol asc, token_symbol asc
        )

    SELECT 
        4
        ,"date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,first_value(supply) OVER (PARTITION BY pool_address, grpSupply ORDER BY "date") AS supply
        ,first_value(reserve) OVER (PARTITION BY pool_address, token_address, grpRes ORDER BY "date") AS reserve
        --,sum(token_fees) OVER (PARTITION BY pool_address,token_address ORDER BY "date") AS cumulative_fees
            FROM  temp_table order by "date" desc, pool_symbol asc, token_symbol asc
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_uniswap_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

--step 11
drop MATERIALIZED VIEW tokemak.view_tokemak_wallet_balances_daily cascade
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_wallet_balances_daily
(
    "date", source_name, wallet_address, token_address, symbol, display_name, tokemak_qty
) AS (
WITH calendar AS  
        (SELECT i::date as "date"
            ,s as source
            ,a.address as wallet_address
            ,tl.address
            ,tl.symbol
            ,tl.display_name
        FROM tokemak."view_tokemak_lookup_tokens" tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
        CROSS JOIN generate_series(0,4,1) tt(s) 
        CROSS JOIN (SELECT address from tokemak."view_tokemak_addresses") as a 
        --WHERE NOT (i>'2022-05-10' AND (tl.address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD' OR tl.address='\xa693b19d2931d498c5b318df961919bb4aee87a5' OR tl.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'))--remove UST tokens and pools
 ) ,
 result AS (
    SELECT "date", source, wallet_address, token_address, symbol, display_name, sum(balance)  as balance FROM (
        SELECT "date", source, wallet_address, token_address, symbol, display_name, sum(balance) as balance --OVER (PARTITION BY source, symbol ORDER BY "date") as balance 
            FROM (
            SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.wallet_address, b.token_address)
            date_trunc('day', "timestamp") as "date",
            CASE WHEN starts_with(tl.display_name , 'Curve.fi') THEN 1 
            WHEN starts_with(tl.symbol, 'SUSHI-') THEN 3
            WHEN starts_with(tl.symbol, 'UNI-V2-') THEN 4
            ELSE 0 END as source,
            b.wallet_address,
            b.token_address,
            tl.symbol as symbol,
            tl.display_name,
            b.amount_raw/10^tl.decimals as balance
            FROM erc20."token_balances" b   --AND b.wallet_address='\x8b4334d4812c530574bd4f2763fcd22de94a969b' 
            --order by "timestamp" desc
            INNER JOIN tokemak."view_tokemak_addresses" ta ON ta.address = b.wallet_address
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON b.token_address = tl.address  
            -- WHERE NOT (date_trunc('day', "timestamp")::date >'2022-05-08' AND (tl.address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD' OR tl.address='\xa693b19d2931d498c5b318df961919bb4aee87a5' OR tl.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'))--remove UST tokens and pools
            ORDER BY "date" desc , b.wallet_address, b.token_address, "timestamp" desc NULLS LAST
            ) as t  GROUP BY 1,2,3,4,5,6 
       -- ORDER BY "date" desc, source, symbol
        UNION
        --ETHER
        SELECT "date", source,wallet_address, token_address, symbol, display_name, SUM(balance) OVER (PARTITION BY wallet_address,symbol ORDER BY "date") as balance FROM (
                SELECT
                date_trunc('day', "block_time") as "date"
                ,0 as source, --0 mean undefined which is our wallets
                '\x8b4334d4812c530574bd4f2763fcd22de94a969b'::bytea as wallet_address,
                '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                 tl.symbol as symbol,
                tl.display_name,
                SUM(CASE WHEN ("to" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b')
                    THEN value/10^tl.decimals 
                    ELSE -value/10^tl.decimals  END) as balance 
                FROM ethereum.traces 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON  tl.address ='\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
                WHERE ("to" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b' OR "from" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b') 
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null) --order by "date" desc
                GROUP BY 1,2,3,4,5,6 --ORDER BY"date" desc
            UNION
                SELECT
                date_trunc('day', "block_time") as "date"
                ,0 as source, --0 mean undefined which is our wallets
                '\xa86e412109f77c45a3bc1c5870b880492fb86a14'::bytea as wallet_address,
                '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                 tl.symbol as symbol,
                tl.display_name,
                SUM(CASE WHEN ("to" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14')
                    THEN value/10^tl.decimals 
                    ELSE -value/10^tl.decimals  END) as balance 
                FROM ethereum.traces 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON  tl.address ='\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
                WHERE ("to" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14' OR "from" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14') 
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null) --order by "date" desc
                GROUP BY 1,2,3,4,5,6 --ORDER BY"date" desc
                ) as t 
        UNION
        --Masterchef v1
        SELECT "date", 3 as source,wallet_address, contract_address as token_address, symbol,display_name, Sum(amount) OVER (PARTITION BY wallet_address,symbol  ORDER BY "date") as balance from (
            SELECT date_trunc('day', d."evt_block_time") as "date",t."from" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) as Amount FROM sushi."MasterChef_evt_Deposit" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address  AND t."to" = '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address 
            GROUP BY "date",t."from",t.contract_address,tl.symbol,tl.display_name
            UNION
            SELECT date_trunc('day', d."evt_block_time") as "date",t."to" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) *-1 as Amount FROM sushi."MasterChef_evt_Withdraw" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address  AND t."from" = '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address   
            GROUP BY "date",t."to",t.contract_address,tl.symbol,tl.display_name) as t
        --GROUP BY "date", source, t.contract_address,symbol,display_name 
        --order by "date" desc, symbol
        UNION
        --Masterchefv2 
        SELECT "date", 3 as source,wallet_address, contract_address as token_address, symbol,display_name, Sum(amount) OVER (PARTITION BY wallet_address,symbol  ORDER BY "date") as balance from (
            SELECT date_trunc('day', d."evt_block_time") as "date",t."from" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) as Amount FROM sushi."MasterChefV2_evt_Deposit" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address  AND t."to" = '\xef0881ec094552b2e128cf945ef17a6752b4ec5d'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address 
            GROUP BY "date",t."from",t.contract_address,tl.symbol,tl.display_name
            UNION
            SELECT date_trunc('day', d."evt_block_time") as "date",t."to" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) *-1 as Amount FROM sushi."MasterChefV2_evt_Withdraw" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address  AND t."from" = '\xef0881ec094552b2e128cf945ef17a6752b4ec5d'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address   
            GROUP BY "date",t."to",t.contract_address,tl.symbol,tl.display_name) as t
        --GROUP BY source, contract_address,symbol,display_name
        --order by "date" desc, symbol
        UNION
        --CONVEX
        SELECT "date", source,wallet_address, token_address, symbol,display_name, sum(qty) OVER (PARTITION BY wallet_address,symbol ORDER BY "date")as balance 
        FROM (
                SELECT "date", 2 as source,wallet_address, contract_address as token_address, symbol,display_name, sum(qty) as qty FROM (
                    SELECT date_trunc('day', t."evt_block_time") as "date",t."to" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM((value/10^tl.decimals)*-1) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address and t."from"='\xF403C135812408BFbE8713b5A23a04b3D48AAE31'
                    GROUP BY 1,2,3,4,5
                    UNION 
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address and t."to"='\x989aeb4d175e16225e39e87d0d97a3360524ad80'
                    GROUP BY 1,2,3,4,5
                    UNION
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address and t."to"='\x72a19342e8f1838460ebfccef09f6585e32db86e' --voting escrow deposit
                    GROUP BY 1,2,3,4,5
                    UNION
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals*-1) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address and t."from"='\x72a19342e8f1838460ebfccef09f6585e32db86e' --voting escrow withdrawal??  not sure about this as we have never withdrawn.  NEED TO VERIFY
                    GROUP BY 1,2,3,4,5
                )as t GROUP BY 1,2,3,4,5,6
            )as t --GROUP BY source, contract_address, symbol, display_name 
            ORDER BY "date" desc, symbol
        ) as t
        GROUP BY 1,2,3,4,5,6 
 --       ORDER BY "date" desc, source, symbol 
    ),
   -- SELECT * FROM result order  by "date" desc, symbol
    
temp_table AS ( 
        SELECT 
            c."date"
            ,c.source
            ,c.wallet_address
            , c.address
            , c.symbol
            , c.display_name
            , r.balance
            , count(balance) OVER (PARTITION BY c.source,c.wallet_address, c.address ORDER BY c."date") AS grpBalance
        FROM calendar c 
        LEFT JOIN result r on c."date"=r."date"  and c.address= r.token_address and  c.source=r.source AND c.wallet_address = r.wallet_address),
--SELECT * FROM temp_table WHERE balance >0 order by "date"desc, symbol 
  res_temp AS(    
    SELECT 
        "date"::date
        ,source
        ,wallet_address
        ,address
        ,symbol
        ,display_name
        ,first_value(balance) OVER (PARTITION BY source,wallet_address, address, grpBalance ORDER BY "date") AS tokemak_qty
    FROM  temp_table 
    order by "date" desc, source, symbol)
    
SELECT "date", s.source_name, wallet_address, address, symbol, display_name, tokemak_qty 
FROM res_temp t 
INNER JOIN tokemak."view_tokemak_lookup_sources" s on s.id = t.source
WHERE tokemak_qty >0 
ORDER BY "date" desc, source, wallet_address,symbol
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_wallet_balances_daily (
   "date",
   source_name,
   wallet_address,
   token_address
);

--step 12
drop MATERIALIZED VIEW tokemak.view_tokemak_prices_usd_eth_daily cascade
;
CREATE MATERIALIZED VIEW tokemak.view_tokemak_prices_usd_eth_daily
(
    "date", contract_address,pricing_contract,symbol, price_usd, price_eth
)
AS
(
WITH contracts as(
--select our tokens and then select the tokens which match to a pricing contract so they are in one table
    SELECT DISTINCT address, pricing_contract, symbol FROM (
        SELECT DISTINCT address, pricing_contract, symbol FROM tokemak."view_tokemak_lookup_tokens" WHERE is_pool = false and pricing_contract <> ''
        UNION 
        SELECT DISTINCT address, address as pricing_contract, symbol FROM tokemak."view_tokemak_lookup_tokens" WHERE is_pool = false and pricing_contract = ''
        )as t ORDER BY symbol, address, pricing_contract
),
calendar AS  
        (SELECT i::date as "date"
            ,c.address
            ,c.pricing_contract
            ,c.symbol
        FROM contracts c
        CROSS JOIN generate_series('2021-08-01'::date at time zone 'UTC', current_date, '1 day') t(i) 
 ) , 

main_prices as (
    SELECT DISTINCT ON(date_trunc('day', "minute"), p.contract_address)
    date_trunc('day', "minute") as "date"
    , p.contract_address as pricing_contract
    , p.price
    from prices."usd" p 
    INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    WHERE minute > '8/1/2021' and price > .0001 and p.contract_address != '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
    ORDER BY "date" desc,p.contract_address, "minute" desc NULLS LAST),

dex_prices as (
    SELECT DISTINCT ON(date_trunc('day', "hour"), p.contract_address)
    date_trunc('day', "hour") as "date"
    , p.contract_address as pricing_contract
    , p.median_price as price
    from prices."prices_from_dex_data" p INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    where "hour" > '8/1/2021' and median_price > .0001 and p.contract_address != '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
    ORDER BY "date" desc,p.contract_address, "hour" desc NULLS LAST
),
steth_prices as (
    select 
        DISTINCT ON(date_trunc('day', "evt_block_time"))
        '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea as pricing_contract,
        date_trunc('day', "evt_block_time") as "date", 
        date_trunc('minute', "evt_block_time") as "minute", 
        CASE WHEN date_trunc('day', "evt_block_time")::date = '2022-05-11'::date THEN .978 ELSE price END as price -- this is done to reflect our exit price so our pca is not overstated
    from (
        select 
            evt_block_time,
            "tokens_sold"/"tokens_bought" as price
        from curvefi."steth_swap_evt_TokenExchange"
        where sold_id = 0  
        union 
        select
            evt_block_time,
            "tokens_bought"/"tokens_sold" as price
        from curvefi."steth_swap_evt_TokenExchange"
        where sold_id = 1 and "tokens_bought" > 0 order by evt_block_time desc
        ) as p
     ORDER BY "date" desc, "minute"  desc NULLS LAST
),
temp as (
    SELECT "date", t.pricing_contract, MAX(price) as price_usd FROM (
        SELECT "date", pricing_contract, price  FROM dex_prices
        UNION
        SELECT "date", pricing_contract, price  FROM main_prices
        UNION
        SELECT "date", pricing_contract, price  FROM steth_prices
    ) as t
     GROUP BY "date", pricing_contract
)
,
eth_prices as (
    SELECT "date", pricing_contract, price_usd as price FROM temp where pricing_contract = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
),
temp1 as (SELECT c."date"
, c.address
, c.pricing_contract
, c.symbol
, CASE WHEN c.pricing_contract = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea THEN t.price_usd * e.price ELSE t.price_usd END as price_usd
, CASE WHEN c.pricing_contract = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea THEN t.price_usd ELSE t.price_usd/e.price END as price_eth
FROM calendar c
LEFT JOIN eth_prices e on e."date" = c."date"
LEFT JOIN temp t on t."date" = c."date" AND t.pricing_contract = c.pricing_contract
order by "date" desc, symbol asc

),
temp2 as (
    SELECT 
    "date"
    , address
    , pricing_contract
    , symbol
    , price_usd
    , price_eth
    , count(price_usd) OVER (PARTITION BY address ORDER BY "date") AS grpUSD
    , count(price_eth) OVER (PARTITION BY address ORDER BY "date") AS grpETH
    FROM temp1
    order by "date" desc, symbol asc
)

    SELECT
    "date"
    , address
    , pricing_contract
    , symbol
    , first_value(price_usd) OVER (PARTITION BY address, grpUSD ORDER BY "date") AS price_usd
    , first_value(price_eth) OVER (PARTITION BY address, grpETH ORDER BY "date") AS price_eth
    FROM temp2 ORDER BY "date" desc, symbol

);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_prices_usd_eth_daily (
   "date",
   contract_address
);


--step 13
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_deployed_asset_balances_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_deployed_asset_balances_daily (
    "date"
    ,source_name
    ,pool_address
    ,pool_symbol
    ,total_lp_supply
    ,tokemak_lp_wallet_qty
    ,tokemak_lp_ownership_pct
    ,token_address
    ,token_symbol
    ,pool_reserve_qty
    ,tokemak_pool_reserve_qty  
    ,price_usd
    ,value_usd
    ,price_eth
    ,value_eth
)
AS
( 
   WITH combined as (
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_uniswap_pool_stats_daily"
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_sushiswap_pool_stats_daily" 
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_curve_pool_stats_daily"
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_convex_pool_stats_daily" order by "date" desc, pool_symbol, token_symbol
        ),
base as (        
    SELECT 
        t."date"
        ,ls.source_name
        ,t.pool_address
        ,t.pool_symbol
        ,t.total_lp_supply
        ,wb.tokemak_qty as tokemak_lp_wallet_qty
        ,(wb.tokemak_qty/t.total_lp_supply) as tokemak_lp_ownership_pct
        ,t.token_address
        ,t.token_symbol
        ,t.reserve as pool_reserve_qty
        --,wb1.tokemak_qty as tokemak_reserve_wallet_qty
        ,(t.reserve * (wb.tokemak_qty/t.total_lp_supply)) as tokemak_pool_reserve_qty        
        FROM combined as t  
            INNER JOIN tokemak."view_tokemak_lookup_sources" ls on ls.id = t.source
            LEFT JOIN 
            (SELECT  "date", source_name, token_address, symbol, display_name, SUM(tokemak_qty) as tokemak_qty
            FROM tokemak."view_tokemak_wallet_balances_daily" 
            GROUP BY 1,2,3,4,5
            ORDER BY "date" desc, source_name, symbol) wb 
            ON wb."date" = t."date" AND t.pool_address = wb.token_address AND wb.source_name = ls.source_name
            ORDER BY t."date" desc, source_name asc, pool_symbol asc, token_symbol asc 

)
--SELECT * FROM base
,
pool_balances as (
    SELECT "date"
        ,source_name
        ,pool_address
        ,pool_symbol
        ,total_lp_supply
        ,tokemak_lp_wallet_qty
        ,tokemak_lp_ownership_pct
        ,token_address
        ,token_symbol
        ,pool_reserve_qty
        ,tokemak_pool_reserve_qty    FROM base 
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = token_address AND tl.is_liability = false --remove liabilities
        order by "date" desc, source_name, pool_symbol 
        )

 , temp_balances AS(
 
    SELECT "date", source_name, token_address, bpool_address, btoken_symbol as token_symbol, sum(tokemak_qty) as tokemak_qty
    FROM (SELECT b."date",b.source_name,b.pool_address as bpool_address, b.pool_symbol as bpool_symbol,t.pool_address as tpool_address, 
        t.pool_symbol as tpool_symbol, b.token_address,t.token_symbol as ttoken_symbol,b.token_symbol as btoken_symbol,
        (t.tokemak_pool_reserve_qty/b.total_lp_supply)*b.pool_reserve_qty as tokemak_qty
        FROM
        pool_balances  b INNER JOIN pool_balances t ON t.token_address = b.pool_address and t.source_name = b.source_name and b."date" = t."date") as t
        GROUP BY "date", source_name, token_address,bpool_address, btoken_symbol 
        ORDER BY "date" desc, source_name, token_address,bpool_address, btoken_symbol 
 )
--SELECT * FROM temp_balances
    SELECT 
    p."date"
        ,p.source_name
        ,p.pool_address
        ,p.pool_symbol
        ,p.total_lp_supply
        ,p.tokemak_lp_wallet_qty
        ,p.tokemak_lp_ownership_pct
        ,p.token_address
        ,p.token_symbol
        , p.pool_reserve_qty
        , COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0) as  tokemak_pool_reserve_qty
       , tp.price_usd
       , (COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0)) * COALESCE(tp.price_usd,0) as value_usd
       , tp.price_eth
      , (COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0)) * COALESCE(tp.price_eth,0) as value_eth
        FROM pool_balances p
        LEFT JOIN temp_balances t on t."date" = p."date" AND t."source_name" = p."source_name" AND p.token_address = t.token_address and p.pool_address = t.bpool_address
        LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = p."token_address" and tp."date" = p."date"
        ORDER BY p."date" desc, p."source_name", p."pool_symbol", p."token_symbol"

 );
 CREATE UNIQUE INDEX ON tokemak.view_tokemak_deployed_asset_balances_daily (
   "date",
   source_name,
   pool_address,
   token_address
);

--step 14
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_reactor_balances_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_reactor_balances_daily
(
    "date", token_address, token_symbol, token_display_name, reactor_name, reactor_address, is_deployable, reactor_qty--,reactor_gross_value_usd, reactor_gross_value_eth
) AS (

WITH calendar AS  
        (SELECT i::date as "date"
            ,r.reactor_address as reactor_address
            ,r.reactor_name ,r.is_deployable
            ,r.underlyer_address as address
            ,tl.symbol
            ,tl.display_name
            
        FROM tokemak."view_tokemak_lookup_reactors" r  
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = r.underlyer_address
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
 ) , 

reactor_underlyer_balances as (
    SELECT  "date",token_address, symbol, display_name,reactor_name,reactor_address,is_deployable, sum(balance) as reactor_qty FROM (
        SELECT DISTINCT ON(date_trunc('day', "timestamp"),b.wallet_address, b.token_address)
        date_trunc('day', "timestamp") as "date",
        b.wallet_address,
        b.token_address,
        r.reactor_name,
        r.reactor_address,
        r.is_deployable,
        tl.symbol as symbol,
        tl.display_name,
        b.amount_raw/10^tl.decimals as balance
        FROM erc20."token_balances" b  
        INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.reactor_address = b.wallet_address AND r.underlyer_address = b.token_address AND r.underlyer_address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'  -- only weth
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = b.token_address
        ORDER BY "date" desc,b.wallet_address, b.token_address, "timestamp" desc NULLS LAST
        ) as t 
    GROUP BY "date",token_address, symbol, display_name,reactor_name, reactor_address, is_deployable
    
    UNION    --need to do this because weth is only close to being correct in the aggregated table "token_balances" but our other tokens are only correct from the evt transfer table
     select "date", underlyer_address,symbol,display_name, reactor_name, reactor_address,is_deployable, SUM(amount) OVER (PARTITION BY  reactor_address  ORDER BY "date") as reactor_qty FROM (
        select "date", t.underlyer_address,tl.symbol,tl.display_name, t.reactor_name, t.reactor_address,t.is_deployable,  SUM(amount/10^tl.decimals) as amount  from 
            ( 
                select date_trunc('day', "evt_block_time") as "date", r.reactor_name,r.reactor_address,r.underlyer_address,r.is_deployable, "to",
                    SUM(value) as amount
                from erc20."ERC20_evt_Transfer" t
                INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.underlyer_address = t.contract_address and r.reactor_address = "to" AND r.underlyer_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                WHERE NOT (t."to" = t."from")
                GROUP BY 1,2,3,4,5,6
                union
                select date_trunc('day', "evt_block_time") as "date", r.reactor_name,r.reactor_address,r.underlyer_address,r.is_deployable,"from",
                    SUM(-value) as amount
                from erc20."ERC20_evt_Transfer" t
                INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.underlyer_address = t.contract_address and r.reactor_address = "from" AND r.underlyer_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                WHERE NOT (t."to" = t."from")
                GROUP BY 1,2,3,4,5,6
            ) t INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = underlyer_address and tl.symbol <> ''
           group by 1,2,3,4,5,6,7
     ) as t  order by "date" desc, reactor_name
) ,

temp as (

    SELECT  c."date", c.address as token_address, c.symbol as token_symbol
    , c.display_name as token_display_name
    , c.reactor_name
    , c.reactor_address
    , c.is_deployable
    , r.reactor_qty
    , count(r.reactor_qty) OVER (PARTITION BY c.reactor_address ORDER BY c."date") AS grpQty
    FROM calendar c 
    LEFT JOIN reactor_underlyer_balances r on c."date" = r."date" and c.address = r.token_address and c.reactor_address = r.reactor_address
    GROUP BY 1,2,3,4,5,6,7,8
    ORDER BY c."date" desc, reactor_name, c.symbol
    ),

  res_temp AS(    
    SELECT 
        "date"
        , token_address
        , token_symbol
        , token_display_name
        , reactor_name
        , reactor_address
        , is_deployable
        ,first_value(reactor_qty) OVER (PARTITION BY reactor_address, grpQty ORDER BY "date") AS reactor_qty
    FROM  temp
    order by "date" desc, reactor_name)

    SELECT  r."date", token_address, token_symbol, token_display_name, reactor_name, reactor_address, is_deployable, reactor_qty 
    FROM res_temp r
    WHERE reactor_qty <> 0 order by "date" desc, reactor_name
 
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_reactor_balances_daily (
   "date",
   token_address,
   reactor_address
);

--step 15
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_all_deployable_assets_by_asset_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_all_deployable_assets_by_asset_daily
(
    "date", token_symbol,token_address, total_qty--,total_value_usd,total_value_eth
)
AS (
    SELECT "date", token_symbol, token_address, SUM(total_qty) as total_qty--, SUM(total_value_usd) as total_value_usd, SUM(total_value_eth) as total_value_eth 
    FROM(
        SELECT "date",
        token_symbol,token_address
       ,tokemak_pool_reserve_qty as total_qty
       --,value_usd as total_value_usd
       --,value_eth as total_value_eth
        FROM tokemak."view_tokemak_deployed_asset_balances_daily" 
        UNION
        SELECT "date",
         token_symbol,token_address
        ,reactor_qty AS total_qty
        --,reactor_gross_value_usd as total_value_usd
        --,reactor_gross_value_eth as total_value_eth
        FROM tokemak."view_tokemak_reactor_balances_daily" b 
        WHERE b.is_deployable = true 
    ) as t GROUP BY 1,2,3 ORDER BY "date" desc, token_symbol
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_all_deployable_assets_by_asset_daily (
   "date", token_address
);


--step 16
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_outstanding_liabilities_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_outstanding_liabilities_daily
(
    "date", token_address, pricing_contract, symbol, is_dollar_stable, total_liability_qty,price_usd,price_eth,total_liability_value_usd, total_liability_value_eth
)
AS ( 

WITH calendar AS  
        (SELECT i::date as "date"
            ,tl.address
            ,tl.pricing_contract
            ,tl.is_dollar_stable
            ,tl.symbol
        FROM tokemak.view_tokemak_lookup_tokens tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
        WHERE tl.is_liability = true --AND NOT (i>'2022-05-10' AND (tl.address='\x7A75ec20249570c935Ec93403A2B840fBdAC63fd' OR tl.address='\x482258099de8de2d0bda84215864800ea7e6b03d')) order by "date" desc
 ) ,
minted as (
    SELECT 
        "date", address, pricing_contract, is_dollar_stable,symbol
        ,first_value(balance) OVER (PARTITION BY address, grpBalance ORDER BY "date") AS total_liability_qty
    FROM(
         SELECT c."date", c.address, c.pricing_contract, c.is_dollar_stable,c.symbol,r.balance,
            count(r.balance) OVER (PARTITION BY c.address ORDER BY c."date") AS grpBalance
                FROM calendar c 
                LEFT JOIN (
                 SELECT "date", address, pricing_contract, is_dollar_stable,symbol, SUM(amount) OVER (PARTITION BY address  ORDER BY "date") as balance FROM (
                     SELECT "date", address, pricing_contract,is_dollar_stable, symbol, Sum(amount) as amount from (
                        SELECT date_trunc('day',"evt_block_time") as "date", tl.address, tl.pricing_contract,tl.is_dollar_stable,  tl.symbol,
                        CASE WHEN tr."from" = '\x0000000000000000000000000000000000000000' THEN 
                                    value/10^tl.decimals
                             WHEN tr."to" = '\x0000000000000000000000000000000000000000'  THEN  
                                    -value/10^tl.decimals 
                             ELSE 
                                0
                             END as amount
                        FROM erc20."ERC20_evt_Transfer" tr
                        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tr.contract_address = tl.address  AND  tl.is_liability = true 
                        WHERE tr."from" = '\x0000000000000000000000000000000000000000' OR tr."to" = '\x0000000000000000000000000000000000000000' 
                        AND NOT (tr."to" = tr."from") 
                        ) as t GROUP BY 1,2,3,4,5 
                    ) as tt ORDER BY "date" desc, symbol
                ) as r ON r."date" = c."date" and r.address = c.address
            ORDER BY "date" desc, symbol
    ) as result ORDER BY "date" desc, symbol
 ),

pools_and_wallets as (
 --liabilities minted but in our wallets
     SELECT 
        "date", address, pricing_contract, is_dollar_stable,symbol
        ,first_value(balance) OVER (PARTITION BY address, grpBalance ORDER BY "date") AS total_liability_qty
    FROM(
         SELECT c."date", c.address, c.pricing_contract, c.is_dollar_stable,c.symbol,r.balance,
            count(r.balance) OVER (PARTITION BY c.address ORDER BY c."date") AS grpBalance
            FROM calendar c 
            LEFT JOIN (
                SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, SUM(balance) as balance FROM 
                (
                 SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, sum(-balance) as balance 
                        FROM (
                            SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.token_address)
                            date_trunc('day', "timestamp") as "date",
                            b.token_address,
                            tl.pricing_contract,
                            tl.is_dollar_stable,
                            tl.symbol as symbol,
                            tl.display_name,
                            b.amount_raw/10^tl.decimals as balance
                            FROM erc20."token_balances" b   --AND b.wallet_address='\x8b4334d4812c530574bd4f2763fcd22de94a969b' 
                            --order by "timestamp" desc
                            INNER JOIN tokemak."view_tokemak_addresses" ta ON ta.address = b.wallet_address
                            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON b.token_address = tl.address and tl.is_liability=true
                            ORDER BY "date" desc ,b.token_address, "timestamp" desc NULLS LAST
                        ) as t  GROUP BY 1,2,3,4,5 --order by "date" desc, symbol
                  UNION
                  SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, sum(-balance) as balance 
                                FROM (
                                SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.token_address)
                                date_trunc('day', "timestamp") as "date",
                                b.token_address,
                                t.pricing_contract,
                                t.is_dollar_stable,
                                t.symbol as symbol,
                                t.display_name,
                                b.amount_raw/10^t.decimals as balance
                        FROM erc20."token_balances" b  
                        INNER JOIN tokemak."view_tokemak_lookup_tokens" t ON b.token_address = t.address AND is_liability=true 
                        WHERE  EXISTS (SELECT address FROM tokemak.view_tokemak_lookup_tokens tl WHERE  b.wallet_address = tl.address and tl.is_pool=true)
                        ORDER BY  "date" desc ,b.token_address, "timestamp" desc NULLS LAST
                        ) as t GROUP BY 1,2,3,4,5
                ) as tt 
                GROUP BY 1,2,3,4,5 ORDER BY "date" desc, symbol
            ) as r ON r."date" = c."date" AND r."token_address"=c."address"
        ) as result ORDER BY "date" desc, symbol
 )

SELECT m."date", m.address, m.pricing_contract, m.symbol,m.is_dollar_stable, COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0) as total_liability_qty, 
COALESCE(p.price_usd, 0) as price,
COALESCE(p.price_eth, 0) as price_eth,
(COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0)) * COALESCE(p.price_usd, 0) as total_liability_value_usd,
(COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0)) * COALESCE(p.price_eth, 0) as total_liability_value_eth  
FROM minted m LEFT JOIN pools_and_wallets wp on wp."date" = m."date" and wp.address = m.address
LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" p ON m.address = p.contract_address and m."date" = p."date"
ORDER BY m."date" desc, m.symbol
    
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_outstanding_liabilities_daily (
   "date",
   token_address
);

--step 17
DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_PCAs_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_PCAs_daily
(
    "date", asset_symbol, token_address, total_asset_qty, total_liability_qty, total_liability_value_usd,total_liability_value_eth,total_asset_value_usd,total_asset_value_eth, pca_value_usd, pca_value_eth, pca_qty
)
AS (
   WITH liabilities_daily as (
        SELECT "date",symbol, pricing_contract, SUM(total_liability_qty) as total_liability_qty
        FROM tokemak."view_tokemak_outstanding_liabilities_daily" 
        GROUP BY 1,2,3
    ),
    assets_daily as(
        SELECT "date",symbol, token_address,  SUM(total_qty) as total_qty FROM (
            SELECT "date", tl.symbol, a.token_address,tl.pricing_contract, total_qty
            ,tl.is_dollar_stable 
            FROM tokemak."view_tokemak_all_deployable_assets_by_asset_daily" a 
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = a.token_address and is_liability=false 
        UNION
        SELECT "date",tl.symbol, token_address, tl.pricing_contract, tokemak_qty as total_qty
        , tl.is_dollar_stable  
            FROM (
            SELECT  b."date", b.wallet_address, b.token_address, b.source_name, b.symbol, tl.address, b.display_name, b.tokemak_qty 
                FROM tokemak."view_tokemak_wallet_balances_daily" b INNER JOIN 
                tokemak.view_tokemak_lookup_tokens tl on tl.address = b.token_address AND tl.is_pool = false AND tl.symbol <>'TOKE'
                LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = b."token_address" and tp."date" = b."date"
                WHERE tl.is_pool = false and tl.symbol <>'TOKE' 
                ORDER BY source_name, symbol, wallet_address
            ) b
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = b.token_address and is_pool = false and is_liability=false ) as t GROUP BY 1,2,3
    ),

temp_combined as (
    SELECT a."date",a.symbol as asset_symbol
    , a.token_address as token_address
    , a.total_qty as total_asset_qty
    , l.total_liability_qty as total_liability_qty
   , a.total_qty * tp.price_usd as  total_asset_value_usd
    , a.total_qty * tp.price_eth as total_asset_value_eth
    , l.total_liability_qty * tp.price_usd as total_liability_value_usd
    , l.total_liability_qty * tp.price_eth as total_liability_value_eth
    , COALESCE(a.total_qty * tp.price_usd,0) - COALESCE(l.total_liability_qty,0) * tp.price_usd as pca_value_usd
    , COALESCE(a.total_qty * tp.price_eth,0) - COALESCE(l.total_liability_qty,0) * tp.price_eth as pca_value_eth
    FROM assets_daily a 
    LEFT JOIN liabilities_daily l on l.pricing_contract = a.token_address AND l."date" = a."date"
    LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = a."token_address" and tp."date" = a."date"
    WHERE NOT (a."date" > '2022-05-10'::date AND (a.token_address = '\x7A75ec20249570c935Ec93403A2B840fBdAC63fd' OR a.token_address='\x482258099de8de2d0bda84215864800ea7e6b03d' OR a.token_address = '\xa693b19d2931d498c5b318df961919bb4aee87a5' OR a.token_address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD')) --remove the UST tokens
    order by a."date" desc, asset_symbol asc)

SELECT "date", asset_symbol, token_address, total_asset_qty, total_liability_qty, total_liability_value_usd,total_liability_value_eth,total_asset_value_usd,total_asset_value_eth
, pca_value_usd
, pca_value_eth, pca_qty FROM(
    SELECT "date"
    , 'Dollar Stable Coins' as asset_symbol
    ,''::bytea as token_address
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token_address 
            AND tl.is_dollar_stable = true  AND token_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AND pricing_contract <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' and token_address <> '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
            GROUP BY 1,2,3
    UNION
    SELECT "date"
    , 'Ethereum' as asset_symbol
    ,'\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON (tl.address = tc.token_address) AND is_dollar_stable = false
            WHERE (tl.address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' OR tl.pricing_contract = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' or tl.address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
            GROUP BY 1,2,3
    UNION
    SELECT "date"
    , asset_symbol
    , token_address 
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = tc.token_address 
            AND is_dollar_stable = false AND tl.address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AND tl.pricing_contract <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
            GROUP BY 1,2,3
    )as t 
    ORDER BY "date" desc, asset_symbol

);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_PCAs_daily (
   "date", token_address
);