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

INSERT INTO cron.job(schedule, command)
VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_lookup_reactors$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;