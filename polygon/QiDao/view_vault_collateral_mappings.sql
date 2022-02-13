BEGIN;
DROP VIEW IF EXISTS qidao.view_vaults_collaterals_mapping CASCADE;

CREATE VIEW qidao.view_vaults_collaterals_mapping (vault_contract, collateral_contract, collateral_symbol, collateral_decimals, collateral_price_symbol) AS VALUES
('\xa3fa99a148fa48d14ed51d610c367c61876997f1'::bytea, NULL::bytea,                                         'MATIC'::text, 18::integer, 'MATIC'::text), -- MATIC vault
('\x3fd939B017b31eaADF9ae50C7fF7Fa5c0661d47C'::bytea, '\x7ceb23fd6bc0add59e62ac25578270cff1b9f619'::bytea, 'WETH'::text, 18::integer, 'WETH'::text), -- WETH Vault
('\x61167073E31b1DAd85a3E531211c7B8F1E5cAE72'::bytea, '\x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39'::bytea, 'LINK'::text, 18::integer, 'LINK'::text), -- LINK Vault
('\x87ee36f780ae843A78D5735867bc1c13792b7b11'::bytea, '\xd6df932a45c0f255f85145f286ea0b292b21c90b'::bytea, 'AAVE'::text, 18::integer, 'AAVE'::text), -- AAVE Vault
('\x98B5F32dd9670191568b661a3e847Ed764943875'::bytea, '\x172370d5cd63279efa6d502dab29171933a610af'::bytea, 'CRV'::text, 18::integer, 'CRV'::text), -- CRV Vault
('\x701A1824e5574B0b6b1c8dA808B184a7AB7A2867'::bytea, '\x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3'::bytea, 'BAL'::text, 18::integer, 'BAL'::text), -- BAL Vault
('\x649Aa6E6b6194250C077DF4fB37c23EE6c098513'::bytea, '\xf28164a485b0b2c90639e47b0f377b4a438a16b1'::bytea, 'dQUICK'::text, 18::integer, 'DQUICK'::text), -- dQUICK Vault
('\x37131aEDd3da288467B6EBe9A77C523A700E6Ca1'::bytea, '\x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6'::bytea, 'WBTC'::text, 8::integer, 'WBTC'::text), -- WBTC Vault
('\xF086dEdf6a89e7B16145b03a6CB0C0a9979F1433'::bytea, '\x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7'::bytea, 'GHST'::text, 18::integer, 'GHST'::text), -- GHST Vault
('\xff2c44fb819757225a176e825255a01b3b8bb051'::bytea, '\x1a3acf6d19267e2d3e7f898f42803e90c9219062'::bytea, 'FXS'::text, 18::integer, 'FXS'::text), -- FXS Vault
('\x88d84a85A87ED12B8f098e8953B322fF789fCD1a'::bytea, '\x7068ea5255cb05931efa8026bd04b18f3deb8b0b'::bytea, 'camWMATIC'::text, 18::integer, 'WMATIC'::text), -- camWMATIC Vault
('\x11A33631a5B5349AF3F165d2B7901A4d67e561ad'::bytea, '\x0470cd31c8fcc42671465880ba81d631f0b76c1d'::bytea, 'camWETH'::text, 18::integer, 'WETH'::text), -- camWETH Vault
('\x578375c3af7d61586c2C3A7BA87d2eEd640EFA40'::bytea, '\xea4040b21cb68afb94889cb60834b13427cfc4eb'::bytea, 'camAAVE'::text, 18::integer, 'AAVE'::text), -- camAAVE Vault 
('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0'::bytea, '\xba6273a78a23169e01317bd0f6338547f869e8df'::bytea, 'camWBTC'::text, 8::integer, 'WBTC'::text), -- camWBTC Vault
('\xD2FE44055b5C874feE029119f70336447c8e8827'::bytea, '\xe6c23289ba5a9f0ef31b8eb36241d5c800889b7b'::bytea, 'camDAI'::text, 18::integer, 'DAI'::text), -- camDAI Vault
('\x57cbf36788113237d64e46f25a88855c3dff1691'::bytea, '\x7d60f21072b585351dfd5e8b17109458d97ec120'::bytea, 'sdam3CRV'::text, 18::integer, 'USDC'::text) -- sdam3CRV Vault
;

COMMIT;