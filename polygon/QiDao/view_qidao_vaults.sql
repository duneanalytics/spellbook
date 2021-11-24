BEGIN;

DROP MATERIALIZED VIEW IF EXISTS qidao."view_qidao_vaults";

CREATE MATERIALIZED VIEW qidao."view_qidao_vaults" AS(
with data AS (
SELECT evt_block_time, contract_address, amount/10^18 AS totals 
FROM qidao."erc20QiStablecoin_evt_DepositCollateral"
WHERE contract_address != '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' --camWBTC
UNION ALL
SELECT evt_block_time, contract_address, amount/10^8 AS totals 
FROM qidao."erc20QiStablecoin_evt_DepositCollateral"
WHERE contract_address = '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' --camWBTC
UNION ALL
SELECT evt_block_time, contract_address, amount/10^8 AS totals 
FROM qidao."erc20QiStablecoinwbtc_evt_DepositCollateral" --wbtc
UNION ALL

SELECT evt_block_time, contract_address, -amount/10^18 AS totals  
FROM qidao."erc20QiStablecoin_evt_WithdrawCollateral"
WHERE contract_address != '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' --camWBTC
UNION ALL
SELECT evt_block_time, contract_address, -amount/10^8 AS totals  
FROM qidao."erc20QiStablecoin_evt_WithdrawCollateral"
WHERE contract_address = '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' --camWBTC
UNION ALL
SELECT evt_block_time, contract_address, -amount/10^8 AS totals  
FROM qidao."erc20QiStablecoinwbtc_evt_WithdrawCollateral" --wbtc
UNION ALL
SELECT evt_block_time, contract_address, amount/10^18 AS totals 
FROM qidao."QiStablecoin_evt_DepositCollateral"
UNION ALL
SELECT evt_block_time, contract_address, -amount/10^18 AS totals 
FROM qidao."QiStablecoin_evt_WithdrawCollateral"
),

mai_vault_collaterals AS (
SELECT evt_block_time, contract_address, SUM(totals) OVER (PARTITION BY contract_address ORDER BY evt_block_time) as totals FROM data
),

mai_vault_collaterals_names AS (
SELECT 
    evt_block_time, totals,
    CASE 
        WHEN contract_address = '\xa3fa99a148fa48d14ed51d610c367c61876997f1' THEN 'MATIC'
        WHEN contract_address = '\x61167073e31b1dad85a3e531211c7b8f1e5cae72' THEN 'LINK'
        WHEN contract_address = '\x87ee36f780ae843a78d5735867bc1c13792b7b11' THEN 'AAVE'
        WHEN contract_address = '\x578375c3af7d61586c2c3a7ba87d2eed640efa40' THEN 'camAAVE'
        WHEN contract_address = '\x98b5f32dd9670191568b661a3e847ed764943875' THEN 'CRV'
        WHEN contract_address = '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' THEN 'camWBTC'
        WHEN contract_address = '\x88d84a85a87ed12b8f098e8953b322ff789fcd1a' THEN 'camWMATIC'
        WHEN contract_address = '\x3fd939b017b31eaadf9ae50c7ff7fa5c0661d47c' THEN 'WETH'
        WHEN contract_address = '\x11A33631a5B5349AF3F165d2B7901A4d67e561ad' THEN 'camWETH'
        WHEN contract_address = '\x701A1824e5574B0b6b1c8dA808B184a7AB7A2867' THEN 'BAL'
        WHEN contract_address = '\x649Aa6E6b6194250C077DF4fB37c23EE6c098513' THEN 'dQUICK'
        WHEN contract_address = '\x37131aEDd3da288467B6EBe9A77C523A700E6Ca1' THEN 'wBTC'
        WHEN contract_address = '\xF086dEdf6a89e7B16145b03a6CB0C0a9979F1433' THEN 'GHST'
    END AS vaults,
    CASE 
        WHEN contract_address = '\xa3fa99a148fa48d14ed51d610c367c61876997f1' THEN 'WMATIC'
        WHEN contract_address = '\x61167073e31b1dad85a3e531211c7b8f1e5cae72' THEN 'LINK'
        WHEN contract_address = '\x87ee36f780ae843a78d5735867bc1c13792b7b11' THEN 'AAVE'
        WHEN contract_address = '\x578375c3af7d61586c2c3a7ba87d2eed640efa40' THEN 'AAVE'
        WHEN contract_address = '\x98b5f32dd9670191568b661a3e847ed764943875' THEN 'CRV'
        WHEN contract_address = '\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0' THEN 'WBTC'
        WHEN contract_address = '\x88d84a85a87ed12b8f098e8953b322ff789fcd1a' THEN 'WMATIC'
        WHEN contract_address = '\x3fd939b017b31eaadf9ae50c7ff7fa5c0661d47c' THEN 'WETH'
        WHEN contract_address = '\x11A33631a5B5349AF3F165d2B7901A4d67e561ad' THEN 'WETH'
        WHEN contract_address = '\x701A1824e5574B0b6b1c8dA808B184a7AB7A2867' THEN 'BAL'
        WHEN contract_address = '\x649Aa6E6b6194250C077DF4fB37c23EE6c098513' THEN 'DQUICK'
        WHEN contract_address = '\x37131aEDd3da288467B6EBe9A77C523A700E6Ca1' THEN 'WBTC'
        WHEN contract_address = '\xF086dEdf6a89e7B16145b03a6CB0C0a9979F1433' THEN 'GHST'
    END AS symbol
from mai_vault_collaterals 
),

price_data AS (
SELECT date_trunc('week', minute) AS week, symbol, price from prices."usd"
WHERE symbol in ('LINK', 'AAVE', 'CRV', 'WBTC', 'WMATIC', 'WETH', 'GHST', 'DQUICK', 'BAL')
AND minute > NOW() - interval '1 week'
),

avg_price_data AS (
SELECT 
    week, symbol, 
    AVG(price) OVER (PARTITION BY symbol ORDER BY week) AS one_week_avg_price, 
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY week DESC) AS transaction_rank 
FROM price_data --avg price is taken over the same period as the vault collaterals
),

avg_price_data_final AS (
SELECT * FROM avg_price_data
WHERE week > now() - interval '1 week'
AND transaction_rank = 1
),

final_data AS (
SELECT DISTINCT c.evt_block_time, c.totals, c.vaults, c.symbol, p.one_week_avg_price FROM mai_vault_collaterals_names c
LEFT JOIN avg_price_data_final p
ON c.symbol = p.symbol
),

final_data_grouped AS (
SELECT evt_block_time, vaults, totals, totals*one_week_avg_price AS vault_tvl_usd FROM final_data
)

SELECT evt_block_time, vaults, totals as total_vault_collateral, vault_tvl_usd as total_vault_collateral_usd FROM final_data_grouped
ORDER BY 1 DESC
);

INSERT INTO cron.job(schedule, command)
VALUES ('0 0-12/12 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_qidao_vaults$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;