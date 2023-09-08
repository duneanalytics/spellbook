{{ config(
	tags=['legacy'],
	
        alias = alias('accounting', legacy_model=True),
        partition_by = ['dt'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "maker",
                                \'["lyt", "adcv", "SebVentures", "steakhouse"]\') }}'
        )
}}

WITH dao_wallet AS (
    SELECT * FROM (VALUES
        (LOWER('0x9e1585d9CA64243CE43D42f7dD7333190F66Ca09'), 'RWF Core Unit Multisig + Operational 1', 'Fixed', 'RWF-001')
        , (LOWER('0xD1505ee500791490DE8642353BA6A5b92e3550F7'), 'RWF Core Unit Multisig + Operational 2', 'Fixed', 'RWF-001')
        , (LOWER('0xe2c16c308b843eD02B09156388Cb240cEd58C01c'), 'PE Core Unit Multisig + PE Continuous Ops Multisig 1', 'Fixed', 'PE-001')
        , (LOWER('0x83e36aaa1c7b99e2d3d07789f7b70fce46f0d45e'), 'PE Core Unit Multisig + PE Continuous Ops Multisig 2', 'Fixed', 'PE-001')
        , (LOWER('0x01D26f8c5cC009868A4BF66E268c17B057fF7A73'), 'GovAlpha Multisig', 'Fixed', 'GOV-001')
        , (LOWER('0xDCAF2C84e1154c8DdD3203880e5db965bfF09B60'), 'Content Prod Multisig 1', 'Fixed', 'OLD-001')
        , (LOWER('0x6a0ce7dbb43fe537e3fd0be12dc1882393895237'), 'Content Prod Multisig 2', 'Fixed', 'OLD-001')
        , (LOWER('0x1eE3ECa7aEF17D1e74eD7C447CcBA61aC76aDbA9'), 'GovCom Multisig + Continuous Operation 1', 'Fixed', 'COM-001')
        , (LOWER('0x99E1696A680c0D9f426Be20400E468089E7FDB0f'), 'GovCom Multisig + Continuous Operation 2', 'Fixed', 'COM-001')
        , (LOWER('0x7800C137A645c07132886539217ce192b9F0528e'), 'Growth Emergency Multisig', 'Fixed', 'GRO-001')
        , (LOWER('0xb5eB779cE300024EDB3dF9b6C007E312584f6F4f'), 'SES Multisigs (Permanent Team, Incubation, Grants) 1', 'Fixed', 'SES-001')
        , (LOWER('0x7c09Ff9b59BAAebfd721cbDA3676826aA6d7BaE8'), 'SES Multisigs (Permanent Team, Incubation, Grants) 2', 'Fixed', 'SES-001')
        , (LOWER('0xf95eB8eC63D6059bA62b0A8A7F843c7D92f41de2'), 'SES Multisigs (Permanent Team, Incubation, Grants) 3', 'Fixed', 'SES-001')
        , (LOWER('0xd98ef20520048a35EdA9A202137847A62120d2d9'), 'Risk Multisig', 'Fixed', 'RISK-001')
        , (LOWER('0x8Cd0ad5C55498Aacb72b6689E1da5A284C69c0C7'), 'DUX Team Wallet', 'Fixed', 'DUX-001')
        , (LOWER('0x6D348f18c88D45243705D4fdEeB6538c6a9191F1'), 'StarkNet Team Wallet', 'Fixed', 'SNE-001')
        , (LOWER('0x955993Df48b0458A01cfB5fd7DF5F5DCa6443550'), 'Strategic Happiness Wallet 1', 'Fixed', 'SH-001') --prior primary wallet, still uses for smaller payments
        , (LOWER('0xc657ac882fb2d6ccf521801da39e910f8519508d'), 'Strategic Happiness Wallet 2', 'Fixed', 'SH-001') --multisig for most expenses
        , (LOWER('0xD740882B8616B50d0B317fDFf17Ec3f4f853F44f'), 'CES Team Wallet', 'Fixed', 'CES-001')
        , (LOWER('0x56349A38e09f36039f6AF77309690d217Beaf0bF'), 'DECO Ops + DECO Protocol Wallets 1', 'Fixed', 'DECO-001')
        , (LOWER('0xA78F1F5698f8d345a14d7323745C6c56fB8227F0'), 'DECO Ops + DECO Protocol Wallets 2', 'Fixed', 'DECO-001')
        , (LOWER('0x465AA62a82E220B331f5ECcA697c20E89554B298'), 'SAS Team Wallet', 'Fixed', 'SAS-001')
        , (LOWER('0x124c759D1084E67B19a206ab85c4527Fab26c342'), 'IS Ops Wallet', 'Fixed', 'IS-001')
        , (LOWER('0x7327Aed0Ddf75391098e8753512D8aEc8D740a1F'), 'Data Insights Wallet', 'Fixed', 'DIN-001')
        , (LOWER('0x2dC0420A736D1F40893B9481D8968E4D7424bC0B'), 'TechOps', 'Fixed', 'TECH-001')
        , (LOWER('0x2B6180b413511ce6e3DA967Ec503b2Cc19B78Db6'), 'Oracle Gas Cost Multisig + Emergency Fund 1', 'Variable', 'GAS')
        , (LOWER('0x1A5B692029b157df517b7d21a32c8490b8692b0f'), 'Oracle Gas Cost Multisig + Emergency Fund 2', 'Variable', 'GAS')
        , (LOWER('0x53CCAA8E3beF14254041500aCC3f1D4edb5B6D24'), 'Oracle Multisig, Emergency Multisig 1', 'Fixed', 'ORA-001')
        , (LOWER('0x2d09B7b95f3F312ba6dDfB77bA6971786c5b50Cf'), 'Oracle Multisig, Emergency Multisig 2', 'Fixed', 'ORA-001')
        , (LOWER('0xf737C76D2B358619f7ef696cf3F94548fEcec379'), 'Strategic Finance Multisig', 'Fixed', 'SF-001')
        , (LOWER('0x3d274fbac29c92d2f624483495c0113b44dbe7d2'), 'Events Multisig', 'Fixed', 'EVENTS-001')
        , (LOWER('0x34d8d61050ef9d2b48ab00e6dc8a8ca6581c5d63'), 'Foundation Operational Wallet', 'Fixed', 'DAIF-001')
        , (LOWER('0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'), 'DS Pause Proxy', 'Variable', 'DSPP')
        , (LOWER('0x73f09254a81e1f835ee442d1b3262c1f1d7a13ff'), 'Interim Multisig', 'Fixed', 'INTERIM')
        , (LOWER('0x87AcDD9208f73bFc9207e1f6F0fDE906bcA95cc6'), 'SES Multisig (Auditor)', 'Fixed', 'SES-001')
        , (LOWER('0x5A994D8428CCEbCC153863CCdA9D2Be6352f89ad'), 'DUX Auditor Wallet', 'Fixed', 'DUX-001')
        , (LOWER('0x25307aB59Cd5d8b4E2C01218262Ddf6a89Ff86da'), 'CES Auditor Wallet', 'Fixed', 'CES-001')
        , (LOWER('0xf482d1031e5b172d42b2daa1b6e5cbf6519596f7'), 'DECO Auditor Wallet', 'Fixed', 'DECO-001')
        , (LOWER('0xb1f950a51516a697e103aaa69e152d839182f6fe'), 'SAS Auditor Wallet', 'Fixed', 'SAS-001')
        , (LOWER('0xd1f2eef8576736c1eba36920b957cd2af07280f4'), 'IS Auditor Wallet', 'Fixed', 'IS-001')
        , (LOWER('0x96d7b01Cc25B141520C717fa369844d34FF116ec'), 'RWF Auditor Wallet', 'Fixed', 'RWF-001')
        , (LOWER('0x1a3da79ee7db30466ca752de6a75def5e635b2f6'), 'TechOps Auditor Wallet', 'Fixed', 'TECH-001')
        , (LOWER('0x5F5c328732c9E52DfCb81067b8bA56459b33921f'), 'Foundation Reserves', 'Fixed', 'DAIF-001')
        , (LOWER('0x478c7ce3e1df09130f8d65a23ad80e05b352af62'), 'Gelato Keepers', 'Variable', 'GELATO')
        --, (LOWER('0x0048FC4357DB3c0f45AdEA433a07A20769dDB0CF'), 'DSS Blow', 'Variable', 'BLOW')
        , (LOWER('0xb386Bc4e8bAE87c3F67ae94Da36F385C100a370a'), 'New Risk Multisig', 'Fixed', 'RISK-001')
    ) AS  t(wallet_address, wallet_label, varfix, code)
)

-- ********** Helper Tables *********** 
, treasury_erc20s AS (
    SELECT '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72' AS contract_address,
           '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72' AS price_address,
           18                                           AS decimals,
           'ENS'                                        AS token
    UNION ALL
    SELECT '0x4da27a545c0c5b758a6ba100e3a049001de870f5' AS contract_address,
           '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9' AS price_address,
           18                                           AS decimals,
           'stkAAVE'                                    AS token
    UNION ALL
    SELECT '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9' AS contract_address,
           '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9' AS price_address,
           18                                           AS decimals,
           'AAVE'                                       AS token
    UNION ALL
    SELECT '0xc00e94cb662c3520282e6f5717214004a7f26888' AS contract_address,
           '0xc00e94cb662c3520282e6f5717214004a7f26888' AS price_address,
           18                                           AS decimals,
           'COMP'                                       AS token
)
, ilk_list AS (
    SELECT STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk) - 2)))) AS ilk
    FROM
    (
        SELECT i AS ilk
        FROM {{ source('maker_ethereum', 'vat_call_frob') }}
        -- {% if is_incremental() %}
        -- WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
        GROUP BY i

        UNION ALL

        SELECT ilk
        FROM {{ source('maker_ethereum', 'spot_call_file') }}
        -- {% if is_incremental() %}
        -- WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
        GROUP BY ilk

        UNION ALL

        SELECT ilk
        FROM {{ source('maker_ethereum', 'jug_call_file') }}
        -- {% if is_incremental() %}
        -- WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
        GROUP BY ilk
    )
    GROUP BY ilk
)
, hashless_trxns AS (
    SELECT CAST('2022-11-01 00:00' AS TIMESTAMP)                  AS ts,
           'noHash:movingGusdPSMBalanceFromNonYieldingToYielding' AS hash,
           13410                                                  AS code,
           -222632234.27                                          AS value,
           'DAI'                                                  AS token,
           'PSM-GUSD-A'                                           AS ilk
    UNION ALL
    SELECT CAST('2022-11-01 00:00' AS TIMESTAMP)                  AS ts,
           'noHash:movingGusdPSMBalanceFromNonYieldingToYielding' AS hash,
           13411                                                  AS code,
           222632234.27                                           AS value,
           'DAI'                                                  AS token,
           'PSM-GUSD-A'                                           AS ilk
)
, ilk_list_manual_input (ilk, begin_dt, end_dt, asset_code, equity_code, apr) AS (
    --every RWA needs to be listed here to be counted (or it defaults to off-chain private credit)
    --PSMs not listed will be assumed non-yield-bearing
    --Any ilk listed in here must have complete history (a row with null as the begin month/yr and a row with null as the end month/year, can be same row)
    values
        ('PSM-GUSD-A',CAST(NULL as date),CAST('2022-10-31' as date),13410,CAST(NULL AS NUMERIC(38)),CAST(NULL AS NUMERIC(38))), --could make rate 0 as well.
        ('PSM-GUSD-A',CAST('2022-11-01' as date),CAST(NULL as date),13411,31180,0.0125),
        ('RWA001-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA002-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA003-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA004-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA005-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA006-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA007-A',CAST(NULL as date),CAST(NULL as date),12320,31172,CAST(NULL AS NUMERIC(38))),
        ('RWA008-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA009-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA014-A',CAST(NULL as date),CAST(NULL as date),13411,31180,CAST(NULL AS NUMERIC(38))),
	('RWA015-A',CAST(NULL as date),CAST(NULL as date),12320,31172,CAST(NULL AS NUMERIC(38))),
        ('UNIV2DAIUSDC-A',CAST(NULL as date),CAST(NULL as date),11140,31140,CAST(NULL AS NUMERIC(38))), --need to list all UNIV2% LP that are stable LPs, all else assumed volatile
        ('UNIV2DAIUSDT-A',CAST(NULL as date),CAST(NULL as date),11140,31140,CAST(NULL AS NUMERIC(38)))
)
, ilk_list_labeled AS
(
    SELECT *
    FROM ilk_list_manual_input

    UNION ALL

    SELECT ilk_list.ilk
    , NULL AS begin_dt
    , NULL AS end_dt
    , CASE WHEN ilk LIKE 'ETH-%' THEN 11110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A' THEN 11120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 11130
        WHEN ilk LIKE 'GUNI%' THEN 11140
        WHEN ilk LIKE 'UNIV2%' THEN 11141
        WHEN ilk LIKE 'DIRECT%' THEN 11210
        WHEN ilk LIKE 'RWA%' THEN 12310 --default rwa into off-chain private credit in case an RWA is not manually listed
        WHEN ilk LIKE 'PSM%' THEN 13410 --defaulting PSMS to non-yielding; exceptions should be listed in manual entry table
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 11510
        ELSE 11199 --other crypto loans category. all other categories are accounted for in the above logic. SAI included here
        END AS asset_code
    , CASE WHEN ilk LIKE 'ETH-%' THEN 31110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A'  THEN 31120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 31130
        WHEN ilk LIKE 'GUNI%' THEN 31140
        WHEN ilk LIKE 'UNIV2%' THEN 31141
        WHEN ilk LIKE 'DIRECT%' THEN 31160
        WHEN ilk LIKE 'RWA%' THEN 31170 --default rwa into off-chain private credit in case an RWA is not manually listed
        WHEN ilk LIKE 'PSM%' THEN CAST(NULL AS NUMERIC(38)) --defaulting PSMS to non-yielding; exceptions should be listed in manual entry table
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 31190
        ELSE 31150 --other crypto loans category. all other categories are accounted for in the above logic. SAI included here
        END AS equity_code
    , CAST(NULL AS NUMERIC(38)) AS apr
    FROM ilk_list
    WHERE ilk NOT IN (SELECT ilk FROM ilk_list_manual_input)
    AND ilk <> 'TELEPORT-FW-A' --Need to look into how to handle teleport and potentially update. Ignoring for now.
)
, chart_of_accounts (code, primary_label, secondary_label, account_label, category_label, subcategory_label) AS (
    values
    (11110, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'ETH', 'ETH'),
    (11120, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'BTC', 'BTC'),
    (11130, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'WSTETH', 'WSTETH'),
    (11140, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Stable LP'),
    (11141, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Volatile LP'),
    (11199, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Other', 'Other'),
    (11210, 'Assets', 'Collateralized Lending', 'Money Market', 'Money Market', 'D3M'),
    (11510, 'Assets', 'Collateralized Lending', 'Legacy', 'Stablecoins', 'Stablecoins'),
    (12310, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Off-Chain Private Credit'),
    (12311, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Tokenized Private Credit'),
    (12320, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Off-Chain Public Credit'), 
    (12321, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Tokenized Public Credit'),
    (13410, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Non-Yielding Stablecoin'),
    (13411, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Yielding Stablecoin'),
    (14620, 'Assets', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (19999, 'Assets', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),

    (21110, 'Liabilities', 'Stablecoin', 'Circulating', 'Interest-bearing', 'Dai'),
    (21120, 'Liabilities', 'Stablecoin', 'Circulating', 'Non-interest bearing', 'Dai'),
    (29999, 'Liabilities', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),

    (31110, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'ETH', 'ETH SF'),
    (31120, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'BTC', 'BTC SF'),
    (31130, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'WSTETH', 'WSTETH SF'),
    (31140, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Stable LP SF'),
    (31141, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Volatile LP SF'),
    (31150, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Other', 'Other SF'),
    (31160, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Money Market', 'D3M SF'),
    (31170, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Private Credit SF'),
    (31171, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Private Credit SF'),
    (31172, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Public Credit Interest'),
    (31173, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Public Credit Interest'),
    (31180, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'PSM', 'Yielding Stablecoin Interest'),
    (31190, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Stablecoins', 'Stablecoins SF'),
    (31210, 'Equity', 'Protocol Surplus', 'Liquidation Revenues', 'Liquidation Revenues', 'Liquidation Revenues'),
    (31310, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Trading Revenues'),
    --(31311, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Teleport Revenues'),  --needs to be added still
    (31410, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Mints', 'MKR Mints'),
    (31420, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Burns', 'MKR Burns'),
    (31510, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Inflow', 'Sin Inflow'),
    (31520, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Outflow', 'Sin Outflow'),
    (31610, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'DSR', 'Circulating Dai'),
    (31620, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Liquidation Expenses', 'Liquidation Expenses'),
    (31630, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Oracle Gas Expenses', 'Oracle Gas Expenses'),
    (31710, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Keeper Maintenance', 'Keeper Maintenance'),
    (31720, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Workforce Expenses'),
    (31730, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Returned Workforce Expenses'),
    (31740, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Direct to Third Party Expenses', 'Direct to Third Party Expenses'),
    (31810, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Direct MKR Token Expenses', 'Direct MKR Token Expenses'),
    (32810, 'Equity', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (33110, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Vested MKR Token Expenses', 'Vested MKR Token Expenses'),
    (34110, 'Equity', 'Reserved MKR Surplus', 'MKR Contra Equity', 'MKR Contra Equity', 'MKR Contra Equity'), 
    (39999, 'Equity', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token')
)

-- ********** Calculation Tables *********** 

, contracts AS ( 
    SELECT 'FlapFlop' AS contract_type
        , data AS contract_address
    FROM {{ source('maker_ethereum','vow_call_file') }}
    WHERE LEFT(data, 2) = '0x'
        AND call_success
        -- {% if is_incremental() %}
        -- AND call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
    GROUP BY data

    UNION ALL

    SELECT 'PSM' AS contract_type
        , u AS contract_address
    FROM {{ source('maker_ethereum','vat_call_frob') }}
    WHERE STRING(UNHEX(TRIM('0', RIGHT(i, LENGTH(i)-2)))) LIKE 'PSM%'
        AND call_success
        -- {% if is_incremental() %}
        -- AND call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
    GROUP BY u
)
, liquidation_excluded_tx AS (
    SELECT t.tx_hash
    FROM {{ source('ethereum', 'traces') }} t
    JOIN contracts c
        ON t.from = c.contract_address
        AND c.contract_type IN ('FlapFlop')
    -- {% if is_incremental() %}
    -- WHERE t.block_time >= date_trunc("day", now() - interval '1 week')
    -- {% endif %}
    GROUP BY t.tx_hash
)
, team_dai_burns_tx AS (
    SELECT call_tx_hash
         , usr
    FROM {{ source('maker_ethereum', 'dai_call_burn') }}
    WHERE call_success
      AND (usr = '0x0048fc4357db3c0f45adea433a07a20769ddb0cf' OR usr IN (SELECT wallet_address FROM dao_wallet))
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY call_tx_hash
        , usr
)
, team_dai_burns_preunioned AS (
    SELECT vat.call_block_time        AS ts
         , vat.call_tx_hash           AS hash
         , SUM(vat.rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }} vat
    JOIN team_dai_burns_tx tx -- Flop income (coming directly from users wallets)
        ON vat.call_tx_hash = tx.call_tx_hash
    WHERE vat.dst = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.call_success
    --   {% if is_incremental() %}
    --   AND vat.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY vat.call_block_time
        , vat.call_tx_hash
)
, team_dai_burns AS (
    SELECT ts
         , hash
         , 31730 AS code
         , value --increased equity
    FROM team_dai_burns_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 21120  AS code
         , -value AS value--decreased liability
    FROM team_dai_burns_preunioned
)
, psm_yield_trxns AS (
    SELECT call_tx_hash
        , CASE WHEN usr = '0xf2e7a5b83525c3017383deed19bb05fe34a62c27'
            THEN 'PSM-GUSD-A' 
        END AS ilk
    FROM {{ source('maker_ethereum', 'dai_call_burn') }}
    WHERE call_success
      AND usr IN ('0xf2e7a5b83525c3017383deed19bb05fe34a62c27') --GUSD interest payment contract
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY call_tx_hash
        , ilk
)
, psm_yield_preunioned AS (
    SELECT vat.call_block_time           ts
         , vat.call_tx_hash              hash
         , ilk
         , SUM(vat.rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }} vat
    JOIN psm_yield_trxns tx
        ON vat.call_tx_hash = tx.call_tx_hash
    WHERE vat.dst = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.call_success
    --   {% if is_incremental() %}
    --   AND vat.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY vat.call_block_time
        , vat.call_tx_hash
        , ilk
)
, psm_yield AS (
    SELECT ts
         , hash
         , 31180 AS code
         , value --increased equity
         , ilk
    FROM psm_yield_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 21120  AS code
         , -value AS value--decreased liability
         , ilk
    FROM psm_yield_preunioned
)
, rwa_yield_trxns AS (
    SELECT call_tx_hash
        , CASE WHEN usr = '0x6c6d4be2223b5d202263515351034861dd9afdb6' THEN 'RWA009-A'
            WHEN usr = '0xef1b095f700be471981aae025f92b03091c3ad47' THEN 'RWA007-A'
            WHEN usr = '0x5c82d7eafd66d7f5edc2b844860bfd93c3b0474f' THEN 'RWA014-A'
        END AS ilk
    FROM {{ source('maker_ethereum', 'dai_call_burn') }}
    WHERE call_success
      AND usr IN ('0x6c6d4be2223b5d202263515351034861dd9afdb6', '0xef1b095f700be471981aae025f92b03091c3ad47', '0x5c82d7eafd66d7f5edc2b844860bfd93c3b0474f') --HVB RWA JAR, MIP65 RWA JAR, COINBASE RWA JAR
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY call_tx_hash
        , ilk
)
, rwa_yield_preunioned AS (
    SELECT vat.call_block_time           ts
         , vat.call_tx_hash              hash
         , ilk
         , SUM(vat.rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }} vat
    JOIN rwa_yield_trxns tx
        ON vat.call_tx_hash = tx.call_tx_hash
    WHERE vat.dst = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.call_success
    --   {% if is_incremental() %}
    --   AND vat.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY vat.call_block_time
        , vat.call_tx_hash
        , ilk
)
, rwa_yield AS (
    SELECT ts
         , hash
         , COALESCE(equity_code, 31170) AS code --default to off-chain private credit
         , value --increased equity
         , ilk
    FROM rwa_yield_preunioned
    LEFT JOIN ilk_list_manual_input
        USING (ilk)

    UNION ALL

    SELECT ts
         , hash
         , 21120  AS code
         , -value AS value--decreased liability
         , ilk
    FROM rwa_yield_preunioned
)
, liquidation_revenues AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , SUM(rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }}
    WHERE dst = '0xa950524441892a31ebddf91d3ceefa04bf454466'                -- vow
      AND call_success
      AND src NOT IN (SELECT contract_address FROM contracts)               -- contract_type = 'PSM' should be enough but letting it wider
      AND src NOT IN ('0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a'
        , '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2')                     --aave v2 d3m, compound v2 d3m
      AND call_tx_hash NOT IN (SELECT tx_hash FROM liquidation_excluded_tx) -- Exclude Flop income (coming directly from users wallets)
      AND call_tx_hash NOT IN (SELECT call_tx_hash FROM team_dai_burns_tx)
      AND call_tx_hash NOT IN (SELECT call_tx_hash FROM psm_yield_trxns)
      AND call_tx_hash NOT IN (SELECT call_tx_hash FROM rwa_yield_trxns)
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY call_block_time
        , call_tx_hash
)
, liquidation_expenses AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , SUM(tab / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vow_call_fess') }}
    WHERE call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY call_block_time
        , call_tx_hash
)
, liquidation AS (
    SELECT ts
        , hash
        , 31210 AS code
        , value AS value
    FROM liquidation_revenues --increased equity
    UNION ALL
    SELECT ts
        , hash
        , 21120 AS code
        , -value AS value
    FROM liquidation_revenues --reduced liability
    UNION ALL
    SELECT ts
        , hash
        , 31620 AS code
        , -value AS value
    FROM liquidation_expenses --decreased equity
    UNION ALL
    SELECT ts
        , hash
        , 21120 AS code
        , value AS value
    FROM liquidation_expenses --increased liability
)
, d3m_revenues_preunion AS (
    SELECT call_block_time                                                                             ts
         , call_tx_hash                                                                                hash
         , CASE
               WHEN src = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' THEN 'DIRECT-AAVEV2-DAI'
               WHEN src = '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2' THEN 'DIRECT-COMPV2-DAI' END AS ilk
         , SUM(rad) / POW(10, 45)                                                                   AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }}
    WHERE call_success
      AND src IN ('0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a'
                  , '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2') --aave d3m, compound v2 d3m
      AND dst = '0xa950524441892a31ebddf91d3ceefa04bf454466'    --vow
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1, 2, 3

    UNION ALL
    
    SELECT call_block_time ts
        , call_tx_hash hash
        , STRING(UNHEX(TRIM('0', RIGHT(i, LENGTH(i)-2)))) AS ilk
        , SUM(dart)/1e18 AS value
    FROM {{ source('maker_ethereum', 'vat_call_grab') }}
    WHERE call_success
    AND dart+0 > 0
    GROUP BY 1, 2, 3
)
, d3m_revenues AS (
    SELECT ts, hash, 31160 AS code, value AS value, ilk
    FROM d3m_revenues_preunion --increased equity
    UNION ALL
    SELECT ts, hash, 21120 AS code, -value AS value, ilk
    FROM d3m_revenues_preunion --reduced liability
)
, psms AS (
    SELECT u                                                 AS psm_address
         , STRING(UNHEX(TRIM('0', RIGHT(i, LENGTH(i) - 2)))) AS ilk
    FROM {{ source('maker_ethereum', 'vat_call_frob') }}
    WHERE STRING (UNHEX(TRIM('0'
        , RIGHT (i
        , LENGTH(i)-2)))) LIKE 'PSM-%'
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1, 2
)
, trading_revenues_preunion AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , ilk
         , SUM(rad) / POW(10, 45) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }} vat
    INNER JOIN psms
        ON vat.src = psms.psm_address
    WHERE vat.call_success
      AND vat.dst = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
    --   {% if is_incremental() %}
    --   AND vat.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1, 2, 3
)
, trading_revenues AS (
    SELECT ts
        , hash
        , 31310 AS code
        , value AS value
        , ilk
    FROM trading_revenues_preunion --increased equity
    UNION ALL
    SELECT ts
        , hash
        , 21120 AS code
        , -value AS value
        , ilk
    FROM trading_revenues_preunion --reduced liability
)
, mkr_mints_preunioned AS (
    SELECT vat.call_block_time           ts
         , vat.call_tx_hash              hash
         , SUM(vat.rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }} vat
    JOIN
    (
        SELECT tx_hash
        FROM liquidation_excluded_tx
    ) tx -- Flop income (coming directly from users wallets)
        ON vat.call_tx_hash = tx.tx_hash
    WHERE vat.dst = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.call_success
      AND vat.src NOT IN (SELECT contract_address FROM contracts WHERE contract_type = 'PSM')
    --   {% if is_incremental() %}
    --   AND vat.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1, 2
)
, mkr_mints AS (
    SELECT ts
         , hash
         , 31410 AS code
         , value --increased equity
    FROM mkr_mints_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 21120  AS code
         , -value AS value--decreased liability
    FROM mkr_mints_preunioned
)
, mkr_burns_preunioned AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , SUM(rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }}
    WHERE src = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1, 2
)
, mkr_burns AS (
    SELECT ts
         , hash
         , 31420  AS code
         , -value AS value --decreased equity
    FROM mkr_burns_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 21120 AS code
         , value --increased liability
    FROM mkr_burns_preunioned
)
, interest_accruals_1 AS (
    SELECT i    AS         ilk
         , call_block_time ts
         , call_tx_hash    hash
         , dart
         , NULL AS         rate
         , call_trace_address
    FROM {{ source('maker_ethereum', 'vat_call_frob') }}
    WHERE call_success
      AND dart <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , dart
            , 0.0 AS rate
            , call_trace_address
    FROM {{ source('maker_ethereum', 'vat_call_grab') }}
    WHERE call_success
      AND dart <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , NULL AS dart
            , rate
            , call_trace_address
    FROM {{ source('maker_ethereum', 'vat_call_fold') }}
    WHERE call_success
      AND rate <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
)
, interest_accruals_2 AS (
    SELECT *
         , SUM(dart) OVER (PARTITION BY ilk ORDER BY ts ASC, call_trace_address ASC) AS cumulative_dart
    FROM interest_accruals_1
)
, interest_accruals_3 AS (
    SELECT STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk) - 2)))) AS ilk
         , ts
         , hash
         , SUM(cumulative_dart * rate) / POW(10, 45)             AS interest_accruals
    FROM interest_accruals_2
    WHERE rate IS NOT NULL
    GROUP BY 1,2,3
)
, interest_accruals AS (
    SELECT ts
         , hash
         , equity_code            AS code
         , SUM(interest_accruals) AS value --increased equity
         , interest_accruals_3.ilk
    FROM interest_accruals_3
    LEFT JOIN ilk_list_labeled
        ON interest_accruals_3.ilk = ilk_list_labeled.ilk
        AND interest_accruals_3.ts BETWEEN COALESCE(ilk_list_labeled.begin_dt, '2000-01-01') AND COALESCE(ilk_list_labeled.end_dt, '2222-12-31') --if null, ensure its not restrictive
    GROUP BY 1,2,3,5

    UNION ALL

    SELECT ts
         , hash
         , asset_code             AS code
         , SUM(interest_accruals) AS value --increased assets
         , interest_accruals_3.ilk
    FROM interest_accruals_3
    LEFT JOIN ilk_list_labeled
        ON interest_accruals_3.ilk = ilk_list_labeled.ilk
        AND CAST(interest_accruals_3.ts AS DATE) BETWEEN COALESCE(ilk_list_labeled.begin_dt, '2000-01-01') AND COALESCE(ilk_list_labeled.end_dt, '2222-12-31') --if null, ensure its not restrictive
    GROUP BY 1,2,3,5
)
, opex_suck_hashes AS (
    SELECT suck.call_tx_hash
    FROM {{ source('maker_ethereum', 'vat_call_suck') }} suck
    WHERE suck.call_success
      AND suck.u = '0xa950524441892a31ebddf91d3ceefa04bf454466'
      AND suck.v IN ('0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
                , '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71'
                , '0xa4c22f0e25c6630b2017979acf1f865e94695c4b')
      AND suck.rad+0 <> 0
    --   {% if is_incremental() %}
    --   AND suck.call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1
)
, opex_preunion AS (
    SELECT mints.call_block_time   AS ts
         , mints.call_tx_hash      AS hash
         , CASE
               WHEN dao_wallet.code = 'GELATO' THEN 31710 --keeper maintenance expenses
               WHEN dao_wallet.code = 'GAS' THEN 31630 -- oracle gas expenses
               WHEN dao_wallet.code IS NOT NULL THEN 31720 --workforce expenses
               ELSE 31740 --direct opex - when a suck operation is used to directly transfer DAI to a third party
        END                  AS equity_code
        , mints.wad / POW(10, 18) AS expense
    FROM {{ source('maker_ethereum', 'dai_call_mint') }} mints
    JOIN opex_suck_hashes opex
        ON mints.call_tx_hash = opex.call_tx_hash
    LEFT JOIN dao_wallet
        ON mints.usr = dao_wallet.wallet_address
    LEFT JOIN interest_accruals_1 AS frobs
            ON mints.call_tx_hash = frobs.hash
            AND mints.wad = frobs.dart
    WHERE mints.call_success
        AND frobs.hash IS NULL --filtering out draws from psm that happened in the same tx as expenses
        -- {% if is_incremental() %}
        -- AND dai.call_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
)
, opex AS (
    SELECT ts
         , hash
         , equity_code AS code
         , -expense    AS value --reduced equity
    FROM opex_preunion

    UNION ALL

    SELECT ts
         , hash
         , 21120   AS code
         , expense AS value --increased liability
    FROM opex_preunion
)
, dsr_expenses AS (
    SELECT call_block_time            ts
         , call_tx_hash               hash
         , 31610                   AS code
         , -SUM(rad) / POW(10, 45) AS value --reduced equity
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE u = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
    
    UNION ALL
    
    SELECT call_block_time ts
            , call_tx_hash hash
            , 21110 AS code
            , SUM(rad)/POW(10, 45) AS value --increased liability
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE u = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
)
, other_sin_outflows AS (
    SELECT call_block_time            ts
         , call_tx_hash               hash
         , 31520                   AS code
         , -SUM(rad) / POW(10, 45) AS value --reduced equity
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE u = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v NOT IN ('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
        , '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
        , '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71'
        , '0xa4c22f0e25c6630b2017979acf1f865e94695c4b')    -- dsr, opex
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
    
    UNION ALL
    
    SELECT call_block_time ts
            , call_tx_hash hash
            , 21120 AS code
            , SUM(rad)/POW(10, 45) AS value --increased liability
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE u = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v NOT IN ('0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
        , '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
        , '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71'
        , '0xa4c22f0e25c6630b2017979acf1f865e94695c4b')    -- dsr, opex
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
)
, sin_inflows AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , 31510                  AS code
         , SUM(rad) / POW(10, 45) AS value --increased equity
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE v = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
    
    UNION ALL
    
    SELECT call_block_time ts
            , call_tx_hash hash
            , 21120 AS code
            , -SUM(rad)/POW(10, 45) AS value --decreased liability
    FROM {{ source('maker_ethereum', 'vat_call_suck') }}
    WHERE v = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND call_success
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
    GROUP BY 1,2
)
, dsr_flows_preunioned AS (
    SELECT call_block_time       ts
         , call_tx_hash          hash
         , -rad / POW(10, 45) AS dsr_flow
    FROM {{ source('maker_ethereum', 'vat_call_move') }} m
    WHERE call_success
      AND src = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}

    UNION ALL

    SELECT call_block_time ts
            , call_tx_hash hash
            , rad/POW(10, 45) AS dsr_flow
    FROM {{ source('maker_ethereum', 'vat_call_move') }} m
    WHERE call_success
      AND dst = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
)
, dsr_flows AS (
    SELECT ts
         , hash
         , 21110    AS code
         , dsr_flow AS value -- positive dsr flow increases interest-bearing dai liability
    FROM dsr_flows_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 21120     AS code
         , -dsr_flow AS value -- positive dsr flow decreases non-interest-bearing dai liability
    FROM dsr_flows_preunioned
)
, treasury_flows_preunioned AS (
    SELECT evt.evt_block_time                    ts
         , evt.evt_tx_hash                       hash
         , t.token
         , SUM(evt.value / pow(10, decimals)) AS value
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} evt
    JOIN treasury_erc20s t
        ON evt.contract_address = t.contract_address
    WHERE evt.to = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
        -- {% if is_incremental() %}
        -- AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT evt.evt_block_time ts
            , evt.evt_tx_hash hash
            , t.token
            , -SUM(evt.value/pow(10, decimals)) AS value
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} evt
    JOIN treasury_erc20s t
        ON evt.contract_address = t.contract_address
    WHERE evt.from = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
        -- {% if is_incremental() %}
        -- AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
        -- {% endif %}
    GROUP BY 1, 2, 3
)
, treasury_flows AS (
    SELECT ts
         , hash
         , 32810 AS code
         , value --increased equity
         , token
    FROM treasury_flows_preunioned

    UNION ALL

    SELECT ts
         , hash
         , 14620 AS code
         , value --increased assets
         , token
    FROM treasury_flows_preunioned
)
/*
, loan_actions_1 AS (
    SELECT i    AS         ilk
         , call_block_time ts
         , call_tx_hash    hash
         , dart
         , NULL AS         rate
    FROM {{ source('maker_ethereum', 'vat_call_frob') }}
    WHERE call_success
      AND dart <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , dart
            , 0.0 AS rate
    FROM {{ source('maker_ethereum', 'vat_call_grab') }}
    WHERE call_success
      AND dart <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , NULL AS dart
            , rate
    FROM {{ source('maker_ethereum', 'vat_call_fold') }}
    WHERE call_success
      AND rate <> 0.0
    --   {% if is_incremental() %}
    --   AND call_block_time >= date_trunc("day", now() - interval '1 week')
    --   {% endif %}
)
*/
, loan_actions_2 AS (
    SELECT STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk)-2)))) AS ilk
        , ts
        , hash
        , dart
        , COALESCE(POW(10,27) + SUM(rate) OVER(PARTITION BY ilk ORDER BY ts ASC, call_trace_address ASC), POW(10,27)) AS rate
    FROM interest_accruals_1 -- loan_actions_1 was previously exactly the same as interest_accruals_1, so instead of being redundant, I am just going from interest_accruals_1 and continuing the naming convention (treating as if it was called loan_actions_1)
    WHERE STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk)-2)))) <> 'TELEPORT-FW-A'
)
, loan_actions AS (
    SELECT ts
         , hash
         , asset_code                     AS code
         , SUM(dart * rate) / POW(10, 45) AS value --increase in assets
         , loan_actions_2.ilk
    FROM loan_actions_2
    LEFT JOIN ilk_list_labeled
        ON loan_actions_2.ilk = ilk_list_labeled.ilk
        AND CAST(loan_actions_2.ts AS DATE) BETWEEN COALESCE(ilk_list_labeled.begin_dt, '2000-01-01') AND COALESCE(ilk_list_labeled.end_dt, '2222-12-31') --if null, ensure its not restrictive
    --WHERE COALESCE(dart+0,0) <> 0  --this would prob work now but query works just fine without
    GROUP BY 1,2,3,5
    HAVING SUM(dart*rate)/POW(10,45) <> 0
    
    UNION ALL
    
    SELECT ts
         , hash
         , 21120                          AS code --increase in liabilities
         , SUM(dart * rate) / POW(10, 45) AS value
         , loan_actions_2.ilk
    FROM loan_actions_2
    --WHERE COALESCE(dart+0,0) <> 0 --this would prob work now but query works just fine without
    GROUP BY 1,2,5
    HAVING SUM(dart*rate)/POW(10,45) <> 0
), create_mkr_vests_raw AS
(
    SELECT call_block_time      ts
        , call_tx_hash          hash
        , output_id
        , _bgn
        , _tau
        , _tot/1e18     AS      total_mkr
    FROM {{ source('maker_ethereum', 'dssvesttransferrable_call_create') }}
    WHERE call_success
), yanks_raw AS 
(
    SELECT call_block_time      ts
        , call_tx_hash          hash
        , _end
        , _id
    FROM {{ source('maker_ethereum', 'dssvesttransferrable_call_yank') }}
    WHERE call_success
), yanks_with_context AS
(
    SELECT yanks_raw.*
        , create_mkr_vests_raw._bgn
        , create_mkr_vests_raw._tau
        , create_mkr_vests_raw.total_mkr
        , CASE WHEN from_unixtime(yanks_raw._end) > yanks_raw.ts THEN from_unixtime(yanks_raw._end) ELSE yanks_raw.ts END AS end_time
    FROM yanks_raw
    LEFT JOIN create_mkr_vests_raw
    ON yanks_raw._id = create_mkr_vests_raw.output_id
), yanks AS
(
    SELECT ts
        , hash
        , _id
        , from_unixtime(_bgn)   AS begin_time
        , end_time
        , _tau
        , total_mkr             AS original_total_mkr
        , (1 - (unix_timestamp(end_time)-_bgn)/ _tau) * total_mkr AS yanked_mkr
    FROM yanks_with_context
), mkr_vest_creates_yanks AS
(
    SELECT ts
        , hash
        , 31810         AS code --MKR expense realized
        , -total_mkr    AS value
    FROM create_mkr_vests_raw
    
    UNION ALL
    
    SELECT ts
        , hash
        , 33110         AS code --MKR in vest contracts increases
        , total_mkr     AS value
    FROM create_mkr_vests_raw
    
    UNION ALL
    
    SELECT ts
        , hash
        , 31810         AS code --MKR expense reversed (yanked)
        , yanked_mkr    AS value
    FROM yanks
    
    UNION ALL
    
    SELECT ts
        , hash
        , 33110         AS code --MKR in vest contracts yanked (decreases)
        , -yanked_mkr   AS value
    FROM yanks
), mkr_vest_trxns AS 
(   
    SELECT evt_tx_hash AS hash
        , 1 AS vested
    FROM {{ source('maker_ethereum', 'dssvesttransferrable_evt_vest') }}
), pause_proxy_mkr_trxns_raw AS
(
    SELECT evt_block_time       ts
        , evt_tx_hash hash
        , value         AS      expense --positive expense which is a reduction in equity
        , `to`          AS      address
    FROM {{ source('maker_ethereum', 'mkr_evt_transfer') }}
    WHERE `from` = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    AND `to` <> '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' --one transaction both to and from pause proxy, ignoring this one
    
    UNION ALL
    
    SELECT evt_block_time       ts
        , evt_tx_hash hash
        , -value        AS      expense --negative expense, increase in equity
        , `from`        AS      address
    FROM {{ source('maker_ethereum', 'mkr_evt_transfer') }}
    WHERE `to` = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    AND `from` NOT IN ('0x8ee7d9235e01e6b42345120b5d270bdb763624c7'
                       , '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb') --filtering out initial transfers in; also one transaction both to and from pause proxy, ignoring this one
), pause_proxy_mkr_trxns_preunion AS
(
    SELECT ts
        , hash
        , CASE WHEN vested IS NOT NULL THEN 33110 --negative number will go into reserved surplus (aka reserved surplus will be depleted when the transaction originates from a vest contract)
            ELSE 31810 --negative number will go into protocol surplus directly when a vest contract wasn't involved
            --positive side (as in, when an expense is made, this number will go up) goes to contra code in either case
            END AS code
        , -expense/1e18 AS value
    FROM pause_proxy_mkr_trxns_raw
    LEFT JOIN mkr_vest_trxns
    USING (hash)
), pause_proxy_mkr_trxns AS
(
    SELECT ts
        , hash
        , code
        , value 
    FROM pause_proxy_mkr_trxns_preunion

    UNION ALL

    SELECT ts
        , hash
        , 34110 AS code
        , -value
    FROM pause_proxy_mkr_trxns_preunion
), m2m_levels AS 
(
    SELECT minute AS ts
        , tokens.token
        , CASE WHEN tokens.token = 'DAI' THEN 1 ELSE price END AS price
    FROM {{ source('prices', 'usd') }} p
    INNER JOIN
    (
        SELECT token, price_address FROM treasury_erc20s
        UNION ALL
        SELECT 'DAI' AS token, '0x6b175474e89094c44da98b954eedeac495271d0f' AS price_address
    ) tokens
    ON p.contract_address = tokens.price_address
    WHERE blockchain = 'ethereum'
    AND EXTRACT(HOUR FROM minute) = 23
    AND EXTRACT(MINUTE from minute) = 59
    AND minute >= '2019-11-01'
), token_prices AS
(
    SELECT minute AS ts
        , tokens.token
        , price
    FROM {{ source('prices', 'usd') }} p
    INNER JOIN
    (
        SELECT token, price_address FROM treasury_erc20s
        UNION ALL
        SELECT 'MKR' AS token, '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' AS price_address
        UNION ALL
        SELECT 'ETH' AS token, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS price_address
    ) tokens 
    ON p.contract_address = tokens.price_address
    WHERE blockchain = 'ethereum'
    AND minute >= '2019-11-01'
    
    UNION ALL
    
    SELECT '2021-11-09 00:02' AS ts, 'ENS' AS token, 44.3 AS price --ENS price history doesn't go back far enough, so manually inputting the first value from 2021-12-17 00:00
), eth_prices AS
(
    SELECT * FROM token_prices WHERE token = 'ETH'
), with_prices AS
(
    SELECT coa.code
    , unioned.ts
    , unioned.hash
    , unioned.value
    , unioned.token
    , unioned.descriptor
    , unioned.ilk
    , unioned.value * CASE WHEN unioned.token = 'DAI' THEN 1 ELSE token_prices.price END AS dai_value
    , unioned.value * CASE WHEN unioned.token = 'DAI' THEN 1 ELSE token_prices.price END / eth_prices.price AS eth_value
    , eth_prices.price AS eth_price
    FROM chart_of_accounts coa
    
    LEFT JOIN 
    (
    SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Returned Workforce Expenses' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM team_dai_burns

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Liquidation Revenues/Expenses' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM liquidation

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Trading Revenues' AS descriptor
            , ilk
        FROM trading_revenues

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'MKR Mints' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM mkr_mints

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'MKR Burns' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM mkr_burns

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Interest Accruals' AS descriptor
            , ilk
        FROM interest_accruals

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'OpEx' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM opex

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'DSR Expenses' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM dsr_expenses

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Other Sin Outflows' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM other_sin_outflows

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Sin Inflows' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM sin_inflows

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'DSR Flows' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM dsr_flows

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , token
            , 'Treasury Flows' AS descriptor
            , CAST(NULL as string) AS ilk
        FROM treasury_flows

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'Loan Draws/Repays' AS descriptor
            , ilk
        FROM loan_actions

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'D3M Revenues' AS descriptor
            , ilk
        FROM d3m_revenues

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'PSM Yield' AS descriptor
            , ilk
        FROM psm_yield

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'MKR' AS token
            , 'MKR Vest Creates/Yanks' AS descriptor
            , NULL AS ilk
        FROM mkr_vest_creates_yanks

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'MKR' AS token
            , 'MKR Pause Proxy Trxns' AS descriptor
            , NULL AS ilk
        FROM pause_proxy_mkr_trxns

        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , 'DAI' AS token
            , 'RWA Yield' AS descriptor
            , ilk
        FROM rwa_yield


        UNION ALL

        SELECT ts
            , hash
            , code
            , value
            , token
            , 'Accounting Plugs' AS descriptor
            , ilk
        FROM hashless_trxns


        UNION ALL

        SELECT ts
            , 'noHash:dailyMarkToMarket' AS hash
            , 19999 AS code
            , 0 AS value
            , token
            , 'Currency Translation to Presentation Token' AS descriptor
            , NULL AS ilk
        FROM m2m_levels

        UNION ALL

        SELECT ts
            , 'noHash:dailyMarkToMarket' AS hash
            , 29999 AS code
            , 0 AS value
            , token
            , 'Currency Translation to Presentation Token' AS descriptor
            , NULL AS ilk
        FROM m2m_levels

        UNION ALL

        SELECT ts
            , 'noHash:dailyMarkToMarket' AS hash
            , 39999 AS code
            , 0 AS value
            , token
            , 'Currency Translation to Presentation Token' AS descriptor
            , NULL AS ilk
        FROM m2m_levels
    ) unioned
    USING (code)
    LEFT JOIN token_prices
    ON CAST(unioned.ts AS DATE) = CAST(token_prices.ts AS DATE)
    AND EXTRACT(HOUR FROM unioned.ts) = EXTRACT(HOUR FROM token_prices.ts)
    AND EXTRACT(MINUTE FROM unioned.ts) = EXTRACT(MINUTE FROM token_prices.ts)
    AND unioned.token = token_prices.token
    LEFT JOIN eth_prices
    ON CAST(unioned.ts AS DATE) = CAST(eth_prices.ts AS DATE)
    AND EXTRACT(HOUR FROM unioned.ts) = EXTRACT(HOUR FROM eth_prices.ts)
    AND EXTRACT(MINUTE FROM unioned.ts) = EXTRACT(MINUTE FROM eth_prices.ts)
    WHERE value IS NOT NULL
), cumulative_sums AS
(
    SELECT with_prices.*
        , SUM(value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS cumulative_ale_token_value
        , SUM(dai_value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS cumulative_ale_dai_value
        , SUM(eth_value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS cumulative_ale_eth_value
        , m2m_levels.price * SUM(value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS dai_value_if_converted_all_once
        , m2m_levels.price/with_prices.eth_price * SUM(value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS eth_value_if_converted_all_once
        , m2m_levels.price * SUM(value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) - SUM(dai_value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) AS dai_m2m
        , m2m_levels.price/with_prices.eth_price * SUM(value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) - SUM(eth_value) OVER (PARTITION BY LEFT(code,1), with_prices.token ORDER BY with_prices.ts) eth_m2m
    FROM with_prices
    LEFT JOIN m2m_levels
    ON with_prices.token = m2m_levels.token
    AND with_prices.ts = m2m_levels.ts
), incremental_m2m AS
(
    SELECT *
        , dai_m2m - COALESCE(LAG(dai_m2m) OVER (PARTITION BY LEFT(code,1), token ORDER BY ts), 0) AS incremental_dai_m2m
        , eth_m2m - COALESCE(LAG(eth_m2m) OVER (PARTITION BY LEFT(code,1), token ORDER BY ts), 0) AS incremental_eth_m2m
    FROM cumulative_sums
    WHERE cumulative_ale_token_value > 0
    AND RIGHT(code,4) = 9999
), final AS
(
    SELECT code
        , ts
        , hash
        , value
        , token
        , descriptor
        , ilk
        , CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE dai_value END AS dai_value
        , CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE eth_value END AS eth_value
        , DATE(ts) AS dt
    FROM with_prices
    WHERE RIGHT(code,4) <> 9999
    
    UNION ALL
    
    SELECT code
        , ts
        , hash
        , NULL AS value
        , token
        , descriptor
        , ilk
        , incremental_dai_m2m AS dai_value
        , incremental_eth_m2m AS eth_value
        , DATE(ts) AS dt
    FROM incremental_m2m
)
SELECT *
FROM final
WHERE ( COALESCE(value, 0) <> 0 OR dai_value <> 0 OR eth_value <> 0 )
AND ts <= (SELECT MAX(ts) + INTERVAL 59 SECONDS FROM eth_prices) --excludes blocks for which we can't price in eth yet (last 30 min or so). 59 second interval is to accomodate the entire minute. Might have to be more restrictive even in the spell depending on how execution works
;
