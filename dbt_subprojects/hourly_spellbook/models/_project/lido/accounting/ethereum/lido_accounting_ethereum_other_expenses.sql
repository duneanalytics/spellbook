{{ config(
        schema='lido_accounting_ethereum',
        alias = 'other_expenses',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        file_format = 'delta',
        unique_key = ['token', 'evt_tx_hash'],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "project",
                                    spell_name = "lido_accounting",
                                    contributors = \'["pipistrella", "adcv", "zergil1397", "hosuke"]\') }}'
        )
}}


WITH tokens AS (
    SELECT address FROM (VALUES
        (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32), --LDO
        (0x6B175474E89094C44Da98b954EedeAC495271d0F), --DAI
        (0xdC035D45d973E3EC169d2276DDab16f1e407384F), --USDS
        (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD), --Savings USDS (sUSDS)
        (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48), --USDC
        (0xdAC17F958D2ee523a2206206994597C13D831ec7), --USDT
        (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2), --WETH
        (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0), --MATIC
        (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84), --stETH
        (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0)  --wstETH
    ) AS t(address)
),

source_multisigs AS (
    SELECT address FROM (VALUES
        (0x3e40d73eb977dc6a537af587d48316fee66e9c8c), -- Aragon
        (0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb)  -- FinanceOpsMsig
    ) AS m(address)
),

multisigs_list AS (
    SELECT address FROM (VALUES
        (0x3e40d73eb977dc6a537af587d48316fee66e9c8c), -- Aragon
        (0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb), -- FinanceOpsMsig
        (0x87D93d9B2C672bf9c9642d853a8682546a5012B5), -- LiquidityRewardsMsig
        (0x753D5167C31fBEB5b49624314d74A957Eb271709), -- LiquidityRewardMngr
        (0x1dD909cDdF3dbe61aC08112dC0Fdf2Ab949f79D8), -- LiquidityRewardMngr
        (0x55c8De1Ac17C1A937293416C9BCe5789CbBf61d1), -- LiquidityRewardMngr
        (0x86F6c353A0965eB069cD7f4f91C1aFEf8C725551), -- LiquidityRewardMngr
        (0xf5436129Cf9d8fa2a1cb6e591347155276550635), -- LiquidityRewardMngr
        (0xE5576eB1dD4aA524D67Cf9a32C8742540252b6F4), -- LiquidityRewardMngr
        (0x87D93d9B2C672bf9c9642d853a8682546a5012B5), -- LiquidityRewardsMsig
        (0x9cd7477521B7d7E7F9e2F091D2eA0084e8AaA290), -- PolygonTeamRewardsMsig
        (0x5033823f27c5f977707b58f0351adcd732c955dd), -- LiquidityRewardsMsig
        (0x8c2b8595ea1b627427efe4f29a64b145df439d16), -- LiquidityRewardsMsig
        (0x13c6ef8d45afbccf15ec0701567cc9fad2b63ce8), -- ReferralRewardsMsig
        (0x12a43b049A7D330cB8aEAB5113032D18AE9a9030), -- LegoMsig
        (0x9B1cebF7616f2BC73b47D226f90b01a7c9F86956), -- ATCMsig
        (0x17F6b2C738a63a8D3A113a228cfd0b373244633D), -- PMLMsig
        (0xde06d17db9295fa8c4082d4f73ff81592a3ac437), -- RCCMsig
        (0x834560f580764bc2e0b16925f8bf229bb00cb759), -- TRPMsig
        (0x606f77BF3dd6Ed9790D9771C7003f269a385D942), -- AllianceMsig
        (0x55897893c19e4B0c52731a3b7C689eC417005Ad6), -- EcosystemBORGMsig
        (0x95B521B4F55a447DB89f6a27f951713fC2035f3F)  -- LabsBORGMsig
    ) AS list(address)
),

intermediate_addresses AS (
    SELECT address FROM (VALUES
        (0xe3224542066d3bbc02bc3d70b641be4bc6f40e36), -- Jumpgate(Solana)
        (0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf), -- Polygon bridge
        (0xa3a7b6f88361f48403514059f1f16c8e78d60eec), -- Arbitrum bridge
        (0x99c9fc46f92e8a1c0dec1b1747d010903e884be1), -- Optimism bridge
        (0x9de443adc5a411e83f1878ef24c3f52c61571e72), -- Base bridge
        (0x41527B2d03844dB6b0945f25702cB958b6d55989), -- zkSync bridge
        (0xb948a93827d68a82F6513Ad178964Da487fe2BD9), -- BnB bridge
        (0x051F1D88f0aF5763fB888eC4378b4D8B29ea3319), -- Linea bridge
        (0x2D001d79E5aF5F65a939781FE228B267a8Ed468B), -- Mantle bridge
        (0x6625C6332c9F91F2D27c304E729B86db87A3f504), -- Scroll bridge
        (0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf), -- AnySwap bridge
        (0x3ee18b2214aff97000d974cf647e7c347e8fa585), -- Wormhole bridge
        (0x9ee91F9f426fA633d227f7a9b000E28b9dfd8599), -- stMatic Contract
        (0xd0A61F2963622e992e6534bde4D52fd0a89F39E0), -- Spark PSM
        (0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341)  -- Sky PSM
    ) AS list(address)
),

ldo_referral_payments_addr AS (
    SELECT address FROM (VALUES
        (0x558247e365be655f9144e1a0140d793984372ef3),
        (0x6DC9657C2D90D57cADfFB64239242d06e6103E43),
        (0xDB2364dD1b1A733A690Bf6fA44d7Dd48ad6707Cd),
        (0x586b9b2F8010b284A0197f392156f1A7Eb5e86e9),
        (0xC976903918A0AF01366B31d97234C524130fc8B1),
        (0x53773e034d9784153471813dacaff53dbbb78e8c),
        (0x883f91D6F3090EA26E96211423905F160A9CA01d),
        (0xf6502Ea7E9B341702609730583F2BcAB3c1dC041),
        (0x82AF9d2Ea81810582657f6DC04B1d7d0D573F616),
        (0x351806B55e93A8Bcb47Be3ACAF71584deDEaB324),
        (0x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9),
        (0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d) --rhino
    ) AS list(address)
),

steth_referral_payments_addr AS (
    SELECT _recipient AS address
    FROM {{ source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientAdded') }}
),

stonks AS (
    SELECT address FROM (VALUES
        (0x3e2D251275A92a8169A3B17A2C49016e2de492a7), -- STETH→DAI
        (0xf4F6A03E3dbf0aA22083be80fDD340943d275Ea5), -- STETH→USDC
        (0x7C2a1E25cA6D778eCaEBC8549371062487846aAF), -- STETH→USDT
        (0x79f5E20996abE9f6a48AF6f9b13f1E55AED6f06D), -- DAI→USDC
        (0x8Ba6D367D15Ebc52f3eBBdb4a8710948C0918d42), -- DAI→USDT
        (0x281e6BB6F26A94250aCEb24396a8E4190726C97e), -- USDT→USDC
        (0x64B6aF9A108dCdF470E48e4c0147127F26221A7C), -- USDT→DAI
        (0x278f7B6CBB3Cc37374e6a40bDFEBfff08f65A5C7), -- USDC→USDT
        (0x2B5a3944A654439379B206DE999639508bA2e850)  -- USDC→DAI
    ) AS list(address)
),

excluded_addresses AS (
    SELECT address FROM multisigs_list
    UNION ALL
    SELECT address FROM intermediate_addresses
    UNION ALL
    SELECT address FROM ldo_referral_payments_addr
    UNION ALL
    SELECT _recipient AS address
    FROM {{ source('lido_ethereum','AllowedRecipientsRegistry_evt_RecipientAdded') }}
    UNION ALL
    SELECT _recipient AS address
    FROM {{ source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientAdded') }}
    UNION ALL
    SELECT 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT address FROM diversifications_addresses
    UNION ALL
    SELECT address FROM stonks
),

token_transfers AS (
    SELECT
        evt_block_time,
        CAST(value AS DOUBLE) AS value,
        evt_tx_hash,
        contract_address
    FROM {{ source('erc20_ethereum','evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
        AND "from" IN (SELECT address FROM source_multisigs)
        AND to NOT IN (SELECT address FROM excluded_addresses)
    {% if is_incremental() %}
        AND {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

eth_traces AS (
    SELECT
        block_time,
        CAST(value AS DOUBLE) AS value,
        tx_hash,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address
    FROM {{ source('ethereum','traces') }}
    WHERE success = True
        AND "from" IN (SELECT address FROM source_multisigs)
        AND to NOT IN (SELECT address FROM excluded_addresses)
        AND type = 'call'
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
             OR call_type IS NULL)
    {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT
    evt_block_time AS period,
    contract_address AS token,
    value AS amount_token,
    evt_tx_hash
FROM token_transfers
WHERE value != 0

UNION ALL

SELECT
    block_time AS period,
    contract_address AS token,
    value AS amount_token,
    tx_hash AS evt_tx_hash
FROM eth_traces
WHERE value != 0
