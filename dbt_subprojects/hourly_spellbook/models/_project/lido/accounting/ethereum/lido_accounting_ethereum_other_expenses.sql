{{ config(
        schema='lido_accounting_ethereum',
        alias = 'other_expenses',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "hosuke"]\') }}'
        )
}}

with tokens AS (
    select * from (values
    (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32), --LDO
    (0x6B175474E89094C44Da98b954EedeAC495271d0F), --DAI
    (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48), --USDC
    (0xdAC17F958D2ee523a2206206994597C13D831ec7), --USDT
    (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2), --WETH
    (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0), --MATIC
    (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84), --stETH
    (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0)  --wstETH
) as tokens(address)),

source_multisigs AS (
    select * from (values
    (0x3e40d73eb977dc6a537af587d48316fee66e9c8c, 'Aragon'),
    (0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb, 'FinanceOpsMsig')
    ) as list(address, name)
),

excluded_addresses AS (
    -- Multisigs
    select address from multisigs_list
    UNION ALL
    -- Intermediate addresses
    select address from intermediate_addresses
    UNION ALL
    -- Referral payments
    select address from ldo_referral_payments_addr
    UNION ALL
    select address from dai_referral_payments_addr
    UNION ALL
    select address from steth_referral_payments_addr
    UNION ALL
    -- Zero address
    select 0x0000000000000000000000000000000000000000
    UNION ALL
    -- Diversification addresses
    select address from diversifications_addresses
    UNION ALL
    -- Stonks addresses
    select address from stonks
),

filtered_transfers AS (
    SELECT
        evt_block_time AS period,
        contract_address AS token,
        CAST(value AS DOUBLE) AS amount_token,
        evt_tx_hash
    FROM {{source('erc20_ethereum','evt_Transfer')}}
    WHERE contract_address IN (SELECT address FROM tokens)
        AND "from" IN (SELECT address FROM source_multisigs)
        AND to NOT IN (SELECT address FROM excluded_addresses)
        AND value != 0

    UNION ALL

    SELECT
        block_time AS period,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,
        CAST(value AS DOUBLE) AS amount_token,
        tx_hash AS evt_tx_hash
    FROM {{source('ethereum','traces')}} tr
    WHERE tr.success = True
        AND tr."from" IN (SELECT address FROM source_multisigs)
        AND tr.to NOT IN (SELECT address FROM excluded_addresses)
        AND tr.type = 'call'
        AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
             OR tr.call_type IS NULL)
        AND value != 0
)

SELECT *
FROM filtered_transfers