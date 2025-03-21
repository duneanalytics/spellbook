{{config(alias = 'ofac_sanctioned',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["hildobby"]\') }}')}}
    
    SELECT address, protocol, description, blockchain, currency_contract, currency_symbol
    FROM (VALUES
    -- Source: https://home.treasury.gov/policy-issues/financial-sanctions/recent-actions/20220808
    -- Edit: https://home.treasury.gov/policy-issues/financial-sanctions/recent-actions/20250321
    (0x905b63fff465b9ffbf41dea908ceb12478ec7601, 'Tornado Cash', 'Old Proxy', 'ethereum', 0x0000000000000000000000000000000000000000, 'ETH')
    ) AS x (address, protocol, description, blockchain, currency_contract, currency_symbol)
