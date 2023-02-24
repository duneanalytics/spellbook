{{
    config(
        alias='ido_pools',
        tags=['static'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "impossible_finance",
                                    \'["kartod"]\') }}'
    )
}}

SELECT blockchain,
       lower(trim(address)) AS pool_address,
       launchpad,
       project_name,
       sale_type,
       launch_order,
       purchase_date,
       start_staking_period,
       end_staking_period,
       CASE
            WHEN sale_type IN ('Unlimited IDIA sale')
            THEN lower('0x0b15Ddf19D47E6a86A56148fb4aFFFc6929BcB89')
            ELSE lower('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56')
        END           AS accepted_currency
FROM (VALUES
    ('bnb','0x69b9737aff26bc528ffee4cc3257407be6a13252', 'Openswap', 'Openswap', 'Standard Sale', 1, timestamp('2021-08-30'), timestamp('2021-08-26'), timestamp('2021-08-26')),
    ('bnb','0x0246F87125973ACAb0293BB851dac34f7644344A', 'Openswap', 'Openswap', 'Unlimited Sale', 1, timestamp('2021-08-30'), timestamp('2021-08-26'), timestamp('2021-08-26')),
    ('bnb','0xD7d47a9B298b7e3C5919D376aAd20Fa4970fb73B', 'Openswap', 'Openswap', 'Whitelist $100', 1, timestamp('2021-08-30'), timestamp('2021-08-26'), timestamp('2021-08-26')),
    ('bnb','0x7fc32F60a92b6C109f3c74Fea3eaaAb9Ad062292', 'Openswap', 'Openswap', 'Whitelist $50', 1, timestamp('2021-08-30'), timestamp('2021-08-26'), timestamp('2021-08-26')),

    ('bnb','0xb278163B1D2Da632D51190001BBb95d45C3b191a', 'BLT', 'Blocto', 'Standard Sale', 2, timestamp('2021-09-28'), timestamp('2021-09-14'), timestamp('2021-09-24')),
    ('bnb','0x9Df80e3DD83C011d1258c6fCD2d723F487751aD7', 'BLT', 'Blocto', 'Unlimited Sale', 2, timestamp('2021-09-28'), timestamp('2021-09-14'), timestamp('2021-09-24')),

    ('bnb','0x09d70dB37cEDe94D1664C0B2fBD4d1b7ec9a88E0', 'HIGH', 'Highstreet', 'Standard Sale', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),
    ('bnb','0xB0b2fD6fe766EaE258e26AdD5E74987E21FA36B2', 'HIGH', 'Highstreet', 'Unlimited Sale', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),
    ('bnb','0xb20278fe899a4aE271a81CA4D70E8D2FA57b24cF', 'HIGH', 'Highstreet', 'Whitelist $100', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),
    ('bnb','0x35De9fDa52B41E77b442416eeAE5FFC16Cd9e2FF', 'HIGH', 'Highstreet', 'Whitelist $100', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),
    ('bnb','0xD19EadFF1f68B4b968D26C3B541A9F305B483b01', 'HIGH', 'Highstreet', 'Whitelist $200', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),
    ('bnb','0x55ce59c6d8ebe7ad7dFFD50821CaB4DB0Fc9D8B5', 'HIGH', 'Highstreet', 'Whitelist $500', 3, timestamp('2021-10-13'), timestamp('2021-09-27'), timestamp('2021-10-09')),

    ('bnb','0x130b6d9ff4470828F220c4056e3EA4a8d20784Eb', 'ARDN', 'Ariadne', 'Standard Sale', 4, timestamp('2021-11-03'), timestamp('2021-10-13'), timestamp('2021-10-31')),
    ('bnb','0x9792ec69629f7db468b05102482a68D547B1F502', 'ARDN', 'Ariadne', 'Unlimited Sale', 4, timestamp('2021-11-03'), timestamp('2021-10-13'), timestamp('2021-10-31')),

    ('bnb','0xB0ecb9451bC31DB00000297A2DAE7Abb5C9Aa42A', 'Genopets', 'Genopets', 'Standard Sale', 5, timestamp('2021-11-16'), timestamp('2021-10-31'), timestamp('2021-11-13')),
    ('bnb','0xfAab7A08fC4E4f643C94dE53A411b24f70b050e5', 'Genopets', 'Genopets', 'Unlimited Sale', 5, timestamp('2021-11-16'), timestamp('2021-10-31'), timestamp('2021-11-13')),
    ('bnb','0xC61D2de437a5dC0aEFB67D3642b635FFc174487C', 'Genopets', 'Genopets', 'Whitelist $100', 5, timestamp('2021-11-16'), timestamp('2021-10-31'), timestamp('2021-11-13')),

    ('bnb','0x568d3BfBBb10c75B7D26ACA1cBfa2e86d632660e', 'Sportium', 'Sportium', 'Standard Sale', 6, timestamp('2021-12-07'), timestamp('2021-11-22'), timestamp('2021-12-03')),
    ('bnb','0x2177Ab865e46Be59c8628cb0b66F439ED4D3Db58', 'Sportium', 'Sportium', 'Unlimited Sale', 6, timestamp('2021-12-07'), timestamp('2021-11-22'), timestamp('2021-12-03')),
    ('bnb','0x3E5d63044491d4782EE68c77471F2b0eB0E62451', 'Sportium', 'Sportium', 'Whitelist $100', 6, timestamp('2021-12-07'), timestamp('2021-11-22'), timestamp('2021-12-03')),

    ('bnb','0xd8905a68C589c08D5d1A1D50E525df1ec67D6b75', 'Fancy Games', 'Fancy Games', 'Standard Sale', 7, timestamp('2021-12-13'), timestamp('2021-11-26'), timestamp('2021-12-09')),
    ('bnb','0x5d7a96E521313Cbd247734FB34615AA780AFA5c9', 'Fancy Games', 'Fancy Games', 'Unlimited Sale', 7, timestamp('2021-12-13'), timestamp('2021-11-26'), timestamp('2021-12-09')),
    ('bnb','0xdE1c9d56a6157b3583DBBcD4eC77b33C76F285c4', 'Fancy Games', 'Fancy Games', 'Whitelist $100', 7, timestamp('2021-12-13'), timestamp('2021-11-26'), timestamp('2021-12-09')),

    ('bnb','0xDc0A7A9Ab0778405A56d662bf21c3295F8A4cA7d', 'Ouro', 'Ouro', 'Standard Sale', 8, timestamp('2022-01-03'), timestamp('2021-11-16'), timestamp('2021-12-30')),
    ('bnb','0x98fF066BB5041DF3e8299Ce2eAf44d5a9E4f32E4', 'Ouro', 'Ouro', 'Unlimited BUSD Sale', 8, timestamp('2022-01-03'), timestamp('2021-11-16'), timestamp('2021-12-30')),
    ('bnb','0x0c8dF3f968eC9F2bf182C41C9D42d79dF4a31857', 'Ouro', 'Ouro', 'Unlimited IDIA Sale', 8, timestamp('2022-01-03'), timestamp('2021-11-16'), timestamp('2021-12-30')),
    ('bnb','0xc5A2fBFAFA03Bb90eFDE3b44Ce13DD99489CDf91', 'Ouro', 'Ouro', 'Private 100', 8, timestamp('2022-01-03'), timestamp('2021-11-16'), timestamp('2021-12-30')),
    ('bnb','0x347cB9f91d89d5F3e19f93081a0F4c95F529E853', 'Ouro', 'Ouro', 'Private 200', 8, timestamp('2022-01-03'), timestamp('2021-11-16'), timestamp('2021-12-30')),

    ('bnb','0x53E36E2565e8113dF6c1C675B3bC7eC1788cD1C9', 'IDIA', 'IDIA', 'Unlimited Sale', 9, timestamp('2022-01-10'), timestamp('2021-12-23'), timestamp('2022-02-19')),
    ('bnb','0x98fF066BB5041DF3e8299Ce2eAf44d5a9E4f32E4', 'IDIA', 'IDIA', 'SDO Standard Sale', 9, timestamp('2022-01-10'), timestamp('2021-12-23'), timestamp('2022-02-19')),
    ('bnb','0x17AA5354E25922A23B952a28fbdA63a0C7d9B09B', 'IDIA', 'IDIA', 'Whitelist $10', 9, timestamp('2022-01-10'), timestamp('2021-12-23'), timestamp('2022-02-19')),
    ('bnb','0x843626d70f7F4B9e9a8a56596d34470e347aeb87', 'IDIA', 'IDIA', 'Whitelist $100', 9, timestamp('2022-01-10'), timestamp('2021-12-23'), timestamp('2022-02-19')),

    ('bnb','0xBAbC012D1be33e8Ae15aD0034f20eaE07359Fe36', 'Starbots', 'Starbots', 'Standard Sale', 10, timestamp('2022-01-11'), timestamp('2021-12-08'), timestamp('2022-01-07')),
    ('bnb','0xBA2aE3c9Cfadf6e86aCFA774C2C95E5646daEde3', 'Starbots', 'Starbots', 'Unlimited Sale', 10, timestamp('2022-01-11'), timestamp('2021-12-08'), timestamp('2022-01-07')),
    ('bnb','0x59299beE8aaA72649ea9Ab52326466b7DA7d1BA2', 'Starbots', 'Starbots', 'Whitelist $100', 10, timestamp('2022-01-11'), timestamp('2021-12-08'), timestamp('2022-01-07')),

    ('bnb','0x78c497Ff03D65FE71dF3Ac7ABEc13C20475a4164', 'Aurigami', 'Aurigami', 'Standard Sale', 11, timestamp('2022-05-03'), timestamp('2022-04-14'), timestamp('2022-05-01')),
    ('bnb','0x78aD9b95737f133Ae0fD50AE38b39BEa5dBc3d5E', 'Aurigami', 'Aurigami', 'Unlimited Sale', 11, timestamp('2022-05-03'), timestamp('2022-04-14'), timestamp('2022-05-01')),
    ('bnb','0x41800041eaD6dE6FAc3A2ddc0873FF936d527044', 'Aurigami', 'Aurigami', 'Whitelist $100', 11, timestamp('2022-05-03'), timestamp('2022-04-14'), timestamp('2022-05-01')),

    ('bnb','0x2bEf414A3Ec52B5426b8b9bac329D8CD95dfc916', 'Basketballverse', 'Basketballverse', 'Standard Sale', 12, timestamp('2022-04-20'), timestamp('2022-02-16'), timestamp('2022-04-18')),
    ('bnb','0xc4Afb7B44E6dd7632373A66a8Ce5cF6132f2387E', 'Basketballverse', 'Basketballverse', 'Unlimited Sale', 12, timestamp('2022-04-20'), timestamp('2022-02-16'), timestamp('2022-04-18')),
    ('bnb','0xE08750f8217b8025DB3d60B13683420cAabD6980', 'Basketballverse', 'Basketballverse', 'Whitelist $100', 12, timestamp('2022-04-20'), timestamp('2022-02-16'), timestamp('2022-04-18')),
    ('bnb','0x974aDe2A5c5065c9a24F87a257788A6b0CEB7712', 'Basketballverse', 'Basketballverse', 'Whitelist $100', 12, timestamp('2022-04-20'), timestamp('2022-02-16'), timestamp('2022-04-18')),

    ('bnb','0x8a2F0D1e8EfA123087359AeF356aa31eC0D512C1', 'Ruby', 'Ruby', 'Standard Sale', 13, timestamp('2022-06-13'), timestamp('2022-05-13'), timestamp('2022-06-13')),
    ('bnb','0x65CB926106686A993Af6501D393A075773A09BA3', 'Ruby', 'Ruby', 'Unlimited Sale', 13, timestamp('2022-06-13'), timestamp('2022-05-13'), timestamp('2022-06-13')),
    ('bnb','0x951c78E0e699E3f29a68E547280D2a0c6D82Af16', 'Ruby', 'Ruby', 'Whitelist $100', 13, timestamp('2022-06-13'), timestamp('2022-05-13'), timestamp('2022-06-13')),

    ('bnb','0x8e930597D1E863698FB54BECc16085DcE683c82b', 'Aura', 'Aura', 'Standard Sale', 14, timestamp('2022-07-26'), timestamp('2022-06-09'), timestamp('2022-07-25')),
    ('bnb','0xbDa707B5AeE2842EBFec559B54Fee45ADb38cf49', 'Aura', 'Aura', 'Unlimited Sale', 14, timestamp('2022-07-26'), timestamp('2022-06-09'), timestamp('2022-07-25')),
    ('bnb','0x605c3605e7487a0d452A8eA9314CE5d2f9927340', 'Aura', 'Aura', 'vIDIA Sale', 14, timestamp('2022-07-26'), timestamp('2022-06-09'), timestamp('2022-07-25')),

    ('bnb','0x6B1f72E2f0FCdE115290Be8CB208Da07860E1E3e', 'QUO', 'Quoll Finance', 'Standard Sale', 15, timestamp('2022-11-01'), timestamp('2022-10-25'), timestamp('2022-11-01')),
    ('bnb','0x61e518042D7783406b273cd29862244805368A75', 'QUO', 'Quoll Finance', 'vIDIA Standard Sale', 15, timestamp('2022-11-01'), timestamp('2022-10-25'), timestamp('2022-11-01')),
    ('bnb','0xfFdc77189033F5f5aa0748B58539d574f97cc038', 'QUO', 'Quoll Finance', 'vIDIA Unlimited Sale', 15, timestamp('2022-11-01'), timestamp('2022-10-25'), timestamp('2022-11-01')),
    ('bnb','0x5c937F89D117Bba82fE49B93F37a0c0420f004cA', 'QUO', 'Quoll Finance', 'Whitelist $250', 15, timestamp('2022-11-01'), timestamp('2022-10-25'), timestamp('2022-11-01')),

    ('bnb','0xeCCFe6A531E1bb379E32CbdD4e9AFd5d30465a6d', 'Pine', 'Pine Protocol', 'IDIA Standard Sale', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0x3Fd53a1B0D1eef5aC963aFB979b47E9e4b13B24e', 'Pine', 'Pine Protocol', 'vIDIA Standard Sale', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0xa800614528FE1360c0AC1Ff2268d1687A8CF97d3', 'Pine', 'Pine Protocol', 'vIDIA Unlimited Sale', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0x80050189e95fb2A27Fc90b1d931F2415Edca3247', 'Pine', 'Pine Protocol', 'Whitelist $50', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0x4f2F9CD3049882e0dbC9e1c5A1910A4dd65d9CB3', 'Pine', 'Pine Protocol', 'Whitelist $100', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0xd978A18f30E08b049b3406A1cA71234176418a2e', 'Pine', 'Pine Protocol', 'Whitelist $200', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0xCD675Ec17Ab0E935C4A70557d47C6Bd785e2aA38', 'Pine', 'Pine Protocol', 'Whitelist $250', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0x388AF868359e2Ec7d19184705621B285e5C83a9D', 'Pine', 'Pine Protocol', 'Whitelist $500', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06')),
    ('bnb','0x27ce3CD4264bfAcd86CcB7c2Ad5748387Faa0746', 'Pine', 'Pine Protocol', 'Whitelist $2000', 16, timestamp('2023-02-06'), timestamp('2022-11-17'), timestamp('2023-02-06'))
    
)AS x (blockchain, address, launchpad, project_name, sale_type, launch_order, purchase_date, start_staking_period, end_staking_period)