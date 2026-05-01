{% set blockchain = 'stellar' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

-- ci-stamp: 1
select
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , to_utf8(contract_address) as contract_address
    , contract_address as contract_address_native
    , cast(decimals as integer) as decimals
from
(
    values
    ('usdc-usd-coin', 'USDC', 'USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN', 7)
    , ('euroc-euro-coin', 'EURC', 'EURC-GAP2JFYUBSSY65FIFUN3NTUKP6MQQ52QETQEBDM25PFMQE2EEN2EEURC', 7)
    , ('aqua-aquarius', 'AQUA', 'AQUA-GBNZILSTVQZ4R7IKQDGHYGY2QXL5QOFJYQMXPKWRRM5PAV7Y4M67AQUA', 7)
    , ('shx-stronghold-token', 'SHX', 'SHX-GDSTRSHXHGJ7ZIVRBXEYE5Q74XUVCUSEKEBR7UCHEUUEK72N7I7KJ6JH', 7)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 'WBTC-GDTGN34MKUEO4BTJET4SX6TCRQ72FS2TPDNFO6JDXXVL4PNBSREHD2D5', 7)
) as temp (token_id, symbol, contract_address, decimals)
