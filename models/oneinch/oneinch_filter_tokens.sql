{{ config(materialized='view', alias='filter_tokens') }}

-- last updated 2023-04-17
with broken_prices as (
    select * 
    from (
        values
            ('0xfb5453340c03db5ade474b27e68b6a9c6b2823eb', 'ethereum', 'ROBOT'), 
            ('0x12b6893ce26ea6341919fe289212ef77e51688c8', 'ethereum', 'TAMA'), 
            ('0x841fb148863454a3b3570f515414759be9091465', 'ethereum', 'SHIH'), 
            ('0xf3b9569f82b18aef890de263b84189bd33ebe452', 'ethereum', 'CAW'), 
            ('0x7815bda662050d84718b988735218cffd32f75ea', 'ethereum', 'YEL'), 
            ('0x22acaee85ddb83a3a33b7f0928a0e2c3bfdb6a4f', 'ethereum', 'PRXY'), 
            ('0x5d858bcd53e085920620549214a8b27ce2f04670', 'ethereum', 'POP'), 
            ('0x090185f2135308bad17527004364ebcc2d37e5f6', 'ethereum', 'SPELL'), 
            ('0xc5b3d3231001a776123194cf1290068e8b0c783b', 'ethereum', 'LIT'), 
            ('0xdb4d1099d53e92593430e33483db41c63525f55f', 'ethereum', 'JOY'), 
            ('0xe94b97b6b43639e238c851a7e693f50033efd75c', 'ethereum', 'RNBW'),
            ('0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9', 'ethereum', 'LUNC'),
            ('0x8e6cd950ad6ba651f6dd608dc70e5886b1aa6b24', 'ethereum', 'STARL'),
            ('0xbd2f0cd039e0bfcf88901c98c0bfac5ab27566e3', 'ethereum', 'DSD'),
            ('0x55d398326f99059ff775485246999027b3197955', 'bnb', 'Binance PEG BSC-USD'),
            ('0x43f3918ff115081cfbfb256a5bde1e8d181f2907', 'bnb', 'ANT'), 
            ('0x587c16b84c64751760f6e3e7e32f896634704352', 'bnb', 'WHALE'),
            ('0xe76804b43f17fc41f226d63fd2a676df409d4678', 'bnb', 'BAT'),
            ('0xa4b6e76bba7413b9b4bd83f4e3aa63cc181d869f', 'bnb', 'FTM'),
            ('0xf50155cffd6c9a3634edfd6a00850016fe02c4dc', 'bnb', 'MED'), 
            ('0xe85afccdafbe7f2b096f268e31cce3da8da2990a', 'bnb', 'aBNBc'),
            ('0xa19d3f4219e2ed6dc1cb595db20f70b8b6866734', 'bnb', 'WIRTUAL'),
            ('0x7ae97042a4a0eb4d1eb370c34bfec71042a056b7', 'optimism', 'UNLOCK') 
    )
    as t (
        contract_address
        , blockchain
        , symbol
    )
) 

select 
    lower(contract_address) as contract_address
    , blockchain
    , symbol
from broken_prices