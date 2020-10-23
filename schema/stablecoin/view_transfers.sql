CREATE OR REPLACE view stablecoin.view_transfers AS (

WITH stablecoins(contract_address, coin_name, symbol, decimals) AS (VALUES 
        ('\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea, 'USD Circle', 'USDC'::text, '6'::integer),
        ('\x6b175474e89094c44da98b954eedeac495271d0f'::bytea, 'DAI Maker DAO', 'DAI'::text, '18'::integer),
        ('\x4fabb145d64652a948d72533023f6e7a623c7c53'::bytea, 'Binance USD', 'BUSD'::text, '18'::integer),
        ('\x8e870d67f660d95d5be530380d0ec0bd388289e1'::bytea, 'Paxos Standard', 'PAX'::text, '18'::integer),
        ('\x0000000000085d4780b73119b644ae5ecd22b376'::bytea, 'TrueUSD', 'TUSD'::text, '18'::integer),
        ('\xdf574c24545e5ffecb9a659c229253d4111d87e1'::bytea, 'St Coins', 'HUSD'::text, '8'::integer),
        ('\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea, 'Synthetix sUSD', 'sUSD'::text, '18'::integer),
        ('\x056fd409e1d7a124bd7017459dfea2f387b6d5cd'::bytea, 'Gemini dollar', 'GUSD'::text, '2'::integer),
        ('\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea, 'Tether', 'USDT'::text, '6'::integer)
        )
        
SELECT 
    "from",
    "to",
    (value/10^(decimals)) AS value,
    symbol,
    evt_block_time,
    coin_name,
    evt_tx_hash,
    value AS value_raw
FROM erc20."ERC20_evt_Transfer" tr 
INNER JOIN stablecoins st ON tr.contract_address = st.contract_address
)
;
