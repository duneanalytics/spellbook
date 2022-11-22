-- https://dune.xyz/queries/576574
-- There are certain wrapped components of Set tokens that map closely 1:1 with their corresponding token
-- but are not actively traded, since they're just individual tokens that represent claims on other tokens


create table if not exists setprotocol_v2.set_component_token_mappings
(
  symbol varchar,
  component_address bytea,
  token_type varchar,
  mapped_symbol varchar,
  mapped_component_address bytea         
);

create index if not exists setprotocol_v2_set_component_mappings_comp_map_adr_idx on setprotocol_v2.set_component_token_mappings(component_address, mapped_component_address);
create index if not exists setprotocol_v2_set_component_mappings_comp_adr_idx on setprotocol_v2.set_component_token_mappings(component_address);
create index if not exists setprotocol_v2_set_component_mappings_comp_map_idx on setprotocol_v2.set_component_token_mappings(mapped_component_address); 

truncate table setprotocol_v2.set_component_token_mappings;

insert into setprotocol_v2.set_component_token_mappings 
(symbol,          component_address,                                    token_type,               mapped_symbol,  mapped_component_address                                ) values 
('astETH',        '\x1982b2f5814301d4e9a8b0201555376e62f82428'::bytea,  'AAVE Interest Bearing',  'stETH',        '\xae7ab96520de3a18e5e111b5eaab095312d7fe84'::bytea     ),
-- https://docs.lido.fi/integrations/aave/specification/ indicates that astETH is pegged to underlying asset 1:1
('aWETH',         '\x030ba81f1c18d280636f32af80b9aad02cf0854e'::bytea,  'AAVE Interest Bearing',  'WETH',         '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea     ),
('ALINK',         '\xa06bc25b5805d5f8d82847d191cb4af5a3e873e0'::bytea,  'AAVE Interest Bearing',  'LINK',         '\x514910771af9ca656af840dff83e8264ecf986ca'::bytea     ),
('ALINK',         '\xa64bd6c70cb9051f6a9ba1f163fdc07e0dfb5f84'::bytea,  'AAVE Interest Bearing',  'LINK',         '\x514910771af9ca656af840dff83e8264ecf986ca'::bytea     ),
('AWBTC',         '\x9ff58f4ffb29fa2266ab25e75e2a8b3503311656'::bytea,  'AAVE Interest Bearing',  'WBTC',         '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea     ),
('aUSDC(v2)',     '\xbcca60bb61934080951369a648fb03df4f96263c'::bytea,  'AAVE Interest Bearing',  'USDC',         '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea     ),
('aBUSD',         '\xA361718326c15715591c299427c62086F69923D9'::bytea,  'AAVE Interest Bearing',  'BUSD',         '\x4Fabb145d64652a948d72533023f6E7A623C7C53'::bytea     ),
('aDAI',          '\xfc1e690f61efd961294b3e1ce3313fbd8aa4f85d'::bytea,  'AAVE Interest Bearing',  'DAI',          '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea     ),
('aDAI(v2)',      '\x028171bca77440897b824ca71d1c56cac55b68a3'::bytea,  'AAVE Interest Bearing',  'DAI',          '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea     )
;