CREATE SCHEMA IF NOT EXISTS aave;

CREATE TABLE IF NOT EXISTS aave.aave_daily_treasury_events (   
contract_address bytea,
version text,
evt_day timestamptz,
difference numeric,
money_out_raw numeric,
money_in_raw numeric,
transfer_out numeric,
transfer_in numeric,
burn_out numeric,
mint_in numeric,
rewards_in numeric,
staking_out numeric,
staking_in numeric,
money_in numeric,
money_out numeric,

borrow_fees_originated numeric,
repay_fees numeric,
flashloan_v1_fees numeric,
flashloan_v2_fees numeric,
liquidation_fees numeric,
swap_fees numeric,
lend_burn_fees numeric,
deployer_in numeric,
other_fees numeric,

swap_out numeric,
swap_in numeric,
gas_out numeric,
	PRIMARY KEY (contract_address, version, evt_day)

);

CREATE INDEX IF NOT EXISTS aave_daily_treasury_events_day_address_version_idx ON aave.aave_daily_treasury_events (evt_day, contract_address, version);
CREATE INDEX IF NOT EXISTS aave_daily_treasury_events_day_address_idx ON aave.aave_daily_treasury_events (evt_day, contract_address);
CREATE INDEX IF NOT EXISTS aave_daily_treasury_events_day_idx ON aave.aave_daily_treasury_events (evt_day);
CREATE INDEX IF NOT EXISTS aave_daily_treasury_events_address_idx ON aave.aave_daily_treasury_events (contract_address);
