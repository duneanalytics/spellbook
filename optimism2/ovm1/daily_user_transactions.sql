CREATE SCHEMA IF NOT EXISTS ovm1;

DROP TABLE ovm1.user_address_daily_transactions;

CREATE TABLE IF NOT EXISTS ovm1.user_address_daily_transactions (
	day timestamptz,
	from_address bytea,
	to_address bytea,
	method_id bytea,
	num_transactions numeric,
		PRIMARY KEY(day, from_address, to_address, method_id)
);

CREATE INDEX IF NOT EXISTS ovm1_user_address_daily_transactions_day_tofrom_addresses_idx ON ovm1.user_address_daily_transactions (day,from_address,to_address);
CREATE INDEX IF NOT EXISTS ovm1_user_address_daily_transactions_day_from_address_method_id_idx ON ovm1.user_address_daily_transactions (day,from_address,method_id);
CREATE INDEX IF NOT EXISTS ovm1_user_address_daily_transactions_day_to_address_method_id_idx ON ovm1.user_address_daily_transactions (day,to_address,method_id);
CREATE INDEX IF NOT EXISTS ovm1_user_address_daily_transactions_day_method_id_idx ON ovm1.user_address_daily_transactions (day,method_id);