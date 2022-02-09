CREATE OR REPLACE FUNCTION get_address_from_data(bytea) --data from logs (64 bit)
RETURNS bytea
STABLE
--STRICT
LANGUAGE plpgsql
AS $$
DECLARE
    _log_data bytea := $1;
    addr_data_part bytea := '\x00'::bytea;
    --nonce INT := $2;
    --gas_limit INT := $3;
BEGIN

addr_data_part = substring($1 from 13 for 20);

RETURN addr_data_part;

END;
$$;
