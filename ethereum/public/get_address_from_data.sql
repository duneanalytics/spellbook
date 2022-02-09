CREATE OR REPLACE FUNCTION get_address_from_data(bytea) --data from logs (64 bit)
RETURNS bytea
STABLE

LANGUAGE plpgsql
AS $$
DECLARE
    _log_data bytea := $1;
    addr_data_part bytea := '\x00'::bytea;

BEGIN

addr_data_part = substring($1 from 13 for 20);

RETURN addr_data_part;

END;
$$;
