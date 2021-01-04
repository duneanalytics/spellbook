DROP FUNCTION loopring.fn_to_uint16;

CREATE OR REPLACE FUNCTION loopring.fn_to_uint16(data bytea) RETURNS integer AS $$
BEGIN
    return get_byte(data, 0) * 256 + get_byte(data, 1);
END; $$
LANGUAGE PLPGSQL;